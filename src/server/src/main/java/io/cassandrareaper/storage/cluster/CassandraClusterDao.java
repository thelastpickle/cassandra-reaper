/*
 * Copyright 2016-2017 Spotify AB
 * Copyright 2016-2019 The Last Pickle Ltd
 * Copyright 2020-2020 DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.cassandrareaper.storage.cluster;

import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.ClusterProperties;
import io.cassandrareaper.storage.events.CassandraEventsDao;
import io.cassandrareaper.storage.repairschedule.CassandraRepairScheduleDao;
import io.cassandrareaper.storage.repairunit.CassandraRepairUnitDao;

import java.io.IOException;
import java.sql.Date;
import java.time.LocalDate;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraClusterDao implements IClusterDao {
  private static final String SELECT_CLUSTER = "SELECT * FROM cluster";
  private static final Logger LOG = LoggerFactory.getLogger(CassandraClusterDao.class);
  /* prepared stmts */
  PreparedStatement insertClusterPrepStmt;
  PreparedStatement getClusterPrepStmt;
  PreparedStatement deleteClusterPrepStmt;

  PreparedStatement deleteRepairRunByClusterPrepStmt;
  private final ObjectMapper objectMapper;
  private final AtomicReference<Collection<Cluster>> clustersCache = new AtomicReference(Collections.EMPTY_SET);
  private final AtomicLong clustersCacheAge = new AtomicLong(0);
  private final CassandraRepairScheduleDao cassRepairScheduleDao;
  private final CassandraRepairUnitDao cassRepairUnitDao;
  private final CassandraEventsDao cassEventsDao;

  private final Session session;

  public CassandraClusterDao(CassandraRepairScheduleDao cassRepairScheduleDao,
                             CassandraRepairUnitDao cassRepairUnitDao,
                             CassandraEventsDao cassEventsDao,
                             Session session,
                             ObjectMapper objectMapper) {

    this.session = session;
    this.objectMapper = objectMapper;
    this.cassRepairScheduleDao = cassRepairScheduleDao;
    this.cassRepairUnitDao = cassRepairUnitDao;
    this.cassEventsDao = cassEventsDao;
    prepareStatements();
  }

  private void prepareStatements() {
    insertClusterPrepStmt = session
        .prepare(
            "INSERT INTO cluster(name, partitioner, seed_hosts, properties, state, last_contact)"
                + " values(?, ?, ?, ?, ?, ?)")
        .setConsistencyLevel(ConsistencyLevel.QUORUM);
    getClusterPrepStmt = session
        .prepare("SELECT * FROM cluster WHERE name = ?")
        .setConsistencyLevel(ConsistencyLevel.QUORUM)
        .setRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE);
    deleteClusterPrepStmt = session.prepare("DELETE FROM cluster WHERE name = ?");
    deleteRepairRunByClusterPrepStmt = session.prepare(
        "DELETE FROM repair_run_by_cluster_v2 WHERE cluster_name = ?");
  }

  @Override
  public Collection<Cluster> getClusters() {
    // cache the clusters list for ten seconds
    if (System.currentTimeMillis() - clustersCacheAge.get() > TimeUnit.SECONDS.toMillis(10)) {
      clustersCacheAge.set(System.currentTimeMillis());
      Collection<Cluster> clusters = Lists.<Cluster>newArrayList();
      for (Row row : session.execute(new SimpleStatement(SELECT_CLUSTER).setIdempotent(Boolean.TRUE))) {
        try {
          clusters.add(parseCluster(row));
        } catch (IOException ex) {
          LOG.error("Failed parsing cluster {}", row.getString("name"), ex);
        }
      }
      clustersCache.set(Collections.unmodifiableCollection(clusters));
    }
    return clustersCache.get();
  }

  @Override
  public boolean addCluster(Cluster cluster) {
    assert addClusterAssertions(cluster);
    try {
      session.execute(
          insertClusterPrepStmt.bind(
              cluster.getName(),
              cluster.getPartitioner().get(),
              cluster.getSeedHosts(),
              objectMapper.writeValueAsString(cluster.getProperties()),
              cluster.getState().name(),
              Date.valueOf(cluster.getLastContact())));
    } catch (IOException e) {
      LOG.error("Failed serializing cluster information for database write", e);
      throw new IllegalStateException(e);
    }
    return true;
  }

  @Override
  public boolean updateCluster(Cluster newCluster) {
    return addCluster(newCluster);
  }

  public boolean addClusterAssertions(Cluster cluster) {
    Preconditions.checkState(
        Cluster.State.UNKNOWN != cluster.getState(),
        "Cluster should not be persisted with UNKNOWN state");

    Preconditions.checkState(cluster.getPartitioner().isPresent(), "Cannot store cluster with no partitioner.");
    // assert we're not overwriting a cluster with the same name but different node list
    Set<String> previousNodes;
    try {
      previousNodes = getCluster(cluster.getName()).getSeedHosts();
    } catch (IllegalArgumentException ignore) {
      // there is no previous cluster with same name
      previousNodes = cluster.getSeedHosts();
    }
    Set<String> addedNodes = cluster.getSeedHosts();

    Preconditions.checkArgument(
        !Collections.disjoint(previousNodes, addedNodes),
        "Trying to add/update cluster using an existing name: %s. No nodes overlap between %s and %s",
        cluster.getName(), StringUtils.join(previousNodes, ','), StringUtils.join(addedNodes, ','));

    return true;
  }

  @Override
  public Cluster getCluster(String clusterName) {
    Row row = session.execute(getClusterPrepStmt.bind(clusterName)).one();
    if (null != row) {
      try {
        return parseCluster(row);
      } catch (IOException e) {
        LOG.error("Failed parsing cluster information from the database entry", e);
        throw new IllegalStateException(e);
      }
    }
    throw new IllegalArgumentException("no such cluster: " + clusterName);
  }

  public Cluster parseCluster(Row row) throws IOException {

    ClusterProperties properties = null != row.getString("properties")
        ? objectMapper.readValue(row.getString("properties"), ClusterProperties.class)
        : ClusterProperties.builder().withJmxPort(Cluster.DEFAULT_JMX_PORT).build();

    LocalDate lastContact = row.getTimestamp("last_contact") == null
        ? LocalDate.MIN
        : new Date(row.getTimestamp("last_contact").getTime()).toLocalDate();

    Cluster.Builder builder = Cluster.builder()
        .withName(row.getString("name"))
        .withSeedHosts(row.getSet("seed_hosts", String.class))
        .withJmxPort(properties.getJmxPort())
        .withState(null != row.getString("state")
            ? Cluster.State.valueOf(row.getString("state"))
            : Cluster.State.UNREACHABLE)
        .withLastContact(lastContact);

    if (null != properties.getJmxCredentials()) {
      builder = builder.withJmxCredentials(properties.getJmxCredentials());
    }

    if (null != row.getString("partitioner")) {
      builder = builder.withPartitioner(row.getString("partitioner"));
    }
    return builder.build();
  }

  @Override
  public Cluster deleteCluster(String clusterName) {
    cassRepairScheduleDao.getRepairSchedulesForCluster(clusterName)
        .forEach(schedule -> cassRepairScheduleDao.deleteRepairSchedule(schedule.getId()));
    session.executeAsync(deleteRepairRunByClusterPrepStmt.bind(clusterName));

    cassEventsDao.getEventSubscriptions(clusterName)
        .stream()
        .filter(subscription -> subscription.getId().isPresent())
        .forEach(subscription -> cassEventsDao.deleteEventSubscription(subscription.getId().get()));

    Statement stmt = new SimpleStatement(CassandraRepairUnitDao.SELECT_REPAIR_UNIT);
    stmt.setIdempotent(true);
    ResultSet results = session.execute(stmt);
    for (Row row : results) {
      if (row.getString("cluster_name").equals(clusterName)) {
        UUID id = row.getUUID("id");
        session.executeAsync(cassRepairUnitDao.deleteRepairUnitPrepStmt.bind(id));
      }
    }
    Cluster cluster = getCluster(clusterName);
    session.execute(deleteClusterPrepStmt.bind(clusterName));
    return cluster;
  }
}