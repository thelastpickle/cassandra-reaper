/*
 * Copyright 2014-2017 Spotify AB
 * Copyright 2016-2019 The Last Pickle Ltd
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
import io.cassandrareaper.storage.MemoryStorageFacade;
import io.cassandrareaper.storage.events.MemoryEventsDao;
import io.cassandrareaper.storage.repairrun.MemoryRepairRunDao;
import io.cassandrareaper.storage.repairschedule.MemoryRepairScheduleDao;
import io.cassandrareaper.storage.repairunit.MemoryRepairUnitDao;
import io.cassandrareaper.storage.sqlite.SqliteHelper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryClusterDao implements IClusterDao {

  private static final Logger LOG = LoggerFactory.getLogger(MemoryClusterDao.class);

  private final Connection connection;
  private final MemoryRepairRunDao memoryRepairRunDao;
  private final MemoryRepairScheduleDao memRepairScheduleDao;
  private final MemoryRepairUnitDao memoryRepairUnitDao;
  private final MemoryEventsDao memEventsDao;

  private final PreparedStatement insertClusterStmt;
  private final PreparedStatement getClusterStmt;
  private final PreparedStatement getAllClustersStmt;
  private final PreparedStatement deleteClusterStmt;

  public MemoryClusterDao(
      MemoryStorageFacade storage,
      MemoryRepairUnitDao memoryRepairUnitDao,
      MemoryRepairRunDao memoryRepairRunDao,
      MemoryRepairScheduleDao memRepairScheduleDao,
      MemoryEventsDao memEventsDao) {
    this.connection = storage.getSqliteConnection();
    this.memoryRepairRunDao = memoryRepairRunDao;
    this.memRepairScheduleDao = memRepairScheduleDao;
    this.memoryRepairUnitDao = memoryRepairUnitDao;
    this.memEventsDao = memEventsDao;

    try {
      // Prepare statements
      this.insertClusterStmt =
          connection.prepareStatement(
              "INSERT OR REPLACE INTO cluster (name, partitioner, seed_hosts, properties, state, "
                  + "last_contact, namespace, jmx_username, jmx_password) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
      this.getClusterStmt = connection.prepareStatement("SELECT * FROM cluster WHERE name = ?");
      this.getAllClustersStmt = connection.prepareStatement("SELECT * FROM cluster");
      this.deleteClusterStmt = connection.prepareStatement("DELETE FROM cluster WHERE name = ?");
    } catch (SQLException e) {
      LOG.error("Failed to prepare statements for MemoryClusterDao", e);
      throw new RuntimeException("Failed to initialize MemoryClusterDao", e);
    }
  }

  @Override
  public Collection<Cluster> getClusters() {
    try {
      ResultSet rs = getAllClustersStmt.executeQuery();
      Collection<Cluster> clusters = new ArrayList<>();
      while (rs.next()) {
        clusters.add(mapRowToCluster(rs));
      }
      return clusters;
    } catch (SQLException e) {
      LOG.error("Failed to get all clusters", e);
      throw new RuntimeException("Failed to get clusters", e);
    }
  }

  @Override
  public boolean addCluster(Cluster cluster) {
    assert addClusterAssertions(cluster);

    try {
      insertClusterStmt.setString(1, cluster.getName());
      insertClusterStmt.setString(2, cluster.getPartitioner().orElse(null));
      insertClusterStmt.setString(3, SqliteHelper.toJson(cluster.getSeedHosts()));
      insertClusterStmt.setString(4, SqliteHelper.toJson(cluster.getProperties()));
      insertClusterStmt.setString(5, cluster.getState().name());
      insertClusterStmt.setLong(
          6,
          cluster.getLastContact() != null ? cluster.getLastContact().toEpochDay() * 86400000L : 0);
      insertClusterStmt.setString(7, null); // namespace not currently used
      insertClusterStmt.setString(
          8, cluster.getJmxCredentials().map(c -> c.getUsername()).orElse(null));
      insertClusterStmt.setString(
          9, cluster.getJmxCredentials().map(c -> c.getPassword()).orElse(null));

      int updated = insertClusterStmt.executeUpdate();
      return updated > 0;
    } catch (SQLException e) {
      LOG.error("Failed to add cluster: {}", cluster.getName(), e);
      throw new RuntimeException("Failed to add cluster", e);
    }
  }

  @Override
  public boolean updateCluster(Cluster newCluster) {
    return addCluster(newCluster); // INSERT OR REPLACE handles updates
  }

  public boolean addClusterAssertions(Cluster cluster) {
    Preconditions.checkState(
        Cluster.State.UNKNOWN != cluster.getState(),
        "Cluster should not be persisted with UNKNOWN state");

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
        cluster.getName(),
        StringUtils.join(previousNodes, ','),
        StringUtils.join(addedNodes, ','));

    return true;
  }

  @Override
  public Cluster getCluster(String clusterName) {
    try {
      getClusterStmt.setString(1, clusterName);
      ResultSet rs = getClusterStmt.executeQuery();

      if (rs.next()) {
        return mapRowToCluster(rs);
      } else {
        throw new IllegalArgumentException("no such cluster: " + clusterName);
      }
    } catch (SQLException e) {
      LOG.error("Failed to get cluster: {}", clusterName, e);
      throw new RuntimeException("Failed to get cluster", e);
    }
  }

  @Override
  public Cluster deleteCluster(String clusterName) {
    // Get the cluster before deleting
    Cluster cluster = getCluster(clusterName);

    // Delete related schedules
    memRepairScheduleDao
        .getRepairSchedulesForCluster(clusterName)
        .forEach(schedule -> memRepairScheduleDao.deleteRepairSchedule(schedule.getId()));

    // Delete related repair runs
    memoryRepairRunDao
        .getRepairRunIdsForCluster(clusterName, Optional.empty())
        .forEach(runId -> memoryRepairRunDao.deleteRepairRun(runId));

    // Delete related event subscriptions
    memEventsDao.getEventSubscriptions(clusterName).stream()
        .filter(subscription -> subscription.getId().isPresent())
        .forEach(subscription -> memEventsDao.deleteEventSubscription(subscription.getId().get()));

    // Delete related repair units
    memoryRepairUnitDao.getRepairUnitsForCluster(clusterName).stream()
        .forEach(
            (unit) -> {
              assert memoryRepairRunDao.getRepairRunsForUnit(unit.getId()).isEmpty()
                  : StringUtils.join(memoryRepairRunDao.getRepairRunsForUnit(unit.getId()));
              memoryRepairUnitDao.deleteRepairUnit(unit.getId());
            });

    // Delete the cluster itself
    try {
      deleteClusterStmt.setString(1, clusterName);
      deleteClusterStmt.executeUpdate();
      return cluster;
    } catch (SQLException e) {
      LOG.error("Failed to delete cluster: {}", clusterName, e);
      throw new RuntimeException("Failed to delete cluster", e);
    }
  }

  /**
   * Map a SQL ResultSet row to a Cluster object.
   *
   * @param rs The ResultSet positioned at a row
   * @return The Cluster object
   */
  private Cluster mapRowToCluster(ResultSet rs) throws SQLException {
    String name = rs.getString("name");
    String partitioner = rs.getString("partitioner");
    String seedHostsJson = rs.getString("seed_hosts");
    String propertiesJson = rs.getString("properties");
    String state = rs.getString("state");
    long lastContactMillis = rs.getLong("last_contact");
    String jmxUsername = rs.getString("jmx_username");
    String jmxPassword = rs.getString("jmx_password");

    Set<String> seedHosts =
        SqliteHelper.fromJson(seedHostsJson, new TypeReference<Set<String>>() {});
    ClusterProperties properties = SqliteHelper.fromJson(propertiesJson, ClusterProperties.class);

    LocalDate lastContact =
        lastContactMillis > 0 ? LocalDate.ofEpochDay(lastContactMillis / 86400000L) : LocalDate.MIN;

    Cluster.Builder builder =
        Cluster.builder()
            .withName(name)
            .withPartitioner(partitioner)
            .withSeedHosts(seedHosts != null ? seedHosts : Collections.emptySet())
            .withState(Cluster.State.valueOf(state))
            .withLastContact(lastContact)
            .withJmxPort(properties != null ? properties.getJmxPort() : Cluster.DEFAULT_JMX_PORT);

    // Add JMX credentials if present
    if (jmxUsername != null && jmxPassword != null) {
      builder.withJmxCredentials(
          io.cassandrareaper.core.JmxCredentials.builder()
              .withUsername(jmxUsername)
              .withPassword(jmxPassword)
              .build());
    }

    return builder.build();
  }
}
