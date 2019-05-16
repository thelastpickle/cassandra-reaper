/*
 * Copyright 2016-2017 Spotify AB
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

package io.cassandrareaper.storage;

import io.cassandrareaper.AppContext;
import io.cassandrareaper.ReaperApplicationConfiguration;
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.ClusterProperties;
import io.cassandrareaper.core.NodeMetrics;
import io.cassandrareaper.core.RepairRun;
import io.cassandrareaper.core.RepairRun.Builder;
import io.cassandrareaper.core.RepairRun.RunState;
import io.cassandrareaper.core.RepairSchedule;
import io.cassandrareaper.core.RepairSegment;
import io.cassandrareaper.core.RepairSegment.State;
import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.core.Segment;
import io.cassandrareaper.core.Snapshot;
import io.cassandrareaper.resources.view.RepairRunStatus;
import io.cassandrareaper.resources.view.RepairScheduleStatus;
import io.cassandrareaper.service.RepairParameters;
import io.cassandrareaper.service.RingRange;
import io.cassandrareaper.storage.cassandra.DateTimeCodec;
import io.cassandrareaper.storage.cassandra.Migration016;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.QueryLogger;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.VersionNumber;
import com.datastax.driver.core.WriteType;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.utils.UUIDs;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import io.dropwizard.setup.Environment;
import io.dropwizard.util.Duration;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.commons.lang3.StringUtils;
import org.cognitor.cassandra.migration.Database;
import org.cognitor.cassandra.migration.MigrationRepository;
import org.cognitor.cassandra.migration.MigrationTask;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import systems.composable.dropwizard.cassandra.CassandraFactory;
import systems.composable.dropwizard.cassandra.pooling.PoolingOptionsFactory;
import systems.composable.dropwizard.cassandra.retry.RetryPolicyFactory;

public final class CassandraStorage implements IStorage, IDistributedStorage {

  /* Simple stmts */
  private static final String SELECT_CLUSTER = "SELECT * FROM cluster";
  private static final String SELECT_REPAIR_SCHEDULE = "SELECT * FROM repair_schedule_v1";
  private static final String SELECT_REPAIR_UNIT = "SELECT * FROM repair_unit_v1";
  private static final String SELECT_LEADERS = "SELECT * FROM leader";
  private static final String SELECT_RUNNING_REAPERS = "SELECT reaper_instance_id FROM running_reapers";
  private static final DateTimeFormatter HOURLY_FORMATTER = DateTimeFormat.forPattern("yyyyMMddHH");

  private static final Logger LOG = LoggerFactory.getLogger(CassandraStorage.class);

  private static AtomicBoolean firstInstantiation = new AtomicBoolean(true);

  private final com.datastax.driver.core.Cluster cassandra;
  private final Session session;
  private final ObjectMapper objectMapper = new ObjectMapper();
  private final VersionNumber version;

  private final LoadingCache<UUID, RepairUnit> repairUnits = CacheBuilder.newBuilder()
      .build(new CacheLoader<UUID, RepairUnit>() {
        @Override
        public RepairUnit load(UUID repairUnitId) throws Exception {
          return getRepairUnitImpl(repairUnitId);
        }
      });

  /* prepared stmts */
  private PreparedStatement insertClusterPrepStmt;
  private PreparedStatement getClusterPrepStmt;
  private PreparedStatement deleteClusterPrepStmt;
  private PreparedStatement insertRepairRunPrepStmt;
  private PreparedStatement insertRepairRunClusterIndexPrepStmt;
  private PreparedStatement insertRepairRunUnitIndexPrepStmt;
  private PreparedStatement getRepairRunPrepStmt;
  private PreparedStatement getRepairRunForClusterPrepStmt;
  private PreparedStatement getRepairRunForUnitPrepStmt;
  private PreparedStatement deleteRepairRunPrepStmt;
  private PreparedStatement deleteRepairRunByClusterPrepStmt;
  private PreparedStatement deleteRepairRunByUnitPrepStmt;
  private PreparedStatement insertRepairUnitPrepStmt;
  private PreparedStatement getRepairUnitPrepStmt;
  private PreparedStatement deleteRepairUnitPrepStmt;
  private PreparedStatement insertRepairSegmentPrepStmt;
  private PreparedStatement insertRepairSegmentIncrementalPrepStmt;
  private PreparedStatement updateRepairSegmentPrepStmt;
  private PreparedStatement insertRepairSegmentEndTimePrepStmt;
  private PreparedStatement getRepairSegmentPrepStmt;
  private PreparedStatement getRepairSegmentsByRunIdPrepStmt;
  private PreparedStatement getRepairSegmentCountByRunIdPrepStmt;
  @Nullable // null on Cassandra-2 as it's not supported syntax
  private PreparedStatement getRepairSegmentsByRunIdAndStatePrepStmt = null;
  @Nullable // null on Cassandra-2 as it's not supported syntax
  private PreparedStatement getRepairSegmentCountByRunIdAndStatePrepStmt = null;
  private PreparedStatement insertRepairSchedulePrepStmt;
  private PreparedStatement getRepairSchedulePrepStmt;
  private PreparedStatement getRepairScheduleByClusterAndKsPrepStmt;
  private PreparedStatement insertRepairScheduleByClusterAndKsPrepStmt;
  private PreparedStatement deleteRepairSchedulePrepStmt;
  private PreparedStatement deleteRepairScheduleByClusterAndKsPrepStmt;
  private PreparedStatement takeLeadPrepStmt;
  private PreparedStatement renewLeadPrepStmt;
  private PreparedStatement releaseLeadPrepStmt;
  private PreparedStatement forceReleaseLeadPrepStmt;
  private PreparedStatement getRunningReapersCountPrepStmt;
  private PreparedStatement saveHeartbeatPrepStmt;
  private PreparedStatement storeNodeMetricsPrepStmt;
  private PreparedStatement getNodeMetricsPrepStmt;
  private PreparedStatement getNodeMetricsByNodePrepStmt;
  private PreparedStatement delNodeMetricsByNodePrepStmt;
  private PreparedStatement getSnapshotPrepStmt;
  private PreparedStatement deleteSnapshotPrepStmt;
  private PreparedStatement saveSnapshotPrepStmt;

  public CassandraStorage(ReaperApplicationConfiguration config, Environment environment) throws ReaperException {
    CassandraFactory cassandraFactory = config.getCassandraFactory();
    overrideQueryOptions(cassandraFactory);
    overrideRetryPolicy(cassandraFactory);
    overridePoolingOptions(cassandraFactory);

    // https://docs.datastax.com/en/developer/java-driver/3.5/manual/metrics/#metrics-4-compatibility
    cassandraFactory.setJmxEnabled(false);
    if (!CassandraStorage.firstInstantiation.compareAndSet(true, false)) {
      // If there's been a past connection attempt, metrics are already registered
      cassandraFactory.setMetricsEnabled(false);
    }

    cassandra = cassandraFactory.build(environment);
    if (config.getActivateQueryLogger()) {
      cassandra.register(QueryLogger.builder().build());
    }
    CodecRegistry codecRegistry = cassandra.getConfiguration().getCodecRegistry();
    codecRegistry.register(new DateTimeCodec());
    session = cassandra.connect(config.getCassandraFactory().getKeyspace());

    initializeAndUpgradeSchema(cassandra, session, config.getCassandraFactory().getKeyspace());

    version = cassandra.getMetadata().getAllHosts()
        .stream()
        .map(h -> h.getCassandraVersion())
        .min(VersionNumber::compareTo)
        .get();

    prepareStatements();
  }

  private static void initializeAndUpgradeSchema(
      com.datastax.driver.core.Cluster cassandra,
      Session session,
      String keyspace) throws ReaperException {

    cassandra.getMetadata().getAllHosts().forEach((host) -> {
      Preconditions.checkState(
              0 >= VersionNumber.parse("2.1").compareTo(host.getCassandraVersion()),
              "All Cassandra nodes in Reaper's backend storage must be running version 2.1+");
    });

    // initialize/upgrade db schema
    try (Database database = new Database(cassandra, keyspace)) {
      int currentVersion = database.getVersion();
      MigrationRepository migrationRepo = new MigrationRepository("db/cassandra");
      if (currentVersion < migrationRepo.getLatestVersion()) {
        LOG.warn("Starting db migration from {} to {}…", currentVersion, migrationRepo.getLatestVersion());

        if (4 <= currentVersion) {
          List<String> otherRunningReapers = session.execute("SELECT reaper_instance_host FROM running_reapers").all()
              .stream()
              .map((row) -> row.getString("reaper_instance_host"))
              .filter((reaperInstanceHost) -> !AppContext.REAPER_INSTANCE_ADDRESS.equals(reaperInstanceHost))
              .collect(Collectors.toList());

          Preconditions.checkState(
              otherRunningReapers.isEmpty(),
              "Database migration can not happen with other reaper instances running. Found ",
              StringUtils.join(otherRunningReapers));
        }

        migrate(database.getVersion(), migrationRepo, session);
        // some migration steps depend on the Cassandra version, so must be rerun every startup
        Migration016.migrate(session, keyspace);
      } else {
        LOG.info(
            String.format("Keyspace %s already at schema version %d", session.getLoggedKeyspace(), currentVersion));
      }
    } catch (RuntimeException e) {
      LOG.error("Failed performing Cassandra schema migrations", e);
      throw new ReaperException(e);
    }
  }

  private static void migrate(int dbVersion, MigrationRepository repository, Session session) {
    Preconditions.checkState(dbVersion < repository.getLatestVersion());

    for (int i = dbVersion + 1 ; i <= repository.getLatestVersion(); ++i) {
      final int nextVersion = i;
      // perform the migrations one at a time, so the MigrationXXX classes can be executed alongside the scripts
      MigrationRepository migrationRepo = new MigrationRepository("db/cassandra") {
        @Override
        public int getLatestVersion() {
          return nextVersion;
        }

        @Override
        public List getMigrationsSinceVersion(int version) {
          return Collections.singletonList((Object)super.getMigrationsSinceVersion(nextVersion - 1).get(0));
        }
      };

      try (Database database = new Database(session.getCluster(), session.getLoggedKeyspace())) {
        MigrationTask migration = new MigrationTask(database, migrationRepo);
        migration.migrate();
        // after the script execute any MigrationXXX class that exists with the same version number
        Class.forName("io.cassandrareaper.storage.cassandra.Migration" + String.format("%03d", nextVersion))
            .getDeclaredMethod("migrate", Session.class)
            .invoke(null, session);

        LOG.info("executed Migration" + String.format("%03d", nextVersion));
      } catch (ReflectiveOperationException ignore) { }
      LOG.info(String.format("Migrated keyspace %s to version %d", session.getLoggedKeyspace(), nextVersion));
    }
  }

  private void prepareStatements() {
    final String timeUdf = 0 < VersionNumber.parse("2.2").compareTo(version) ? "dateOf" : "toTimestamp";
    insertClusterPrepStmt
        = session
            .prepare(
                "INSERT INTO cluster(name, partitioner, seed_hosts, properties) values(?, ?, ?, ?)")
            .setConsistencyLevel(ConsistencyLevel.QUORUM);
    getClusterPrepStmt = session
        .prepare("SELECT * FROM cluster WHERE name = ?")
        .setConsistencyLevel(ConsistencyLevel.QUORUM)
        .setRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE);
    deleteClusterPrepStmt = session.prepare("DELETE FROM cluster WHERE name = ?");
    insertRepairRunPrepStmt = session
        .prepare(
            "INSERT INTO repair_run(id, cluster_name, repair_unit_id, cause, owner, state, creation_time, "
                + "start_time, end_time, pause_time, intensity, last_event, segment_count, repair_parallelism) "
                + "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .setConsistencyLevel(ConsistencyLevel.QUORUM);
    insertRepairRunClusterIndexPrepStmt
        = session.prepare("INSERT INTO repair_run_by_cluster(cluster_name, id) values(?, ?)");
    insertRepairRunUnitIndexPrepStmt
        = session.prepare("INSERT INTO repair_run_by_unit(repair_unit_id, id) values(?, ?)");
    getRepairRunPrepStmt = session
        .prepare(
            "SELECT id,cluster_name,repair_unit_id,cause,owner,state,creation_time,start_time,end_time,"
                + "pause_time,intensity,last_event,segment_count,repair_parallelism "
                + "FROM repair_run WHERE id = ? LIMIT 1")
        .setConsistencyLevel(ConsistencyLevel.QUORUM);
    getRepairRunForClusterPrepStmt = session.prepare("SELECT * FROM repair_run_by_cluster WHERE cluster_name = ?");
    getRepairRunForUnitPrepStmt = session.prepare("SELECT * FROM repair_run_by_unit WHERE repair_unit_id = ?");
    deleteRepairRunPrepStmt = session.prepare("DELETE FROM repair_run WHERE id = ?");
    deleteRepairRunByClusterPrepStmt
        = session.prepare("DELETE FROM repair_run_by_cluster WHERE id = ? and cluster_name = ?");
    deleteRepairRunByUnitPrepStmt = session.prepare("DELETE FROM repair_run_by_unit "
        + "WHERE id = ? and repair_unit_id= ?");
    insertRepairUnitPrepStmt = session
        .prepare(
            "INSERT INTO repair_unit_v1(id, cluster_name, keyspace_name, column_families, "
                + "incremental_repair, nodes, \"datacenters\", blacklisted_tables, repair_thread_count) "
                + "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .setConsistencyLevel(ConsistencyLevel.QUORUM);
    getRepairUnitPrepStmt = session
        .prepare("SELECT * FROM repair_unit_v1 WHERE id = ?")
        .setConsistencyLevel(ConsistencyLevel.QUORUM);
    deleteRepairUnitPrepStmt = session.prepare("DELETE FROM repair_unit_v1 WHERE id = ?");
    insertRepairSegmentPrepStmt = session
        .prepare(
            "INSERT INTO repair_run"
                + "(id,segment_id,repair_unit_id,start_token,end_token,segment_state,fail_count, token_ranges)"
                + " VALUES(?, ?, ?, ?, ?, ?, ?, ?)")
        .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
    insertRepairSegmentIncrementalPrepStmt = session
        .prepare(
            "INSERT INTO repair_run"
                + "(id,segment_id,repair_unit_id,start_token,end_token,segment_state,coordinator_host,fail_count)"
                + " VALUES(?, ?, ?, ?, ?, ?, ?, ?)")
        .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
    updateRepairSegmentPrepStmt = session
        .prepare(
            "INSERT INTO repair_run"
                + "(id,segment_id,segment_state,coordinator_host,segment_start_time,fail_count)"
                + " VALUES(?, ?, ?, ?, ?, ?)")
        .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
    insertRepairSegmentEndTimePrepStmt = session
        .prepare("INSERT INTO repair_run(id, segment_id, segment_end_time) VALUES(?, ?, ?)")
        .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
    getRepairSegmentPrepStmt = session
            .prepare(
                "SELECT id,repair_unit_id,segment_id,start_token,end_token,segment_state,coordinator_host,"
                    + "segment_start_time,segment_end_time,fail_count, token_ranges"
                    + " FROM repair_run WHERE id = ? and segment_id = ?")
            .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
    getRepairSegmentsByRunIdPrepStmt = session.prepare(
        "SELECT id,repair_unit_id,segment_id,start_token,end_token,segment_state,coordinator_host,segment_start_time,"
            + "segment_end_time,fail_count, token_ranges FROM repair_run WHERE id = ?");
    getRepairSegmentCountByRunIdPrepStmt = session.prepare("SELECT count(*) FROM repair_run WHERE id = ?");
    insertRepairSchedulePrepStmt
        = session
            .prepare(
                "INSERT INTO repair_schedule_v1(id, repair_unit_id, state,"
                    + "days_between, next_activation, run_history, "
                    + "segment_count, repair_parallelism, intensity, "
                    + "creation_time, owner, pause_time, segment_count_per_node) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .setConsistencyLevel(ConsistencyLevel.QUORUM);
    getRepairSchedulePrepStmt
        = session
            .prepare("SELECT * FROM repair_schedule_v1 WHERE id = ?")
            .setConsistencyLevel(ConsistencyLevel.QUORUM);
    insertRepairScheduleByClusterAndKsPrepStmt = session.prepare(
        "INSERT INTO repair_schedule_by_cluster_and_keyspace(cluster_name, keyspace_name, repair_schedule_id)"
            + " VALUES(?, ?, ?)");
    getRepairScheduleByClusterAndKsPrepStmt = session.prepare(
        "SELECT repair_schedule_id FROM repair_schedule_by_cluster_and_keyspace "
            + "WHERE cluster_name = ? and keyspace_name = ?");
    deleteRepairSchedulePrepStmt = session.prepare("DELETE FROM repair_schedule_v1 WHERE id = ?");
    deleteRepairScheduleByClusterAndKsPrepStmt = session.prepare(
        "DELETE FROM repair_schedule_by_cluster_and_keyspace "
            + "WHERE cluster_name = ? and keyspace_name = ? and repair_schedule_id = ?");
    prepareLeaderElectionStatements(timeUdf);
    getRunningReapersCountPrepStmt = session.prepare(SELECT_RUNNING_REAPERS);
    saveHeartbeatPrepStmt = session
        .prepare(
            "INSERT INTO running_reapers(reaper_instance_id, reaper_instance_host, last_heartbeat)"
                + " VALUES(?,?," + timeUdf + "(now()))")
        .setIdempotent(false);

    getSnapshotPrepStmt = session.prepare("SELECT * FROM snapshot WHERE cluster = ? and snapshot_name = ?");
    deleteSnapshotPrepStmt = session.prepare("DELETE FROM snapshot WHERE cluster = ? and snapshot_name = ?");
    saveSnapshotPrepStmt = session.prepare(
            "INSERT INTO snapshot (cluster, snapshot_name, owner, cause, creation_time)"
                + " VALUES(?,?,?,?,?)");

    if (0 >= VersionNumber.parse("3.0").compareTo(version)) {
      try {
        getRepairSegmentsByRunIdAndStatePrepStmt = session.prepare(
            "SELECT id,repair_unit_id,segment_id,start_token,end_token,segment_state,coordinator_host,"
                + "segment_start_time,segment_end_time,fail_count, token_ranges FROM repair_run "
                + "WHERE id = ? AND segment_state = ? ALLOW FILTERING");
        getRepairSegmentCountByRunIdAndStatePrepStmt = session.prepare(
            "SELECT count(segment_id) FROM repair_run WHERE id = ? AND segment_state = ? ALLOW FILTERING");
      } catch (InvalidQueryException ex) {
        throw new AssertionError(
            "Failure preparing `SELECT… FROM repair_run WHERE… ALLOW FILTERING` should only happen on Cassandra-2",
            ex);
      }
    }
    prepareMetricStatements();
  }

  private void prepareLeaderElectionStatements(final String timeUdf) {
    takeLeadPrepStmt = session
        .prepare(
            "INSERT INTO leader(leader_id, reaper_instance_id, reaper_instance_host, last_heartbeat)"
                + "VALUES(?, ?, ?, " + timeUdf + "(now())) IF NOT EXISTS");
    renewLeadPrepStmt = session
        .prepare(
            "UPDATE leader SET reaper_instance_id = ?, reaper_instance_host = ?,"
                + " last_heartbeat = " + timeUdf + "(now()) WHERE leader_id = ? IF reaper_instance_id = ?");
    releaseLeadPrepStmt = session.prepare("DELETE FROM leader WHERE leader_id = ? IF reaper_instance_id = ?");
    forceReleaseLeadPrepStmt = session.prepare("DELETE FROM leader WHERE leader_id = ?");
  }

  private void prepareMetricStatements() {
    storeNodeMetricsPrepStmt = session
        .prepare(
            "INSERT INTO node_metrics_v1 (time_partition,run_id,node,datacenter,cluster,requested,pending_compactions,"
                + "has_repair_running,active_anticompactions) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .setIdempotent(false);
    getNodeMetricsPrepStmt = session.prepare("SELECT * FROM node_metrics_v1"
        + " WHERE time_partition = ? AND run_id = ?");
    getNodeMetricsByNodePrepStmt = session.prepare("SELECT * FROM node_metrics_v1"
        + " WHERE time_partition = ? AND run_id = ? AND node = ?");
    delNodeMetricsByNodePrepStmt = session.prepare("DELETE FROM node_metrics_v1"
        + " WHERE time_partition = ? AND run_id = ? AND node = ?");
  }

  @Override
  public boolean isStorageConnected() {
    return session != null && !session.isClosed();
  }

  @Override
  public Collection<Cluster> getClusters() throws ReaperException {
    Collection<Cluster> clusters = Lists.<Cluster>newArrayList();
    Statement stmt = new SimpleStatement(SELECT_CLUSTER);
    stmt.setIdempotent(Boolean.TRUE);
    ResultSet clusterResults = session.execute(stmt);
    for (Row cluster : clusterResults) {
      try {
        ClusterProperties properties
            = cluster.getString("properties") != null
                ? objectMapper.readValue(cluster.getString("properties"), ClusterProperties.class)
                : ClusterProperties.builder().withJmxPort(Cluster.DEFAULT_JMX_PORT).build();

        clusters.add(
            new Cluster(
                cluster.getString("name"),
                Optional.ofNullable(cluster.getString("partitioner")),
                cluster.getSet("seed_hosts", String.class),
                properties));
      } catch (IOException e) {
        LOG.error("Failed parsing cluster information from the database entry", e);
        throw new ReaperException(e);
      }
    }

    return clusters;
  }

  @Override
  public boolean addCluster(Cluster cluster) throws ReaperException {
    try {
      Preconditions.checkState(cluster.getPartitioner().isPresent(),
          "Cannot store a cluster that has no partitioner.");
      session.execute(
          insertClusterPrepStmt.bind(
              cluster.getName(),
              cluster.getPartitioner().get(),
              cluster.getSeedHosts(),
              objectMapper.writeValueAsString(cluster.getProperties())));
    } catch (JsonProcessingException e) {
      LOG.error("Failed serializing cluster information for database write", e);
      throw new ReaperException(e);
    }
    return true;
  }

  @Override
  public boolean updateCluster(Cluster newCluster) throws ReaperException {
    return addCluster(newCluster);
  }

  @Override
  public Optional<Cluster> getCluster(String clusterName) throws ReaperException {
    Row row = session.execute(getClusterPrepStmt.bind(clusterName)).one();

    try {
      ClusterProperties properties
          = row != null && row.getString("properties") != null
              ? objectMapper.readValue(row.getString("properties"), ClusterProperties.class)
              : ClusterProperties.builder().withJmxPort(Cluster.DEFAULT_JMX_PORT).build();
      return row != null
          ? Optional.ofNullable(
              new Cluster(
                  row.getString("name"),
                  Optional.ofNullable(row.getString("partitioner")),
                  row.getSet("seed_hosts", String.class),
                  properties))
          : Optional.empty();
    } catch (RuntimeException | IOException e) {
      LOG.error("Failed parsing cluster information from the database entry", e);
      throw new ReaperException(e);
    }
  }

  @Override
  public Optional<Cluster> deleteCluster(String clusterName) {
    assert getRepairSchedulesForCluster(clusterName).isEmpty()
        : StringUtils.join(getRepairSchedulesForCluster(clusterName));

    assert getRepairRunsForCluster(clusterName, Optional.of(Integer.MAX_VALUE)).isEmpty()
        : StringUtils.join(getRepairRunsForCluster(clusterName, Optional.of(Integer.MAX_VALUE)));

    Statement stmt = new SimpleStatement(SELECT_REPAIR_UNIT);
    stmt.setIdempotent(Boolean.TRUE);
    ResultSet results = session.execute(stmt);
    for (Row row : results) {
      if (row.getString("cluster_name").equals(clusterName)) {
        UUID id = row.getUUID("id");
        assert getRepairRunsForUnit(id).isEmpty() : StringUtils.join(getRepairRunsForUnit(id));
        session.executeAsync(deleteRepairUnitPrepStmt.bind(id));
      }
    }
    session.executeAsync(deleteClusterPrepStmt.bind(clusterName));
    return Optional.ofNullable(new Cluster(clusterName, null, null));
  }

  @Override
  public RepairRun addRepairRun(Builder repairRun, Collection<RepairSegment.Builder> newSegments) {
    RepairRun newRepairRun = repairRun.build(UUIDs.timeBased());
    BatchStatement repairRunBatch = new BatchStatement(BatchStatement.Type.UNLOGGED);
    List<ResultSetFuture> futures = Lists.newArrayList();
    Boolean isIncremental = null;

    repairRunBatch.add(
        insertRepairRunPrepStmt.bind(
            newRepairRun.getId(),
            newRepairRun.getClusterName(),
            newRepairRun.getRepairUnitId(),
            newRepairRun.getCause(),
            newRepairRun.getOwner(),
            newRepairRun.getRunState().toString(),
            newRepairRun.getCreationTime(),
            newRepairRun.getStartTime(),
            newRepairRun.getEndTime(),
            newRepairRun.getPauseTime(),
            newRepairRun.getIntensity(),
            newRepairRun.getLastEvent(),
            newRepairRun.getSegmentCount(),
            newRepairRun.getRepairParallelism().toString()));

    int nbRanges = 0;
    for (RepairSegment.Builder builder : newSegments) {
      RepairSegment segment = builder.withRunId(newRepairRun.getId()).withId(UUIDs.timeBased()).build();
      isIncremental = null == isIncremental ? null != segment.getCoordinatorHost() : isIncremental;

      assert RepairSegment.State.NOT_STARTED == segment.getState();
      assert null == segment.getStartTime();
      assert null == segment.getEndTime();
      assert 0 == segment.getFailCount();
      assert (null != segment.getCoordinatorHost()) == isIncremental;

      if (isIncremental) {
        repairRunBatch.add(
            insertRepairSegmentIncrementalPrepStmt.bind(
              segment.getRunId(),
              segment.getId(),
              segment.getRepairUnitId(),
              segment.getStartToken(),
              segment.getEndToken(),
              segment.getState().ordinal(),
              segment.getCoordinatorHost(),
              segment.getFailCount()));
      } else {
        try {
          repairRunBatch.add(
              insertRepairSegmentPrepStmt.bind(
                  segment.getRunId(),
                  segment.getId(),
                  segment.getRepairUnitId(),
                  segment.getStartToken(),
                  segment.getEndToken(),
                  segment.getState().ordinal(),
                  segment.getFailCount(),
                  objectMapper.writeValueAsString(segment.getTokenRange().getTokenRanges())));
        } catch (JsonProcessingException e) {
          throw new IllegalStateException(e);
        }
      }

      nbRanges += segment.getTokenRange().getTokenRanges().size();

      if (100 <= nbRanges) {
        // Limit batch size to prevent queries being rejected
        futures.add(session.executeAsync(repairRunBatch));
        repairRunBatch = new BatchStatement(BatchStatement.Type.UNLOGGED);
        nbRanges = 0;
      }
    }
    assert getRepairUnit(newRepairRun.getRepairUnitId()).getIncrementalRepair() == isIncremental.booleanValue();

    futures.add(session.executeAsync(repairRunBatch));
    futures.add(
        session.executeAsync(
            insertRepairRunClusterIndexPrepStmt.bind(newRepairRun.getClusterName(), newRepairRun.getId())));
    futures.add(
        session.executeAsync(
            insertRepairRunUnitIndexPrepStmt.bind(newRepairRun.getRepairUnitId(), newRepairRun.getId())));

    try {
      Futures.allAsList(futures).get();
    } catch (InterruptedException | ExecutionException ex) {
      LOG.error("failed to quorum insert new repair run " + newRepairRun.getId(), ex);
    }
    return newRepairRun;
  }

  @Override
  public boolean updateRepairRun(RepairRun repairRun) {
    session.execute(
          insertRepairRunPrepStmt.bind(
              repairRun.getId(),
              repairRun.getClusterName(),
              repairRun.getRepairUnitId(),
              repairRun.getCause(),
              repairRun.getOwner(),
              repairRun.getRunState().toString(),
              repairRun.getCreationTime(),
              repairRun.getStartTime(),
              repairRun.getEndTime(),
              repairRun.getPauseTime(),
              repairRun.getIntensity(),
              repairRun.getLastEvent(),
              repairRun.getSegmentCount(),
            repairRun.getRepairParallelism().toString()));
    return true;
  }

  @Override
  public Optional<RepairRun> getRepairRun(UUID id) {
    RepairRun repairRun = null;
    Row repairRunResult = session.execute(getRepairRunPrepStmt.bind(id)).one();
    if (repairRunResult != null) {
      try {
        repairRun = buildRepairRunFromRow(repairRunResult, id);
      } catch (RuntimeException ignore) {
        // has been since deleted, but zombie segments has been re-inserted
      }
    }
    return Optional.ofNullable(repairRun);
  }

  @Override
  public Collection<RepairRun> getRepairRunsForCluster(String clusterName, Optional<Integer> limit) {
    List<ResultSetFuture> repairRunFutures = Lists.<ResultSetFuture>newArrayList();

    // Grab all ids for the given cluster name
    Collection<UUID> repairRunIds = getRepairRunIdsForCluster(clusterName);
    // Grab repair runs asynchronously for all the ids returned by the index table
    for (UUID repairRunId : repairRunIds) {
      repairRunFutures.add(session.executeAsync(getRepairRunPrepStmt.bind(repairRunId)));
      if (repairRunFutures.size() == limit.orElse(1000)) {
        break;
      }
    }

    return getRepairRunsAsync(repairRunFutures);
  }

  @Override
  public Collection<RepairRun> getRepairRunsForUnit(UUID repairUnitId) {
    List<ResultSetFuture> repairRunFutures = Lists.<ResultSetFuture>newArrayList();

    // Grab all ids for the given cluster name
    ResultSet repairRunIds = session.execute(getRepairRunForUnitPrepStmt.bind(repairUnitId));

    // Grab repair runs asynchronously for all the ids returned by the index table
    for (Row repairRunId : repairRunIds) {
      repairRunFutures.add(session.executeAsync(getRepairRunPrepStmt.bind(repairRunId.getUUID("id"))));
    }

    return getRepairRunsAsync(repairRunFutures);
  }

  /**
   * Create a collection of RepairRun objects out of a list of ResultSetFuture. Used to handle async queries on the
   * repair_run table with a list of ids.
   */
  private Collection<RepairRun> getRepairRunsAsync(List<ResultSetFuture> repairRunFutures) {
    Collection<RepairRun> repairRuns = Lists.<RepairRun>newArrayList();

    for (ResultSetFuture repairRunFuture : repairRunFutures) {
      Row repairRunResult = repairRunFuture.getUninterruptibly().one();
      if (repairRunResult != null) {
        RepairRun repairRun = buildRepairRunFromRow(repairRunResult, repairRunResult.getUUID("id"));
        repairRuns.add(repairRun);
      }
    }

    return repairRuns;
  }

  @Override
  public Collection<RepairRun> getRepairRunsWithState(RunState runState) throws ReaperException {
    Set<RepairRun> repairRunsWithState = Sets.newHashSet();

    List<Collection<UUID>> repairRunIds = getClusters()
        .stream()
        // Grab all ids for the given cluster name
        .map(cluster -> getRepairRunIdsForCluster(cluster.getName()))
        .collect(Collectors.toList());

    for (Collection<UUID> clusterRepairRunIds : repairRunIds) {
      repairRunsWithState.addAll(getRepairRunsWithStateForCluster(clusterRepairRunIds, runState));
    }

    return repairRunsWithState;
  }

  private Collection<? extends RepairRun> getRepairRunsWithStateForCluster(
      Collection<UUID> clusterRepairRunsId,
      RunState runState) {

    Collection<RepairRun> repairRuns = Sets.newHashSet();
    List<ResultSetFuture> futures = Lists.newArrayList();

    for (UUID repairRunId : clusterRepairRunsId) {
      futures.add(session.executeAsync(getRepairRunPrepStmt.bind(repairRunId)));
    }

    for (ResultSetFuture future : futures) {
      ResultSet repairRunResult = future.getUninterruptibly();
      for (Row row : repairRunResult) {
        repairRuns.add(buildRepairRunFromRow(row, row.getUUID("id")));
      }
    }

    return repairRuns.stream().filter(repairRun -> repairRun.getRunState() == runState).collect(Collectors.toSet());
  }

  @Override
  public Optional<RepairRun> deleteRepairRun(UUID id) {
    Optional<RepairRun> repairRun = getRepairRun(id);
    if (repairRun.isPresent()) {
      session.executeAsync(deleteRepairRunByUnitPrepStmt.bind(id, repairRun.get().getRepairUnitId()));
      session.executeAsync(deleteRepairRunByClusterPrepStmt.bind(id, repairRun.get().getClusterName()));
    }
    session.executeAsync(deleteRepairRunPrepStmt.bind(id));
    return repairRun;
  }

  @Override
  public RepairUnit addRepairUnit(RepairUnit.Builder newRepairUnit) {
    RepairUnit repairUnit = newRepairUnit.build(UUIDs.timeBased());
    session.execute(
        insertRepairUnitPrepStmt.bind(
            repairUnit.getId(),
            repairUnit.getClusterName(),
            repairUnit.getKeyspaceName(),
            repairUnit.getColumnFamilies(),
            repairUnit.getIncrementalRepair(),
            repairUnit.getNodes(),
            repairUnit.getDatacenters(),
            repairUnit.getBlacklistedTables(),
            repairUnit.getRepairThreadCount()));

    repairUnits.put(repairUnit.getId(), repairUnit);
    return repairUnit;
  }

  private RepairUnit getRepairUnitImpl(UUID id) {
    Row repairUnitRow = session.execute(getRepairUnitPrepStmt.bind(id)).one();
    if (repairUnitRow != null) {
      return RepairUnit.builder()
              .clusterName(repairUnitRow.getString("cluster_name"))
              .keyspaceName(repairUnitRow.getString("keyspace_name"))
              .columnFamilies(repairUnitRow.getSet("column_families", String.class))
              .incrementalRepair(repairUnitRow.getBool("incremental_repair"))
              .nodes(repairUnitRow.getSet("nodes", String.class))
              .datacenters(repairUnitRow.getSet("datacenters", String.class))
              .blacklistedTables(repairUnitRow.getSet("blacklisted_tables", String.class))
              .repairThreadCount(repairUnitRow.getInt("repair_thread_count"))
              .build(id);
    }
    throw new IllegalArgumentException("No repair unit exists for " + id);
  }

  @Override
  public RepairUnit getRepairUnit(UUID id) {
    return repairUnits.getUnchecked(id);
  }

  @Override
  public Optional<RepairUnit> getRepairUnit(RepairUnit.Builder params) {
    // brute force again
    RepairUnit repairUnit = null;
    Statement stmt = new SimpleStatement(SELECT_REPAIR_UNIT);
    stmt.setIdempotent(Boolean.TRUE);
    ResultSet results = session.execute(stmt);
    for (Row repairUnitRow : results) {
      if (repairUnitRow.getString("cluster_name").equals(params.clusterName)
          && repairUnitRow.getString("keyspace_name").equals(params.keyspaceName)
          && repairUnitRow.getSet("column_families", String.class).equals(params.columnFamilies)
          && repairUnitRow.getBool("incremental_repair") == params.incrementalRepair
          && repairUnitRow.getSet("nodes", String.class).equals(params.nodes)
          && repairUnitRow.getSet("datacenters", String.class).equals(params.datacenters)
          && repairUnitRow
              .getSet("blacklisted_tables", String.class)
              .equals(params.blacklistedTables)
          && repairUnitRow.getInt("repair_thread_count") == params.repairThreadCount) {

        repairUnit = RepairUnit.builder()
            .clusterName(repairUnitRow.getString("cluster_name"))
            .keyspaceName(repairUnitRow.getString("keyspace_name"))
            .columnFamilies(repairUnitRow.getSet("column_families", String.class))
            .incrementalRepair(repairUnitRow.getBool("incremental_repair"))
            .nodes(repairUnitRow.getSet("nodes", String.class))
            .datacenters(repairUnitRow.getSet("datacenters", String.class))
            .blacklistedTables(repairUnitRow.getSet("blacklisted_tables", String.class))
            .repairThreadCount(repairUnitRow.getInt("repair_thread_count"))
            .build(repairUnitRow.getUUID("id"));
        // exit the loop once we find a match
        break;
      }
    }

    return Optional.ofNullable(repairUnit);
  }

  @Override
  public boolean updateRepairSegment(RepairSegment segment) {

    assert hasLeadOnSegment(segment.getId())
        || (hasLeadOnSegment(segment.getRunId())
          && getRepairUnit(segment.getRepairUnitId()).getIncrementalRepair())
        : "non-leader trying to update repair segment " + segment.getId() + " of run " + segment.getRunId();

    BatchStatement updateRepairSegmentBatch = new BatchStatement(BatchStatement.Type.UNLOGGED);

    updateRepairSegmentBatch.add(
        updateRepairSegmentPrepStmt.bind(
            segment.getRunId(),
            segment.getId(),
            segment.getState().ordinal(),
            segment.getCoordinatorHost(),
            segment.hasStartTime() ? segment.getStartTime().toDate() : null,
            segment.getFailCount()));

    if (null != segment.getEndTime() || State.NOT_STARTED == segment.getState()) {

      Preconditions.checkArgument(
          RepairSegment.State.RUNNING != segment.getState() ,
          "un/setting endTime not permitted when state is RUNNING");

      Preconditions.checkArgument(
          RepairSegment.State.NOT_STARTED != segment.getState() || !segment.hasEndTime(),
          "endTime can only be nulled when state is NOT_STARTED");

      Preconditions.checkArgument(
          RepairSegment.State.DONE != segment.getState() || segment.hasEndTime(),
          "endTime can't be null when state is DONE");

      updateRepairSegmentBatch.add(
          insertRepairSegmentEndTimePrepStmt.bind(
              segment.getRunId(),
              segment.getId(),
              segment.hasEndTime() ? segment.getEndTime().toDate() : null));

    } else if (State.STARTED == segment.getState()) {
      updateRepairSegmentBatch.setConsistencyLevel(ConsistencyLevel.EACH_QUORUM);
    }
    session.execute(updateRepairSegmentBatch);
    return true;
  }

  @Override
  public Optional<RepairSegment> getRepairSegment(UUID runId, UUID segmentId) {
    RepairSegment segment = null;
    Row segmentRow = session.execute(getRepairSegmentPrepStmt.bind(runId, segmentId)).one();
    if (segmentRow != null) {
      segment = createRepairSegmentFromRow(segmentRow);
    }

    return Optional.ofNullable(segment);
  }

  @Override
  public Collection<RepairSegment> getRepairSegmentsForRun(UUID runId) {
    Collection<RepairSegment> segments = Lists.newArrayList();
    // First gather segments ids
    ResultSet segmentsIdResultSet = session.execute(getRepairSegmentsByRunIdPrepStmt.bind(runId));
    for (Row segmentRow : segmentsIdResultSet) {
      segments.add(createRepairSegmentFromRow(segmentRow));
    }

    return segments;
  }

  private static boolean segmentIsWithinRange(RepairSegment segment, RingRange range) {
    return range.encloses(new RingRange(segment.getStartToken(), segment.getEndToken()));
  }

  private static RepairSegment createRepairSegmentFromRow(Row segmentRow) {

    List<RingRange> tokenRanges
        = JsonParseUtils.parseRingRangeList(Optional.ofNullable(segmentRow.getString("token_ranges")));

    Segment.Builder segmentBuilder = Segment.builder();

    if (tokenRanges.size() > 0) {
      segmentBuilder.withTokenRanges(tokenRanges);
    } else {
      // legacy path, for segments that don't have a token range list
      segmentBuilder.withTokenRange(
          new RingRange(
              new BigInteger(segmentRow.getVarint("start_token") + ""),
              new BigInteger(segmentRow.getVarint("end_token") + "")));
    }

    RepairSegment.Builder builder
        = RepairSegment.builder(segmentBuilder.build(), segmentRow.getUUID("repair_unit_id"))
            .withRunId(segmentRow.getUUID("id"))
            .withState(State.values()[segmentRow.getInt("segment_state")])
            .withFailCount(segmentRow.getInt("fail_count"));

    if (null != segmentRow.getString("coordinator_host")) {
      builder = builder.withCoordinatorHost(segmentRow.getString("coordinator_host"));
    }
    if (null != segmentRow.getTimestamp("segment_start_time")) {
      builder = builder.withStartTime(new DateTime(segmentRow.getTimestamp("segment_start_time")));
    }
    if (null != segmentRow.getTimestamp("segment_end_time")) {
      builder = builder.withEndTime(new DateTime(segmentRow.getTimestamp("segment_end_time")));
    }
    return builder.withId(segmentRow.getUUID("segment_id")).build();
  }

  @Override
  public Optional<RepairSegment> getNextFreeSegmentInRange(UUID runId, Optional<RingRange> range) {
    List<RepairSegment> segments = Lists.<RepairSegment>newArrayList(getRepairSegmentsForRun(runId));
    Collections.shuffle(segments);

    for (RepairSegment seg : segments) {
      if (seg.getState().equals(State.NOT_STARTED) && withinRange(seg, range)) {
        return Optional.of(seg);
      }
    }
    return Optional.empty();
  }

  @Override
  public Collection<RepairSegment> getSegmentsWithState(UUID runId, State segmentState) {
    Collection<RepairSegment> segments = Lists.newArrayList();

    Statement statement = null != getRepairSegmentsByRunIdAndStatePrepStmt
        ? getRepairSegmentsByRunIdAndStatePrepStmt.bind(runId, segmentState.ordinal())
        // legacy mode for Cassandra-2 backends
        : getRepairSegmentsByRunIdPrepStmt.bind(runId);

    if (State.STARTED == segmentState) {
      statement = statement.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
    }
    for (Row segmentRow : session.execute(statement)) {
      if (segmentRow.getInt("segment_state") == segmentState.ordinal()) {
        segments.add(createRepairSegmentFromRow(segmentRow));
      }
    }
    return segments;
  }

  @Override
  public Collection<RepairParameters> getOngoingRepairsInCluster(String clusterName) {
    Collection<RepairParameters> repairs = Lists.<RepairParameters>newArrayList();
    Collection<RepairRun> repairRuns = getRepairRunsForCluster(clusterName, Optional.empty());

    for (RepairRun repairRun : repairRuns) {
      Collection<RepairSegment> runningSegments = getSegmentsWithState(repairRun.getId(), State.RUNNING);
      for (RepairSegment segment : runningSegments) {
        RepairUnit repairUnit = getRepairUnit(repairRun.getRepairUnitId());
        repairs.add(
            new RepairParameters(
                Segment.builder().withTokenRanges(segment.getTokenRange().getTokenRanges()).build(),
                repairUnit.getKeyspaceName(),
                repairUnit.getColumnFamilies(),
                repairRun.getRepairParallelism()));
      }
    }

    LOG.trace("found ongoing repairs {} {}", repairs.size(), repairs);

    return repairs;
  }

  @Override
  public SortedSet<UUID> getRepairRunIdsForCluster(String clusterName) {
    SortedSet<UUID> repairRunIds = Sets.newTreeSet((u0, u1) -> (int)(u0.timestamp() - u1.timestamp()));
    ResultSet results = session.execute(getRepairRunForClusterPrepStmt.bind(clusterName));
    for (Row result : results) {
      repairRunIds.add(result.getUUID("id"));
    }

    LOG.trace("repairRunIds : {}", repairRunIds);
    return repairRunIds;
  }

  @Override
  public int getSegmentAmountForRepairRun(UUID runId) {
    return (int) session
        .execute(getRepairSegmentCountByRunIdPrepStmt.bind(runId))
        .one()
        .getLong(0);
  }

  @Override
  public int getSegmentAmountForRepairRunWithState(UUID runId, State state) {
    if (null != getRepairSegmentCountByRunIdAndStatePrepStmt) {
      return (int) session
          .execute(getRepairSegmentCountByRunIdAndStatePrepStmt.bind(runId, state.ordinal()))
          .one()
          .getLong(0);
    } else {
      // legacy mode for Cassandra-2 backends
      return getSegmentsWithState(runId, state).size();
    }
  }

  @Override
  public RepairSchedule addRepairSchedule(io.cassandrareaper.core.RepairSchedule.Builder repairSchedule) {
    RepairSchedule schedule = repairSchedule.build(UUIDs.timeBased());
    updateRepairSchedule(schedule);

    return schedule;
  }

  @Override
  public Optional<RepairSchedule> getRepairSchedule(UUID repairScheduleId) {
    Row sched = session.execute(getRepairSchedulePrepStmt.bind(repairScheduleId)).one();

    return sched != null ? Optional.ofNullable(createRepairScheduleFromRow(sched)) : Optional.empty();
  }

  private RepairSchedule createRepairScheduleFromRow(Row repairScheduleRow) {
    return RepairSchedule.builder(repairScheduleRow.getUUID("repair_unit_id"))
        .state(RepairSchedule.State.valueOf(repairScheduleRow.getString("state")))
        .daysBetween(repairScheduleRow.getInt("days_between"))
        .nextActivation(new DateTime(repairScheduleRow.getTimestamp("next_activation")))
        .runHistory(ImmutableList.copyOf(repairScheduleRow.getSet("run_history", UUID.class)))
        .segmentCount(repairScheduleRow.getInt("segment_count"))
        .repairParallelism(RepairParallelism.fromName(repairScheduleRow.getString("repair_parallelism")))
        .intensity(repairScheduleRow.getDouble("intensity"))
        .creationTime(new DateTime(repairScheduleRow.getTimestamp("creation_time")))
        .segmentCountPerNode(repairScheduleRow.getInt("segment_count_per_node"))
        .owner(repairScheduleRow.getString("owner"))
        .pauseTime(new DateTime(repairScheduleRow.getTimestamp("pause_time")))
        .build(repairScheduleRow.getUUID("id"));
  }

  @Override
  public Collection<RepairSchedule> getRepairSchedulesForCluster(String clusterName) {
    Collection<RepairSchedule> schedules = Lists.<RepairSchedule>newArrayList();
    ResultSet scheduleIds = session.execute(getRepairScheduleByClusterAndKsPrepStmt.bind(clusterName, " "));
    for (Row scheduleId : scheduleIds) {
      Optional<RepairSchedule> schedule = getRepairSchedule(scheduleId.getUUID("repair_schedule_id"));
      if (schedule.isPresent()) {
        schedules.add(schedule.get());
      }
    }

    return schedules;
  }

  @Override
  public Collection<RepairSchedule> getRepairSchedulesForKeyspace(String keyspaceName) {
    Collection<RepairSchedule> schedules = Lists.<RepairSchedule>newArrayList();
    ResultSet scheduleIds = session.execute(getRepairScheduleByClusterAndKsPrepStmt.bind(" ", keyspaceName));
    for (Row scheduleId : scheduleIds) {
      Optional<RepairSchedule> schedule = getRepairSchedule(scheduleId.getUUID("repair_schedule_id"));
      if (schedule.isPresent()) {
        schedules.add(schedule.get());
      }
    }

    return schedules;
  }

  @Override
  public Collection<RepairSchedule> getRepairSchedulesForClusterAndKeyspace(String clusterName, String keyspaceName) {
    Collection<RepairSchedule> schedules = Lists.<RepairSchedule>newArrayList();
    ResultSet scheduleIds = session.execute(getRepairScheduleByClusterAndKsPrepStmt.bind(clusterName, keyspaceName));
    for (Row scheduleId : scheduleIds) {
      Optional<RepairSchedule> schedule = getRepairSchedule(scheduleId.getUUID("repair_schedule_id"));
      if (schedule.isPresent()) {
        schedules.add(schedule.get());
      }
    }

    return schedules;
  }

  @Override
  public Collection<RepairSchedule> getAllRepairSchedules() {
    Collection<RepairSchedule> schedules = Lists.<RepairSchedule>newArrayList();
    Statement stmt = new SimpleStatement(SELECT_REPAIR_SCHEDULE);
    stmt.setIdempotent(Boolean.TRUE);
    ResultSet scheduleResults = session.execute(stmt);
    for (Row scheduleRow : scheduleResults) {
      schedules.add(createRepairScheduleFromRow(scheduleRow));
    }

    return schedules;
  }

  @Override
  public boolean updateRepairSchedule(RepairSchedule newRepairSchedule) {
    final Set<UUID> repairHistory = Sets.newHashSet();
    repairHistory.addAll(newRepairSchedule.getRunHistory());
    RepairUnit repairUnit = getRepairUnit(newRepairSchedule.getRepairUnitId());
    List<ResultSetFuture> futures = Lists.newArrayList();

    futures.add(
        session.executeAsync(
            insertRepairSchedulePrepStmt.bind(
                newRepairSchedule.getId(),
                newRepairSchedule.getRepairUnitId(),
                newRepairSchedule.getState().toString(),
                newRepairSchedule.getDaysBetween(),
                newRepairSchedule.getNextActivation(),
                repairHistory,
                newRepairSchedule.getSegmentCount(),
                newRepairSchedule.getRepairParallelism().toString(),
                newRepairSchedule.getIntensity(),
                newRepairSchedule.getCreationTime(),
                newRepairSchedule.getOwner(),
                newRepairSchedule.getPauseTime(),
                newRepairSchedule.getSegmentCountPerNode())));

    futures.add(
        session.executeAsync(
            insertRepairScheduleByClusterAndKsPrepStmt.bind(
                repairUnit.getClusterName(), repairUnit.getKeyspaceName(), newRepairSchedule.getId())));

    futures.add(
        session.executeAsync(
            insertRepairScheduleByClusterAndKsPrepStmt.bind(
                repairUnit.getClusterName(), " ", newRepairSchedule.getId())));

    futures.add(
        session.executeAsync(
            insertRepairScheduleByClusterAndKsPrepStmt.bind(
                " ", repairUnit.getKeyspaceName(), newRepairSchedule.getId())));

    try {
      Futures.allAsList(futures).get();
    } catch (InterruptedException | ExecutionException ex) {
      LOG.error("failed to quorum update repair schedule " + newRepairSchedule.getId(), ex);
    }

    return true;
  }

  @Override
  public Optional<RepairSchedule> deleteRepairSchedule(UUID id) {
    Optional<RepairSchedule> repairSchedule = getRepairSchedule(id);
    if (repairSchedule.isPresent()) {
      RepairUnit repairUnit = getRepairUnit(repairSchedule.get().getRepairUnitId());

      session.executeAsync(
          deleteRepairScheduleByClusterAndKsPrepStmt.bind(
              repairUnit.getClusterName(), repairUnit.getKeyspaceName(), repairSchedule.get().getId()));

      session.executeAsync(
          deleteRepairScheduleByClusterAndKsPrepStmt.bind(
              repairUnit.getClusterName(), " ", repairSchedule.get().getId()));

      session.executeAsync(
          deleteRepairScheduleByClusterAndKsPrepStmt.bind(
              " ", repairUnit.getKeyspaceName(), repairSchedule.get().getId()));

      session.executeAsync(deleteRepairSchedulePrepStmt.bind(repairSchedule.get().getId()));
    }

    return repairSchedule;
  }

  @Override
  public Collection<RepairRunStatus> getClusterRunStatuses(String clusterName, int limit) {
    Collection<RepairRunStatus> repairRunStatuses = Lists.<RepairRunStatus>newArrayList();
    Collection<RepairRun> repairRuns = getRepairRunsForCluster(clusterName, Optional.of(limit));
    for (RepairRun repairRun : repairRuns) {
      Collection<RepairSegment> segments = getRepairSegmentsForRun(repairRun.getId());
      RepairUnit repairUnit = getRepairUnit(repairRun.getRepairUnitId());

      int segmentsRepaired
          = (int) segments.stream().filter(seg -> seg.getState().equals(RepairSegment.State.DONE)).count();

      repairRunStatuses.add(new RepairRunStatus(repairRun, repairUnit, segmentsRepaired));
    }

    return repairRunStatuses;
  }

  @Override
  public Collection<RepairScheduleStatus> getClusterScheduleStatuses(String clusterName) {
    Collection<RepairSchedule> repairSchedules = getRepairSchedulesForCluster(clusterName);

    Collection<RepairScheduleStatus> repairScheduleStatuses = repairSchedules
        .stream()
        .map(sched -> new RepairScheduleStatus(sched, getRepairUnit(sched.getRepairUnitId())))
        .collect(Collectors.toList());

    return repairScheduleStatuses;
  }

  private RepairRun buildRepairRunFromRow(Row repairRunResult, UUID id) {
    LOG.trace("buildRepairRunFromRow {} / {}", id, repairRunResult);

    Date startTime = repairRunResult.getTimestamp("start_time");
    Date pauseTime = repairRunResult.getTimestamp("pause_time");
    Date endTime = repairRunResult.getTimestamp("end_time");

    return RepairRun.builder(repairRunResult.getString("cluster_name"), repairRunResult.getUUID("repair_unit_id"))
        .creationTime(new DateTime(repairRunResult.getTimestamp("creation_time")))
        .intensity(repairRunResult.getDouble("intensity"))
        .segmentCount(repairRunResult.getInt("segment_count"))
        .repairParallelism(RepairParallelism.fromName(repairRunResult.getString("repair_parallelism")))
        .cause(repairRunResult.getString("cause"))
        .owner(repairRunResult.getString("owner"))
        .startTime(null != startTime ? new DateTime(startTime) : null)
        .pauseTime(null != pauseTime ? new DateTime(pauseTime) : null)
        .endTime(null != endTime ? new DateTime(endTime) : null)
        .lastEvent(repairRunResult.getString("last_event"))
        .runState(RunState.valueOf(repairRunResult.getString("state")))
        .build(id);
  }

  @Override
  public boolean takeLead(UUID leaderId) {
    LOG.debug("Trying to take lead on segment {}", leaderId);
    ResultSet lwtResult = session.execute(
        takeLeadPrepStmt.bind(leaderId, AppContext.REAPER_INSTANCE_ID, AppContext.REAPER_INSTANCE_ADDRESS));

    if (lwtResult.wasApplied()) {
      LOG.debug("Took lead on segment {}", leaderId);
      return true;
    }

    // Another instance took the lead on the segmen
    LOG.debug("Could not take lead on segment {}", leaderId);
    return false;
  }

  @Override
  public boolean renewLead(UUID leaderId) {
    ResultSet lwtResult = session.execute(
        renewLeadPrepStmt.bind(
            AppContext.REAPER_INSTANCE_ID,
            AppContext.REAPER_INSTANCE_ADDRESS,
            leaderId,
            AppContext.REAPER_INSTANCE_ID));

    if (lwtResult.wasApplied()) {
      LOG.debug("Renewed lead on segment {}", leaderId);
      return true;
    }
    assert false : "Could not renew lead on segment " + leaderId;
    LOG.error("Failed to renew lead on segment {}", leaderId);
    return false;
  }

  @Override
  public List<UUID> getLeaders() {
    return session.execute(new SimpleStatement(SELECT_LEADERS))
        .all()
        .stream()
        .map(leader -> leader.getUUID("leader_id"))
        .collect(Collectors.toList());
  }

  @Override
  public void releaseLead(UUID leaderId) {
    Preconditions.checkNotNull(leaderId);
    ResultSet lwtResult = session.execute(releaseLeadPrepStmt.bind(leaderId, AppContext.REAPER_INSTANCE_ID));

    if (lwtResult.wasApplied()) {
      LOG.debug("Released lead on segment {}", leaderId);
    } else {
      assert false : "Could not release lead on segment " + leaderId;
      LOG.error("Could not release lead on segment {}", leaderId);
    }
  }

  @Override
  public void forceReleaseLead(UUID leaderId) {
    session.execute(forceReleaseLeadPrepStmt.bind(leaderId));

    LOG.debug("Force released lead on segment {}", leaderId);
  }

  private boolean hasLeadOnSegment(UUID leaderId) {
    ResultSet lwtResult = session.execute(
        renewLeadPrepStmt.bind(
            AppContext.REAPER_INSTANCE_ID,
            AppContext.REAPER_INSTANCE_ADDRESS,
            leaderId,
            AppContext.REAPER_INSTANCE_ID));

    return lwtResult.wasApplied();
  }

  @Override
  public void storeNodeMetrics(UUID runId, NodeMetrics nodeMetrics) {
    long minute = TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis());
    storeNodeMetricsImpl(runId, nodeMetrics, minute);
    storeNodeMetricsImpl(runId, nodeMetrics, minute + 1);
    storeNodeMetricsImpl(runId, nodeMetrics, minute + 2);
  }

  private void storeNodeMetricsImpl(UUID runId, NodeMetrics nodeMetrics, long minute) {
    session.executeAsync(
        storeNodeMetricsPrepStmt.bind(
            minute,
            runId,
            nodeMetrics.getNode(),
            nodeMetrics.getDatacenter(),
            nodeMetrics.getCluster(),
            nodeMetrics.isRequested(),
            nodeMetrics.getPendingCompactions(),
            nodeMetrics.hasRepairRunning(),
            nodeMetrics.getActiveAnticompactions()));
  }

  @Override
  public Collection<NodeMetrics> getNodeMetrics(UUID runId) {
    long minute = TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis());

    return session.execute(getNodeMetricsPrepStmt.bind(minute, runId)).all().stream()
        .map((row) -> createNodeMetrics(row))
        .collect(Collectors.toSet());
  }

  @Override
  public Optional<NodeMetrics> getNodeMetrics(UUID runId, String node) {
    long minute = TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis());
    Row row = session.execute(getNodeMetricsByNodePrepStmt.bind(minute, runId, node)).one();
    return null != row ? Optional.of(createNodeMetrics(row)) : Optional.empty();
  }

  @Override
  public void deleteNodeMetrics(UUID runId, String node) {
    long minute = TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis());
    session.executeAsync(delNodeMetricsByNodePrepStmt.bind(minute, runId, node));
  }

  private static NodeMetrics createNodeMetrics(Row row) {
    return NodeMetrics.builder()
        .withNode(row.getString("node"))
        .withDatacenter(row.getString("datacenter"))
        .withCluster(row.getString("cluster"))
        .withRequested(row.getBool("requested"))
        .withPendingCompactions(row.getInt("pending_compactions"))
        .withHasRepairRunning(row.getBool("has_repair_running"))
        .withActiveAnticompactions(row.getInt("active_anticompactions"))
        .build();
  }

  @Override
  public int countRunningReapers() {
    ResultSet result = session.execute(getRunningReapersCountPrepStmt.bind());
    int runningReapers = result.all().size();
    LOG.debug("Running reapers = {}", runningReapers);
    return runningReapers > 0 ? runningReapers : 1;
  }

  @Override
  public void saveHeartbeat() {
    session.executeAsync(
        saveHeartbeatPrepStmt.bind(AppContext.REAPER_INSTANCE_ID, AppContext.REAPER_INSTANCE_ADDRESS));
  }


  private static void overrideQueryOptions(CassandraFactory cassandraFactory) {
    // all INSERT and DELETE stmt prepared in this class are idempoten
    if (cassandraFactory.getQueryOptions().isPresent()
        && ConsistencyLevel.LOCAL_ONE != cassandraFactory.getQueryOptions().get().getConsistencyLevel()) {
      LOG.warn("Customization of cassandra's queryOptions is not supported and will be overridden");
    }
    cassandraFactory.setQueryOptions(java.util.Optional.of(new QueryOptions().setDefaultIdempotence(true)));
  }

  private static void overrideRetryPolicy(CassandraFactory cassandraFactory) {
    if (cassandraFactory.getRetryPolicy().isPresent()) {
      LOG.warn("Customization of cassandra's retry policy is not supported and will be overridden");
    }
    cassandraFactory.setRetryPolicy(java.util.Optional.of((RetryPolicyFactory) () -> new RetryPolicyImpl()));
  }

  private static void overridePoolingOptions(CassandraFactory cassandraFactory) {
    PoolingOptionsFactory newPoolingOptionsFactory = new PoolingOptionsFactory() {
      @Override
      public PoolingOptions build() {
        if (null == getPoolTimeout()) {
          setPoolTimeout(Duration.minutes(2));
        }
        return super.build().setMaxQueueSize(40960);
      }
    };
    cassandraFactory.getPoolingOptions().ifPresent((originalPoolingOptions) -> {
      newPoolingOptionsFactory.setHeartbeatInterval(originalPoolingOptions.getHeartbeatInterval());
      newPoolingOptionsFactory.setIdleTimeout(originalPoolingOptions.getIdleTimeout());
      newPoolingOptionsFactory.setLocal(originalPoolingOptions.getLocal());
      newPoolingOptionsFactory.setRemote(originalPoolingOptions.getRemote());
      newPoolingOptionsFactory.setPoolTimeout(originalPoolingOptions.getPoolTimeout());
    });
    cassandraFactory.setPoolingOptions(java.util.Optional.of(newPoolingOptionsFactory));
  }

  private static boolean withinRange(RepairSegment segment, Optional<RingRange> range) {
    return !range.isPresent() || segmentIsWithinRange(segment, range.get());
  }

  /**
   * Retry all statements.
   *
   * <p>
   * All reaper statements are idempotent. Reaper generates few read and writes requests, so it's ok to keep
   * retrying.
   *
   * <p>
   * Sleep 100 milliseconds in between subsequent read retries. Fail after the tenth read retry.
   *
   * <p>
   * Writes keep retrying forever.
   */
  private static class RetryPolicyImpl implements RetryPolicy {

    @Override
    public RetryDecision onReadTimeout(
        Statement stmt,
        ConsistencyLevel cl,
        int required,
        int received,
        boolean retrieved,
        int retry) {

      if (retry > 1) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException expected) { }
      }
      return null != stmt && Boolean.FALSE != stmt.isIdempotent()
          ? retry < 10 ? RetryDecision.retry(cl) : RetryDecision.rethrow()
          : DefaultRetryPolicy.INSTANCE.onReadTimeout(stmt, cl, required, received, retrieved, retry);
    }

    @Override
    public RetryDecision onWriteTimeout(
        Statement stmt,
        ConsistencyLevel cl,
        WriteType type,
        int required,
        int received,
        int retry) {

      Preconditions.checkState(WriteType.CAS != type ||  ConsistencyLevel.SERIAL == cl);

      return null != stmt && Boolean.FALSE != stmt.isIdempotent()
          ? WriteType.CAS == type ? RetryDecision.retry(ConsistencyLevel.ONE) : RetryDecision.retry(cl)
          : DefaultRetryPolicy.INSTANCE.onWriteTimeout(stmt, cl, type, required, received, retry);
    }

    @Override
    public RetryDecision onUnavailable(Statement stmt, ConsistencyLevel cl, int required, int aliveReplica, int retry) {
      return DefaultRetryPolicy.INSTANCE.onUnavailable(stmt, cl, required, aliveReplica, retry == 1 ? 0 : retry);
    }

    @Override
    public RetryDecision onRequestError(Statement stmt, ConsistencyLevel cl, DriverException ex, int nbRetry) {
      return DefaultRetryPolicy.INSTANCE.onRequestError(stmt, cl, ex, nbRetry);
    }

    @Override
    public void init(com.datastax.driver.core.Cluster cluster) {
    }

    @Override
    public void close() {
    }
  }

  @Override
  public boolean saveSnapshot(Snapshot snapshot) {
    session.execute(
        saveSnapshotPrepStmt.bind(
            snapshot.getClusterName(),
            snapshot.getName(),
            snapshot.getOwner().orElse("reaper"),
            snapshot.getCause().orElse("taken with reaper"),
            snapshot.getCreationDate().get()));

    return true;
  }

  @Override
  public boolean deleteSnapshot(Snapshot snapshot) {
    session.execute(deleteSnapshotPrepStmt.bind(snapshot.getClusterName(), snapshot.getName()));
    return false;
  }

  @Override
  public Snapshot getSnapshot(String clusterName, String snapshotName) {
    Snapshot.Builder snapshotBuilder = Snapshot.builder().withClusterName(clusterName).withName(snapshotName);

    ResultSet result = session.execute(getSnapshotPrepStmt.bind(clusterName, snapshotName));
    for (Row row : result) {
      snapshotBuilder
          .withCause(row.getString("cause"))
          .withOwner(row.getString("owner"))
          .withCreationDate(new DateTime(row.getTimestamp("creation_time")));
    }

    return snapshotBuilder.build();
  }
}
