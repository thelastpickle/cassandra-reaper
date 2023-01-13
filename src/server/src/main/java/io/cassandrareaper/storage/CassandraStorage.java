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

package io.cassandrareaper.storage;

import io.cassandrareaper.AppContext;
import io.cassandrareaper.ReaperApplicationConfiguration;
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.ClusterProperties;
import io.cassandrareaper.core.DiagEventSubscription;
import io.cassandrareaper.core.GenericMetric;
import io.cassandrareaper.core.PercentRepairedMetric;
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
import io.cassandrareaper.service.RingRange;
import io.cassandrareaper.storage.cassandra.DateTimeCodec;
import io.cassandrareaper.storage.cassandra.Migration016;
import io.cassandrareaper.storage.cassandra.Migration021;
import io.cassandrareaper.storage.cassandra.Migration024;
import io.cassandrareaper.storage.cassandra.Migration025;

import java.io.IOException;
import java.math.BigInteger;
import java.time.LocalDate;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
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

import static java.lang.Math.min;

public final class CassandraStorage implements IStorage, IDistributedStorage {

  private static final int METRICS_PARTITIONING_TIME_MINS = 10;
  private static final int LEAD_DURATION = 90;
  private static final int MAX_RETURNED_REPAIR_RUNS = 1000;
  /* Simple stmts */
  private static final String SELECT_CLUSTER = "SELECT * FROM cluster";
  private static final String SELECT_REPAIR_SCHEDULE = "SELECT * FROM repair_schedule_v1";
  private static final String SELECT_REPAIR_UNIT = "SELECT * FROM repair_unit_v1";
  private static final String SELECT_LEADERS = "SELECT * FROM leader";
  private static final String SELECT_RUNNING_REAPERS = "SELECT reaper_instance_id FROM running_reapers";
  private static final DateTimeFormatter TIME_BUCKET_FORMATTER = DateTimeFormat.forPattern("yyyyMMddHHmm");
  private static final Logger LOG = LoggerFactory.getLogger(CassandraStorage.class);
  private static final AtomicBoolean UNINITIALISED = new AtomicBoolean(true);

  private final com.datastax.driver.core.Cluster cassandra;
  private final Session session;
  private final ObjectMapper objectMapper = new ObjectMapper();
  private final VersionNumber version;
  private final UUID reaperInstanceId;
  private final int defaultTimeout;

  private final AtomicReference<Collection<Cluster>> clustersCache = new AtomicReference(Collections.EMPTY_SET);
  private final AtomicLong clustersCacheAge = new AtomicLong(0);

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
  private PreparedStatement insertRepairRunNoStatePrepStmt;
  private PreparedStatement insertRepairRunClusterIndexPrepStmt;
  private PreparedStatement insertRepairRunUnitIndexPrepStmt;
  private PreparedStatement getRepairRunPrepStmt;
  private PreparedStatement getRepairRunForClusterPrepStmt;
  private PreparedStatement getRepairRunForClusterWhereStatusPrepStmt;
  private PreparedStatement getRepairRunForUnitPrepStmt;
  private PreparedStatement deleteRepairRunPrepStmt;
  private PreparedStatement deleteRepairRunByClusterPrepStmt;
  private PreparedStatement deleteRepairRunByClusterByIdPrepStmt;
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
  private PreparedStatement deleteRepairScheduleByClusterAndKsByIdPrepStmt;
  private PreparedStatement takeLeadPrepStmt;
  private PreparedStatement renewLeadPrepStmt;
  private PreparedStatement releaseLeadPrepStmt;
  private PreparedStatement getRunningReapersCountPrepStmt;
  private PreparedStatement saveHeartbeatPrepStmt;
  private PreparedStatement deleteHeartbeatPrepStmt;
  private PreparedStatement storeNodeMetricsPrepStmt;
  private PreparedStatement getNodeMetricsPrepStmt;
  private PreparedStatement getNodeMetricsByNodePrepStmt;
  private PreparedStatement delNodeMetricsByNodePrepStmt;
  private PreparedStatement getSnapshotPrepStmt;
  private PreparedStatement deleteSnapshotPrepStmt;
  private PreparedStatement saveSnapshotPrepStmt;
  private PreparedStatement storeMetricsPrepStmt;
  private PreparedStatement getMetricsForHostPrepStmt;
  private PreparedStatement insertOperationsPrepStmt;
  private PreparedStatement listOperationsForNodePrepStmt;
  private PreparedStatement getDiagnosticEventsPrepStmt;
  private PreparedStatement getDiagnosticEventPrepStmt;
  private PreparedStatement deleteDiagnosticEventPrepStmt;
  private PreparedStatement saveDiagnosticEventPrepStmt;
  private PreparedStatement setRunningRepairsPrepStmt;
  private PreparedStatement getRunningRepairsPrepStmt;
  private PreparedStatement storePercentRepairedForSchedulePrepStmt;
  private PreparedStatement getPercentRepairedForSchedulePrepStmt;

  public CassandraStorage(
      UUID reaperInstanceId,
      ReaperApplicationConfiguration config,
      Environment environment,
      CassandraMode mode) throws ReaperException {

    this.reaperInstanceId = reaperInstanceId;
    this.defaultTimeout = config.getHangingRepairTimeoutMins();
    CassandraFactory cassandraFactory = config.getCassandraFactory();
    overrideQueryOptions(cassandraFactory, mode);
    overrideRetryPolicy(cassandraFactory);
    overridePoolingOptions(cassandraFactory);

    // https://docs.datastax.com/en/developer/java-driver/3.5/manual/metrics/#metrics-4-compatibility
    cassandraFactory.setJmxEnabled(false);
    if (!CassandraStorage.UNINITIALISED.compareAndSet(true, false)) {
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

    version = cassandra.getMetadata().getAllHosts()
        .stream()
        .map(h -> h.getCassandraVersion())
        .min(VersionNumber::compareTo)
        .get();

    boolean skipMigration = System.getenv().containsKey("REAPER_SKIP_SCHEMA_MIGRATION")
        ? Boolean.parseBoolean(System.getenv("REAPER_SKIP_SCHEMA_MIGRATION"))
        : Boolean.FALSE;

    if (skipMigration) {
      LOG.info("Skipping schema migration as requested.");
    } else {
      initializeAndUpgradeSchema(cassandra, session, config, version, mode);
    }
    prepareStatements();
  }

  private static void initializeAndUpgradeSchema(
      com.datastax.driver.core.Cluster cassandra,
      Session session,
      ReaperApplicationConfiguration config,
      VersionNumber version,
      CassandraMode mode) throws ReaperException {

    if (mode.equals(CassandraMode.CASSANDRA)) {
      initializeCassandraSchema(cassandra, session, config, version);
    } else if (mode.equals(CassandraMode.ASTRA)) {
      initializeAstraSchema(cassandra, session, config, version);
    }
  }

  private static void initializeCassandraSchema(
      com.datastax.driver.core.Cluster cassandra,
      Session session,
      ReaperApplicationConfiguration config,
      VersionNumber version
  ) {
    Preconditions.checkState(
            0 >= VersionNumber.parse("2.1").compareTo(version),
            "All Cassandra nodes in Reaper's backend storage must be running version 2.1+");

    try (Database database = new Database(cassandra, config.getCassandraFactory().getKeyspace())) {

      int currentVersion = database.getVersion();
      Preconditions.checkState(
          currentVersion == 0 || currentVersion >= 15,
          "You need to upgrade from Reaper 1.2.2 at least in order to run this version. "
          + "Please upgrade to 1.2.2, or greater, before performing this upgrade.");

      MigrationRepository migrationRepo = new MigrationRepository("db/cassandra");
      if (currentVersion < migrationRepo.getLatestVersion()) {
        LOG.warn("Starting db migration from {} to {}…", currentVersion, migrationRepo.getLatestVersion());

        if (15 <= currentVersion) {
          List<String> otherRunningReapers = session.execute("SELECT reaper_instance_host FROM running_reapers").all()
              .stream()
              .map((row) -> row.getString("reaper_instance_host"))
              .filter((reaperInstanceHost) -> !AppContext.REAPER_INSTANCE_ADDRESS.equals(reaperInstanceHost))
              .collect(Collectors.toList());

          LOG.warn(
              "Database migration is happenning with other reaper instances possibly running. Found {}",
              StringUtils.join(otherRunningReapers));
        }

        // We now only support migrations starting at version 15 (Reaper 1.2.2)
        int startVersion = database.getVersion() == 0 ? 15 : database.getVersion();
        migrate(startVersion, migrationRepo, session, CassandraMode.CASSANDRA);
        // some migration steps depend on the Cassandra version, so must be rerun every startup
        Migration016.migrate(session, config.getCassandraFactory().getKeyspace());
        // Switch metrics table to TWCS if possible, this is intentionally executed every startup
        Migration021.migrate(session, config.getCassandraFactory().getKeyspace());
        // Switch metrics table to TWCS if possible, this is intentionally executed every startup
        Migration024.migrate(session, config.getCassandraFactory().getKeyspace());
        if (database.getVersion() == 25) {
          Migration025.migrate(session, config.getCassandraFactory().getKeyspace());
        }
      } else {
        LOG.info(
            String.format("Keyspace %s already at schema version %d", session.getLoggedKeyspace(), currentVersion));
      }
    }
  }

  private static void initializeAstraSchema(
      com.datastax.driver.core.Cluster cassandra,
      Session session,
      ReaperApplicationConfiguration config,
      VersionNumber version
  ) {
    Preconditions.checkState(
            0 >= VersionNumber.parse("2.1").compareTo(version),
            "All Cassandra nodes in Reaper's backend storage must be running version 2.1+");

    try (Database database = new Database(cassandra, config.getCassandraFactory().getKeyspace())) {

      int currentVersion = database.getVersion();

      MigrationRepository migrationRepo = new MigrationRepository("db/astra");
      if (currentVersion < migrationRepo.getLatestVersion()) {
        LOG.warn("Starting db migration from {} to {}…", currentVersion, migrationRepo.getLatestVersion());

        int startVersion = database.getVersion();
        migrate(startVersion, migrationRepo, session, CassandraMode.ASTRA);
      } else {
        LOG.info(
            String.format("Keyspace %s already at schema version %d", session.getLoggedKeyspace(), currentVersion));
      }
    }
  }

  private static void migrate(
      int dbVersion,
      MigrationRepository repository,
      Session session,
      CassandraMode mode) {
    Preconditions.checkState(dbVersion < repository.getLatestVersion());

    for (int i = dbVersion + 1 ; i <= repository.getLatestVersion(); ++i) {
      final int nextVersion = i;
      String migrationRepoPath = mode.equals(CassandraMode.CASSANDRA) ? "db/cassandra" : "db/astra";
      // perform the migrations one at a time, so the MigrationXXX classes can be executed alongside the scripts
      MigrationRepository migrationRepo = new MigrationRepository(migrationRepoPath) {
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
        MigrationTask migration = new MigrationTask(database, migrationRepo, true);
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
    insertRepairRunPrepStmt = session
        .prepare(
            "INSERT INTO repair_run(id, cluster_name, repair_unit_id, cause, owner, state, creation_time, "
                + "start_time, end_time, pause_time, intensity, last_event, segment_count, repair_parallelism, "
                + "tables, adaptive_schedule) "
                + "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .setConsistencyLevel(ConsistencyLevel.QUORUM);
    insertRepairRunNoStatePrepStmt = session
        .prepare(
            "INSERT INTO repair_run(id, cluster_name, repair_unit_id, cause, owner, creation_time, "
                + "intensity, last_event, segment_count, repair_parallelism, tables, adaptive_schedule) "
                + "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .setConsistencyLevel(ConsistencyLevel.QUORUM);
    insertRepairRunClusterIndexPrepStmt
        = session.prepare("INSERT INTO repair_run_by_cluster_v2(cluster_name, id, repair_run_state) values(?, ?, ?)");
    insertRepairRunUnitIndexPrepStmt
        = session.prepare("INSERT INTO repair_run_by_unit(repair_unit_id, id) values(?, ?)");
    getRepairRunPrepStmt = session
        .prepare(
            "SELECT id,cluster_name,repair_unit_id,cause,owner,state,creation_time,start_time,end_time,"
                + "pause_time,intensity,last_event,segment_count,repair_parallelism,tables,adaptive_schedule "
                + "FROM repair_run WHERE id = ? LIMIT 1")
        .setConsistencyLevel(ConsistencyLevel.QUORUM);
    getRepairRunForClusterPrepStmt = session.prepare(
        "SELECT * FROM repair_run_by_cluster_v2 WHERE cluster_name = ? limit ?");
    getRepairRunForClusterWhereStatusPrepStmt = session.prepare(
        "SELECT id FROM repair_run_by_cluster_v2 WHERE cluster_name = ? AND repair_run_state = ? limit ?");
    getRepairRunForUnitPrepStmt = session.prepare("SELECT * FROM repair_run_by_unit WHERE repair_unit_id = ?");
    deleteRepairRunPrepStmt = session.prepare("DELETE FROM repair_run WHERE id = ?");
    deleteRepairRunByClusterPrepStmt
        = session.prepare("DELETE FROM repair_run_by_cluster_v2 WHERE cluster_name = ?");
    deleteRepairRunByClusterByIdPrepStmt
        = session.prepare("DELETE FROM repair_run_by_cluster_v2 WHERE id = ? and cluster_name = ?");
    deleteRepairRunByUnitPrepStmt = session.prepare("DELETE FROM repair_run_by_unit "
        + "WHERE id = ? and repair_unit_id= ?");
    insertRepairUnitPrepStmt = session
        .prepare(
            "INSERT INTO repair_unit_v1(id, cluster_name, keyspace_name, column_families, "
                + "incremental_repair, nodes, \"datacenters\", blacklisted_tables, repair_thread_count, timeout) "
                + "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .setConsistencyLevel(ConsistencyLevel.QUORUM);
    getRepairUnitPrepStmt = session
        .prepare("SELECT * FROM repair_unit_v1 WHERE id = ?")
        .setConsistencyLevel(ConsistencyLevel.QUORUM);
    deleteRepairUnitPrepStmt = session.prepare("DELETE FROM repair_unit_v1 WHERE id = ?");
    insertRepairSegmentPrepStmt = session
        .prepare(
            "INSERT INTO repair_run"
                + "(id,segment_id,repair_unit_id,start_token,end_token,"
                + " segment_state,fail_count, token_ranges, replicas,host_id)"
                + " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
    insertRepairSegmentIncrementalPrepStmt = session
        .prepare(
            "INSERT INTO repair_run"
                + "(id,segment_id,repair_unit_id,start_token,end_token,"
                + "segment_state,coordinator_host,fail_count,replicas,host_id)"
                + " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
    updateRepairSegmentPrepStmt = session
        .prepare(
            "INSERT INTO repair_run"
                + "(id,segment_id,segment_state,coordinator_host,segment_start_time,fail_count,host_id)"
                + " VALUES(?, ?, ?, ?, ?, ?, ?)")
        .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
    insertRepairSegmentEndTimePrepStmt = session
        .prepare("INSERT INTO repair_run(id, segment_id, segment_end_time) VALUES(?, ?, ?)")
        .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
    getRepairSegmentPrepStmt = session
            .prepare(
                "SELECT id,repair_unit_id,segment_id,start_token,end_token,segment_state,coordinator_host,"
                    + "segment_start_time,segment_end_time,fail_count, token_ranges, replicas, host_id"
                    + " FROM repair_run WHERE id = ? and segment_id = ?")
            .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
    getRepairSegmentsByRunIdPrepStmt = session.prepare(
        "SELECT id,repair_unit_id,segment_id,start_token,end_token,segment_state,coordinator_host,segment_start_time,"
            + "segment_end_time,fail_count, token_ranges, replicas, host_id FROM repair_run WHERE id = ?");
    getRepairSegmentCountByRunIdPrepStmt = session.prepare("SELECT count(*) FROM repair_run WHERE id = ?");
    prepareScheduleStatements();
    prepareLeaderElectionStatements(timeUdf);
    getRunningReapersCountPrepStmt = session.prepare(SELECT_RUNNING_REAPERS);
    saveHeartbeatPrepStmt = session
        .prepare(
            "INSERT INTO running_reapers(reaper_instance_id, reaper_instance_host, last_heartbeat)"
                + " VALUES(?,?," + timeUdf + "(now()))")
        .setIdempotent(false);
    deleteHeartbeatPrepStmt = session
        .prepare(
            "DELETE FROM running_reapers WHERE reaper_instance_id = ?")
        .setIdempotent(true);

    getSnapshotPrepStmt = session.prepare("SELECT * FROM snapshot WHERE cluster = ? and snapshot_name = ?");
    deleteSnapshotPrepStmt = session.prepare("DELETE FROM snapshot WHERE cluster = ? and snapshot_name = ?");
    saveSnapshotPrepStmt = session.prepare(
            "INSERT INTO snapshot (cluster, snapshot_name, owner, cause, creation_time)"
                + " VALUES(?,?,?,?,?)");

    getDiagnosticEventsPrepStmt = session.prepare("SELECT * FROM diagnostic_event_subscription");
    getDiagnosticEventPrepStmt = session.prepare("SELECT * FROM diagnostic_event_subscription WHERE id = ?");
    deleteDiagnosticEventPrepStmt = session.prepare("DELETE FROM diagnostic_event_subscription WHERE id = ?");

    saveDiagnosticEventPrepStmt = session.prepare("INSERT INTO diagnostic_event_subscription "
        + "(id,cluster,description,nodes,events,export_sse,export_file_logger,export_http_endpoint)"
        + " VALUES(?,?,?,?,?,?,?,?)");

    if (0 >= VersionNumber.parse("3.0").compareTo(version)) {
      try {
        getRepairSegmentsByRunIdAndStatePrepStmt = session.prepare(
            "SELECT id,repair_unit_id,segment_id,start_token,end_token,segment_state,coordinator_host,"
                + "segment_start_time,segment_end_time,fail_count, token_ranges, replicas, host_id FROM repair_run "
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
    prepareOperationsStatements();
  }

  private void prepareScheduleStatements() {
    insertRepairSchedulePrepStmt
        = session
            .prepare(
                "INSERT INTO repair_schedule_v1(id, repair_unit_id, state,"
                    + "days_between, next_activation, "
                    + "repair_parallelism, intensity, "
                    + "creation_time, owner, pause_time, segment_count_per_node, "
                    + "adaptive, percent_unrepaired_threshold, last_run) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
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
    deleteRepairScheduleByClusterAndKsByIdPrepStmt = session.prepare(
        "DELETE FROM repair_schedule_by_cluster_and_keyspace "
            + "WHERE cluster_name = ? and keyspace_name = ? and repair_schedule_id = ?");
  }

  private void prepareLeaderElectionStatements(final String timeUdf) {
    takeLeadPrepStmt = session
        .prepare(
            "INSERT INTO leader(leader_id, reaper_instance_id, reaper_instance_host, last_heartbeat)"
                + "VALUES(?, ?, ?, " + timeUdf + "(now())) IF NOT EXISTS USING TTL ?")
        .setConsistencyLevel(ConsistencyLevel.QUORUM);
    renewLeadPrepStmt = session
        .prepare(
            "UPDATE leader USING TTL ? SET reaper_instance_id = ?, reaper_instance_host = ?,"
                + " last_heartbeat = " + timeUdf + "(now()) WHERE leader_id = ? IF reaper_instance_id = ?")
        .setConsistencyLevel(ConsistencyLevel.QUORUM);
    releaseLeadPrepStmt = session.prepare("DELETE FROM leader WHERE leader_id = ? IF reaper_instance_id = ?")
        .setConsistencyLevel(ConsistencyLevel.QUORUM);
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
    storeMetricsPrepStmt
        = session
            .prepare(
                "INSERT INTO node_metrics_v3 (cluster, metric_domain, metric_type, time_bucket, "
                    + "host, metric_scope, metric_name, ts, metric_attribute, value) "
                    + "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
    getMetricsForHostPrepStmt
        = session
            .prepare(
                "SELECT cluster, metric_domain, metric_type, time_bucket, host, "
                    + "metric_scope, metric_name, ts, metric_attribute, value "
                    + "FROM node_metrics_v3 "
                    + "WHERE metric_domain = ? and metric_type = ? and cluster = ? and time_bucket = ? and host = ?");
    setRunningRepairsPrepStmt
      = session
        .prepare(
            "UPDATE running_repairs USING TTL ?"
            + " SET reaper_instance_host = ?, reaper_instance_id = ?, segment_id = ?"
            + " WHERE repair_id = ? AND node = ? IF reaper_instance_id = ?")
        .setSerialConsistencyLevel(ConsistencyLevel.SERIAL)
        .setConsistencyLevel(ConsistencyLevel.QUORUM)
        .setIdempotent(false);

    getRunningRepairsPrepStmt
      = session
      .prepare(
          "select repair_id, node, reaper_instance_host, reaper_instance_id, segment_id"
          + " FROM running_repairs"
          + " WHERE repair_id = ?")
      .setConsistencyLevel(ConsistencyLevel.QUORUM);

    storePercentRepairedForSchedulePrepStmt
      = session
      .prepare(
          "INSERT INTO percent_repaired_by_schedule"
            + " (cluster_name, repair_schedule_id, time_bucket, node, keyspace_name, table_name, percent_repaired, ts)"
            + " values(?, ?, ?, ?, ?, ?, ?, ?)"
      );

    getPercentRepairedForSchedulePrepStmt
      = session
      .prepare(
          "SELECT * FROM percent_repaired_by_schedule"
            + " WHERE cluster_name = ? and repair_schedule_id = ? AND time_bucket = ?"
      );
  }

  private void prepareOperationsStatements() {
    insertOperationsPrepStmt
        = session.prepare(
            "INSERT INTO node_operations(cluster, type, time_bucket, host, ts, data) "
                + "values(?,?,?,?,?,?)");

    listOperationsForNodePrepStmt
        = session.prepare(
            "SELECT cluster, type, time_bucket, host, ts, data FROM node_operations "
                + "WHERE cluster = ? AND type = ? and time_bucket = ? and host = ? LIMIT 1");
  }

  @Override
  public boolean isStorageConnected() {
    return session != null && !session.isClosed();
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
              java.sql.Date.valueOf(cluster.getLastContact())));
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

  private boolean addClusterAssertions(Cluster cluster) {
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

  private Cluster parseCluster(Row row) throws IOException {

    ClusterProperties properties = null != row.getString("properties")
          ? objectMapper.readValue(row.getString("properties"), ClusterProperties.class)
          : ClusterProperties.builder().withJmxPort(Cluster.DEFAULT_JMX_PORT).build();

    LocalDate lastContact = row.getTimestamp("last_contact") == null
        ? LocalDate.MIN
        : new java.sql.Date(row.getTimestamp("last_contact").getTime()).toLocalDate();

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
    getRepairSchedulesForCluster(clusterName).forEach(schedule -> deleteRepairSchedule(schedule.getId()));
    session.executeAsync(deleteRepairRunByClusterPrepStmt.bind(clusterName));

    getEventSubscriptions(clusterName)
        .stream()
        .filter(subscription -> subscription.getId().isPresent())
        .forEach(subscription -> deleteEventSubscription(subscription.getId().get()));

    Statement stmt = new SimpleStatement(SELECT_REPAIR_UNIT);
    stmt.setIdempotent(true);
    ResultSet results = session.execute(stmt);
    for (Row row : results) {
      if (row.getString("cluster_name").equals(clusterName)) {
        UUID id = row.getUUID("id");
        session.executeAsync(deleteRepairUnitPrepStmt.bind(id));
      }
    }
    Cluster cluster = getCluster(clusterName);
    session.execute(deleteClusterPrepStmt.bind(clusterName));
    return cluster;
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
            newRepairRun.getRepairParallelism().toString(),
            newRepairRun.getTables(),
            newRepairRun.getAdaptiveSchedule()));

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
              segment.getFailCount(),
              segment.getReplicas(),
              segment.getHostID()
            )
        );
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
                  objectMapper.writeValueAsString(segment.getTokenRange().getTokenRanges()),
                  segment.getReplicas(),
                  segment.getHostID()
              )
          );
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
    assert getRepairUnit(newRepairRun.getRepairUnitId()).getIncrementalRepair() == isIncremental;

    futures.add(session.executeAsync(repairRunBatch));
    futures.add(
        session.executeAsync(
            insertRepairRunClusterIndexPrepStmt.bind(
                newRepairRun.getClusterName(),
                newRepairRun.getId(),
                newRepairRun.getRunState().toString())));
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
    return updateRepairRun(repairRun, Optional.of(true));
  }

  @Override
  public boolean updateRepairRun(RepairRun repairRun, Optional<Boolean> updateRepairState) {
    if (updateRepairState.orElse(true)) {
      BatchStatement updateRepairRunBatch = new BatchStatement(BatchStatement.Type.LOGGED);
      // Updates of the last event impact the repair state.
      // We want to limit overwrites in this case.
      updateRepairRunBatch.add(
          insertRepairRunClusterIndexPrepStmt.bind(
              repairRun.getClusterName(), repairRun.getId(), repairRun.getRunState().toString()));
      // Repair state will be updated
      updateRepairRunBatch.add(
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
              repairRun.getRepairParallelism().toString(),
              repairRun.getTables(),
              repairRun.getAdaptiveSchedule()));
      session.execute(updateRepairRunBatch);
    } else {
      session.execute(
          insertRepairRunNoStatePrepStmt.bind(
              repairRun.getId(),
              repairRun.getClusterName(),
              repairRun.getRepairUnitId(),
              repairRun.getCause(),
              repairRun.getOwner(),
              repairRun.getCreationTime(),
              repairRun.getIntensity(),
              repairRun.getLastEvent(),
              repairRun.getSegmentCount(),
              repairRun.getRepairParallelism().toString(),
              repairRun.getTables(),
              repairRun.getAdaptiveSchedule()));
    }

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
    Collection<UUID> repairRunIds = getRepairRunIdsForCluster(clusterName, limit);
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
  public List<RepairRun> getRepairRunsForClusterPrioritiseRunning(String clusterName, Optional<Integer> limit) {
    List<ResultSetFuture> repairUuidFuturesByState = Lists.<ResultSetFuture>newArrayList();
    // We've set up the RunState enum so that values are declared in order of "interestingness",
    // we iterate over the table via the secondary index according to that ordering.
    for (RunState state : RepairRun.RunState.values()) {
      repairUuidFuturesByState.add(
          // repairUUIDFutures will be a List of resultSetFutures, each of which contains a ResultSet of
          // UUIDs for one status.
          session
              .executeAsync(getRepairRunForClusterWhereStatusPrepStmt
                  .bind(clusterName, state.toString(), limit.orElse(MAX_RETURNED_REPAIR_RUNS)
                  )
              )
      );
    }
    List<UUID> flattenedUuids = Lists.<UUID>newArrayList();
    // Flatten the UUIDs from each status down into a single array and trim.
    for (ResultSetFuture idResSetFuture : repairUuidFuturesByState) {
      idResSetFuture
          .getUninterruptibly()
          .forEach(
              row -> flattenedUuids.add(row.getUUID("id"))
        );
    }
    flattenedUuids.subList(0, min(flattenedUuids.size(), limit.orElse(MAX_RETURNED_REPAIR_RUNS)) + 1);
    // Run an async query on each UUID in the flattened list, against the main repair_run table with
    // all columns required as an input to `buildRepairRunFromRow`.
    List<ResultSetFuture> repairRunFutures = Lists.<ResultSetFuture>newArrayList();
    flattenedUuids.forEach(uuid ->
        repairRunFutures.add(
            session
                .executeAsync(getRepairRunPrepStmt.bind(uuid)
                )
        )
    );
    // Defuture the repair_run rows and build the strongly typed RepairRun objects from the contents.
    return repairRunFutures
        .stream()
        .map(
            row -> {
              Row extractedRow = row.getUninterruptibly().one();
              return buildRepairRunFromRow(extractedRow, extractedRow.getUUID("id"));
            }
        ).collect(Collectors.toList());
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
  public Collection<RepairRun> getRepairRunsWithState(RunState runState) {
    Set<RepairRun> repairRunsWithState = Sets.newHashSet();

    List<Collection<UUID>> repairRunIds = getClusters()
        .stream()
        // Grab all ids for the given cluster name
        .map(cluster -> getRepairRunIdsForClusterWithState(cluster.getName(), runState))
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
      session.execute(deleteRepairRunByUnitPrepStmt.bind(id, repairRun.get().getRepairUnitId()));
      session.execute(deleteRepairRunByClusterByIdPrepStmt.bind(id, repairRun.get().getClusterName()));
    }
    session.execute(deleteRepairRunPrepStmt.bind(id));
    return repairRun;
  }

  @Override
  public RepairUnit addRepairUnit(RepairUnit.Builder newRepairUnit) {
    RepairUnit repairUnit = newRepairUnit.build(UUIDs.timeBased());
    updateRepairUnit(repairUnit);

    repairUnits.put(repairUnit.getId(), repairUnit);
    return repairUnit;
  }

  @Override
  public void updateRepairUnit(RepairUnit updatedRepairUnit) {
    session.execute(
        insertRepairUnitPrepStmt.bind(
            updatedRepairUnit.getId(),
            updatedRepairUnit.getClusterName(),
            updatedRepairUnit.getKeyspaceName(),
            updatedRepairUnit.getColumnFamilies(),
            updatedRepairUnit.getIncrementalRepair(),
            updatedRepairUnit.getNodes(),
            updatedRepairUnit.getDatacenters(),
            updatedRepairUnit.getBlacklistedTables(),
            updatedRepairUnit.getRepairThreadCount(),
            updatedRepairUnit.getTimeout()));
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
              .timeout(repairUnitRow.isNull("timeout") ? this.defaultTimeout : repairUnitRow.getInt("timeout"))
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
      RepairUnit existingRepairUnit = RepairUnit.builder()
            .clusterName(repairUnitRow.getString("cluster_name"))
            .keyspaceName(repairUnitRow.getString("keyspace_name"))
            .columnFamilies(repairUnitRow.getSet("column_families", String.class))
            .incrementalRepair(repairUnitRow.getBool("incremental_repair"))
            .nodes(repairUnitRow.getSet("nodes", String.class))
            .datacenters(repairUnitRow.getSet("datacenters", String.class))
            .blacklistedTables(repairUnitRow.getSet("blacklisted_tables", String.class))
            .repairThreadCount(repairUnitRow.getInt("repair_thread_count"))
            .timeout(repairUnitRow.isNull("timeout") ? this.defaultTimeout : repairUnitRow.getInt("timeout"))
            .build(repairUnitRow.getUUID("id"));
      if (existingRepairUnit.with().equals(params)) {
        repairUnit = existingRepairUnit;
        LOG.info("Found matching repair unit: {}", repairUnitRow.getUUID("id"));
        // exit the loop once we find a match
        break;
      }
    }

    return Optional.ofNullable(repairUnit);
  }

  @Override
  public boolean updateRepairSegment(RepairSegment segment) {

    assert hasLeadOnSegment(segment)
        || (hasLeadOnSegment(segment.getRunId())
          && getRepairUnit(segment.getRepairUnitId()).getIncrementalRepair())
        : "non-leader trying to update repair segment " + segment.getId() + " of run " + segment.getRunId();

    return updateRepairSegmentUnsafe(segment);
  }

  @Override
  public boolean updateRepairSegmentUnsafe(RepairSegment segment) {

    BatchStatement updateRepairSegmentBatch = new BatchStatement(BatchStatement.Type.UNLOGGED);

    updateRepairSegmentBatch.add(
        updateRepairSegmentPrepStmt.bind(
            segment.getRunId(),
            segment.getId(),
            segment.getState().ordinal(),
            segment.getCoordinatorHost(),
            segment.hasStartTime() ? segment.getStartTime().toDate() : null,
            segment.getFailCount(),
            segment.getHostID()
        )
    );

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
      if (null != segmentRow.getMap("replicas", String.class, String.class)) {
        segmentBuilder = segmentBuilder.withReplicas(segmentRow.getMap("replicas", String.class, String.class));
      }
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
    if (null != segmentRow.getMap("replicas", String.class, String.class)) {
      builder = builder.withReplicas(segmentRow.getMap("replicas", String.class, String.class));
    }

    if (null != segmentRow.getUUID("host_id")) {
      builder = builder.withHostID(segmentRow.getUUID("host_id"));
    }

    return builder.withId(segmentRow.getUUID("segment_id")).build();
  }

  @Override
  public List<RepairSegment> getNextFreeSegments(UUID runId) {
    List<RepairSegment> segments = Lists.<RepairSegment>newArrayList(getRepairSegmentsForRun(runId));
    Collections.shuffle(segments);

    Set<String> lockedNodes = getLockedNodesForRun(runId);
    List<RepairSegment> candidates = segments.stream()
                                             .filter(seg -> segmentIsCandidate(seg, lockedNodes))
                                             .collect(Collectors.toList());
    return candidates;
  }

  @Override
  public List<RepairSegment> getNextFreeSegmentsForRanges(
      UUID runId,
      List<RingRange> ranges) {
    List<RepairSegment> segments
        = Lists.<RepairSegment>newArrayList(getRepairSegmentsForRun(runId));
    Collections.shuffle(segments);
    Set<String> lockedNodes = getLockedNodesForRun(runId);
    List<RepairSegment> candidates = segments.stream()
                                             .filter(seg -> segmentIsCandidate(seg, lockedNodes))
                                             .filter(seg -> segmentIsWithinRanges(seg, ranges))
                                             .collect(Collectors.toList());

    return candidates;
  }

  private boolean segmentIsWithinRanges(RepairSegment seg, List<RingRange> ranges) {
    for (RingRange range : ranges) {
      if (segmentIsWithinRange(seg, range)) {
        return true;
      }
    }

    return false;
  }

  private boolean segmentIsCandidate(RepairSegment seg, Set<String> lockedNodes) {
    return seg.getState().equals(State.NOT_STARTED)
        && Sets.intersection(lockedNodes, seg.getReplicas().keySet()).isEmpty();
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
  public SortedSet<UUID> getRepairRunIdsForCluster(String clusterName, Optional<Integer> limit) {
    SortedSet<UUID> repairRunIds = Sets.newTreeSet((u0, u1) -> (int)(u0.timestamp() - u1.timestamp()));
    ResultSet results = session.execute(getRepairRunForClusterPrepStmt.bind(clusterName, limit.orElse(
        MAX_RETURNED_REPAIR_RUNS)));
    for (Row result : results) {
      repairRunIds.add(result.getUUID("id"));
    }

    LOG.trace("repairRunIds : {}", repairRunIds);
    return repairRunIds;
  }

  private SortedSet<UUID> getRepairRunIdsForClusterWithState(String clusterName, RunState runState) {
    SortedSet<UUID> repairRunIds = Sets.newTreeSet((u0, u1) -> (int)(u0.timestamp() - u1.timestamp()));
    ResultSet results = session.execute(getRepairRunForClusterPrepStmt.bind(clusterName, MAX_RETURNED_REPAIR_RUNS));
    results.all()
           .stream()
           .filter(run -> run.getString("repair_run_state").equals(runState.toString()))
           .map(run -> run.getUUID("id"))
           .forEach(runId -> repairRunIds.add(runId));


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
        .repairParallelism(RepairParallelism.fromName(repairScheduleRow.getString("repair_parallelism")))
        .intensity(repairScheduleRow.getDouble("intensity"))
        .creationTime(new DateTime(repairScheduleRow.getTimestamp("creation_time")))
        .segmentCountPerNode(repairScheduleRow.getInt("segment_count_per_node"))
        .owner(repairScheduleRow.getString("owner"))
        .pauseTime(new DateTime(repairScheduleRow.getTimestamp("pause_time")))
        .adaptive(repairScheduleRow.isNull("adaptive") ? false : repairScheduleRow.getBool("adaptive"))
        .percentUnrepairedThreshold(repairScheduleRow.isNull("percent_unrepaired_threshold")
          ? -1
          : repairScheduleRow.getInt("percent_unrepaired_threshold"))
        .lastRun(repairScheduleRow.getUUID("last_run"))
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
  public Collection<RepairSchedule> getRepairSchedulesForCluster(String clusterName, boolean incremental) {
    return getRepairSchedulesForCluster(clusterName).stream()
        .filter(schedule -> getRepairUnit(schedule.getRepairUnitId()).getIncrementalRepair() == incremental)
        .collect(Collectors.toList());
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
                newRepairSchedule.getRepairParallelism().toString(),
                newRepairSchedule.getIntensity(),
                newRepairSchedule.getCreationTime(),
                newRepairSchedule.getOwner(),
                newRepairSchedule.getPauseTime(),
                newRepairSchedule.getSegmentCountPerNode(),
                newRepairSchedule.getAdaptive(),
                newRepairSchedule.getPercentUnrepairedThreshold(),
                newRepairSchedule.getLastRun())));

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

      session.execute(
          deleteRepairScheduleByClusterAndKsByIdPrepStmt.bind(
              repairUnit.getClusterName(), repairUnit.getKeyspaceName(), repairSchedule.get().getId()));

      session.execute(
          deleteRepairScheduleByClusterAndKsByIdPrepStmt.bind(
              repairUnit.getClusterName(), " ", repairSchedule.get().getId()));

      session.execute(
          deleteRepairScheduleByClusterAndKsByIdPrepStmt.bind(
              " ", repairUnit.getKeyspaceName(), repairSchedule.get().getId()));

      session.execute(deleteRepairSchedulePrepStmt.bind(repairSchedule.get().getId()));
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
        .tables(repairRunResult.getSet("tables", String.class))
        .adaptiveSchedule(repairRunResult.isNull("adaptive_schedule")
            ? false
            : repairRunResult.getBool("adaptive_schedule"))
        .build(id);
  }

  @Override
  public boolean takeLead(UUID leaderId) {
    return takeLead(leaderId, LEAD_DURATION);
  }

  @Override
  public boolean takeLead(UUID leaderId, int ttl) {
    LOG.debug("Trying to take lead on segment {}", leaderId);
    ResultSet lwtResult = session.execute(
        takeLeadPrepStmt.bind(leaderId, reaperInstanceId, AppContext.REAPER_INSTANCE_ADDRESS, ttl));

    if (lwtResult.wasApplied()) {
      LOG.debug("Took lead on segment {}", leaderId);
      return true;
    }

    // Another instance took the lead on the segment
    LOG.debug("Could not take lead on segment {}", leaderId);
    return false;
  }

  @Override
  public boolean renewLead(UUID leaderId) {
    return renewLead(leaderId, LEAD_DURATION);
  }

  @Override
  public boolean renewLead(UUID leaderId, int ttl) {
    ResultSet lwtResult = session.execute(
        renewLeadPrepStmt.bind(
            ttl,
            reaperInstanceId,
            AppContext.REAPER_INSTANCE_ADDRESS,
            leaderId,
            reaperInstanceId));

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
    ResultSet lwtResult = session.execute(releaseLeadPrepStmt.bind(leaderId, reaperInstanceId));
    LOG.info("Trying to release lead on segment {} for instance {}", leaderId, reaperInstanceId);
    if (lwtResult.wasApplied()) {
      LOG.info("Released lead on segment {}", leaderId);
    } else {
      assert false : "Could not release lead on segment " + leaderId;
      LOG.error("Could not release lead on segment {}", leaderId);
    }
  }

  private boolean hasLeadOnSegment(RepairSegment segment) {
    return renewRunningRepairsForNodes(segment.getRunId(), segment.getId(), segment.getReplicas().keySet());
  }

  private boolean hasLeadOnSegment(UUID leaderId) {
    ResultSet lwtResult = session.execute(
        renewLeadPrepStmt.bind(
            LEAD_DURATION,
            reaperInstanceId,
            AppContext.REAPER_INSTANCE_ADDRESS,
            leaderId,
            reaperInstanceId));

    return lwtResult.wasApplied();
  }

  @Override
  public int countRunningReapers() {
    int runningReapers = getRunningReapers().size();
    LOG.debug("Running reapers = {}", runningReapers);
    return runningReapers > 0 ? runningReapers : 1;
  }

  @Override
  public List<UUID> getRunningReapers() {
    ResultSet result = session.execute(getRunningReapersCountPrepStmt.bind());
    return result.all().stream().map(row -> row.getUUID("reaper_instance_id")).collect(Collectors.toList());
  }

  @Override
  public void saveHeartbeat() {
    session.executeAsync(
        saveHeartbeatPrepStmt.bind(reaperInstanceId, AppContext.REAPER_INSTANCE_ADDRESS));
  }


  private static void overrideQueryOptions(CassandraFactory cassandraFactory, CassandraMode mode) {
    // all INSERT and DELETE stmt prepared in this class are idempotent
    ConsistencyLevel requiredCl = mode.equals(CassandraMode.ASTRA)
        ? ConsistencyLevel.LOCAL_QUORUM
        : ConsistencyLevel.LOCAL_ONE;
    if (cassandraFactory.getQueryOptions().isPresent()
        && ConsistencyLevel.LOCAL_ONE != cassandraFactory.getQueryOptions().get().getConsistencyLevel()) {
      LOG.warn("Customization of cassandra's queryOptions is not supported and will be overridden");
    }
    cassandraFactory.setQueryOptions(java.util.Optional.of(
        new QueryOptions()
          .setConsistencyLevel(requiredCl)
          .setDefaultIdempotence(true)));
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

  @Override
  public Collection<DiagEventSubscription> getEventSubscriptions() {
    return session.execute(getDiagnosticEventsPrepStmt.bind()).all().stream()
        .map((row) -> createDiagEventSubscription(row))
        .collect(Collectors.toList());
  }

  @Override
  public Collection<DiagEventSubscription> getEventSubscriptions(String clusterName) {
    Preconditions.checkNotNull(clusterName);

    return session.execute(getDiagnosticEventsPrepStmt.bind()).all().stream()
        .map((row) -> createDiagEventSubscription(row))
        .filter((subscription) -> clusterName.equals(subscription.getCluster()))
        .collect(Collectors.toList());
  }

  @Override
  public DiagEventSubscription getEventSubscription(UUID id) {
    Row row  = session.execute(getDiagnosticEventPrepStmt.bind(id)).one();
    if (null != row) {
      return createDiagEventSubscription(row);
    }
    throw new IllegalArgumentException("No event subscription with id " + id);
  }

  private static DiagEventSubscription createDiagEventSubscription(Row row) {
    return new DiagEventSubscription(
        Optional.of(row.getUUID("id")),
        row.getString("cluster"),
        Optional.of(row.getString("description")),
        row.getSet("nodes", String.class),
        row.getSet("events", String.class),
        row.getBool("export_sse"),
        row.getString("export_file_logger"),
        row.getString("export_http_endpoint"));
  }

  @Override
  public DiagEventSubscription addEventSubscription(DiagEventSubscription subscription) {
    Preconditions.checkArgument(subscription.getId().isPresent());

    session.execute(saveDiagnosticEventPrepStmt.bind(
        subscription.getId().get(),
        subscription.getCluster(),
        subscription.getDescription(),
        subscription.getNodes(),
        subscription.getEvents(),
        subscription.getExportSse(),
        subscription.getExportFileLogger(),
        subscription.getExportHttpEndpoint()));

    return subscription;
  }

  @Override
  public boolean deleteEventSubscription(UUID id) {
    session.execute(deleteDiagnosticEventPrepStmt.bind(id));
    return true;
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
      return null != stmt && !Objects.equals(Boolean.FALSE, stmt.isIdempotent())
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

      return null != stmt && !Objects.equals(Boolean.FALSE, stmt.isIdempotent())
          ? RetryDecision.retry(cl)
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

  @Override
  public List<GenericMetric> getMetrics(
      String clusterName,
      Optional<String> host,
      String metricDomain,
      String metricType,
      long since) {
    List<GenericMetric> metrics = Lists.newArrayList();
    List<ResultSetFuture> futures = Lists.newArrayList();
    List<String> timeBuckets = Lists.newArrayList();
    long now = DateTime.now().getMillis();
    long startTime = since;

    // Compute the hourly buckets since the requested lower bound timestamp
    while (startTime < now) {
      timeBuckets.add(DateTime.now().withMillis(startTime).toString(TIME_BUCKET_FORMATTER).substring(0, 11) + "0");
      startTime += 600000;
    }

    for (String timeBucket:timeBuckets) {
      if (host.isPresent()) {
        //metric = ? and cluster = ? and time_bucket = ? and host = ? and ts >= ? and ts <= ?
        futures.add(session.executeAsync(
            getMetricsForHostPrepStmt.bind(
                metricDomain,
                metricType,
                clusterName,
                timeBucket,
                host.get())));
      }
    }

    for (ResultSetFuture future : futures) {
      for (Row row : future.getUninterruptibly()) {
        metrics.add(
            GenericMetric.builder()
                .withClusterName(row.getString("cluster"))
                .withHost(row.getString("host"))
                .withMetricType(row.getString("metric_type"))
                .withMetricScope(row.getString("metric_scope"))
                .withMetricName(row.getString("metric_name"))
                .withMetricAttribute(row.getString("metric_attribute"))
                .withTs(new DateTime(row.getTimestamp("ts")))
                .withValue(row.getDouble("value"))
                .build());
      }
    }


    return metrics;
  }

  @Override
  public void storeMetrics(List<GenericMetric> metrics) {
    Map<String, List<GenericMetric>> metricsPerPartition = metrics.stream()
        .collect(Collectors.groupingBy(metric ->
          metric.getClusterName()
          + metric.getMetricDomain()
          + metric.getMetricType()
          + computeMetricsPartition(metric.getTs()).toString(TIME_BUCKET_FORMATTER)
          + metric.getHost()
          ));

    for (Entry<String, List<GenericMetric>> metricPartition:metricsPerPartition.entrySet()) {
      BatchStatement batch = new BatchStatement(BatchStatement.Type.UNLOGGED);
      for (GenericMetric metric:metricPartition.getValue()) {
        batch.add(
            storeMetricsPrepStmt.bind(
              metric.getClusterName(),
              metric.getMetricDomain(),
              metric.getMetricType(),
              computeMetricsPartition(metric.getTs()).toString(TIME_BUCKET_FORMATTER),
              metric.getHost(),
              metric.getMetricScope(),
              metric.getMetricName(),
              computeMetricsPartition(metric.getTs()),
              metric.getMetricAttribute(),
              metric.getValue()));
      }
      session.execute(batch);
    }
  }

  /**
   * Truncates a metric date time to the closest partition based on the definesd partition sizes
   * @param metricTime the time of the metric
   * @return the time truncated to the closest partition
   */
  private DateTime computeMetricsPartition(DateTime metricTime) {
    return metricTime
        .withMinuteOfHour(
            (metricTime.getMinuteOfHour() / METRICS_PARTITIONING_TIME_MINS)
                * METRICS_PARTITIONING_TIME_MINS)
        .withSecondOfMinute(0)
        .withMillisOfSecond(0);
  }

  @Override
  public void purgeMetrics() {}

  @Override
  public void storeOperations(String clusterName, OpType operationType, String host, String operationsJson) {
    session.executeAsync(
        insertOperationsPrepStmt.bind(
            clusterName,
            operationType.getName(),
            DateTime.now().toString(TIME_BUCKET_FORMATTER),
            host,
            DateTime.now().toDate(),
            operationsJson));
  }

  @Override
  public String listOperations(String clusterName, OpType operationType, String host) {
    List<ResultSetFuture> futures = Lists.newArrayList();
    futures.add(session.executeAsync(
            listOperationsForNodePrepStmt.bind(
                clusterName, operationType.getName(), DateTime.now().toString(TIME_BUCKET_FORMATTER), host)));
    futures.add(session.executeAsync(
        listOperationsForNodePrepStmt.bind(
            clusterName,
            operationType.getName(),
            DateTime.now().minusMinutes(1).toString(TIME_BUCKET_FORMATTER),
            host)));
    for (ResultSetFuture future:futures) {
      ResultSet operations = future.getUninterruptibly();
      for (Row row:operations) {
        return row.getString("data");
      }
    }

    return "";
  }

  @Override
  public void purgeNodeOperations() {}

  @Override
  public boolean lockRunningRepairsForNodes(
      UUID repairId,
      UUID segmentId,
      Set<String> replicas) {

    // Attempt to lock all the nodes involved in the segment
    BatchStatement batch = new BatchStatement();
    for (String replica:replicas) {
      batch.add(
          setRunningRepairsPrepStmt.bind(
              LEAD_DURATION,
              AppContext.REAPER_INSTANCE_ADDRESS,
              reaperInstanceId,
              segmentId,
              repairId,
              replica,
              null));
    }

    ResultSet results = session.execute(batch);
    if (!results.wasApplied()) {
      logFailedLead(results, repairId, segmentId);
    }

    return results.wasApplied();
  }

  @Override
  public boolean renewRunningRepairsForNodes(
      UUID repairId,
      UUID segmentId,
      Set<String> replicas) {
    // Attempt to renew lock on all the nodes involved in the segment
    BatchStatement batch = new BatchStatement();
    for (String replica:replicas) {
      batch.add(
          setRunningRepairsPrepStmt.bind(
              LEAD_DURATION,
              AppContext.REAPER_INSTANCE_ADDRESS,
              reaperInstanceId,
              segmentId,
              repairId,
              replica,
              reaperInstanceId));
    }

    ResultSet results = session.execute(batch);
    if (!results.wasApplied()) {
      logFailedLead(results, repairId, segmentId);
    }

    return results.wasApplied();
  }

  private void logFailedLead(ResultSet results, UUID repairId, UUID segmentId) {
    LOG.debug("Failed taking/renewing lock for repair {} and segment {} "
        + "because segments are already running for some nodes.",
        repairId, segmentId);
    for (Row row:results) {
      LOG.debug("node {} is locked by {}/{} for segment {}",
          row.getColumnDefinitions().contains("node")
            ? row.getString("node")
            : "unknown",
          row.getColumnDefinitions().contains("reaper_instance_host")
            ? row.getString("reaper_instance_host")
            : "unknown",
          row.getColumnDefinitions().contains("reaper_instance_id")
            ? row.getUUID("reaper_instance_id")
            : "unknown",
          row.getColumnDefinitions().contains("segment_id")
            ? row.getUUID("segment_id")
            : "unknown"
      );
    }
  }

  @Override
  public boolean releaseRunningRepairsForNodes(
      UUID repairId,
      UUID segmentId,
      Set<String> replicas) {
    // Attempt to release all the nodes involved in the segment
    BatchStatement batch = new BatchStatement();
    for (String replica:replicas) {
      batch.add(
          //reaperInstanceId, AppContext.REAPER_INSTANCE_ADDRESS
          setRunningRepairsPrepStmt.bind(
              LEAD_DURATION,
              null,
              null,
              null,
              repairId,
              replica,
              reaperInstanceId));
    }

    ResultSet results = session.execute(batch);
    if (!results.wasApplied()) {
      logFailedLead(results, repairId, segmentId);
    }

    return results.wasApplied();
  }

  @Override
  public Set<UUID> getLockedSegmentsForRun(UUID runId) {
    ResultSet results
        = session.execute(getRunningRepairsPrepStmt.bind(runId));

    Set<UUID> lockedSegments = results.all()
                                     .stream()
                                     .filter(row -> row.getUUID("reaper_instance_id") != null)
                                     .map(row -> row.getUUID("segment_id"))
                                     .collect(Collectors.toSet());
    return lockedSegments;
  }

  public Set<String> getLockedNodesForRun(UUID runId) {
    ResultSet results
        = session.execute(getRunningRepairsPrepStmt.bind(runId));

    Set<String> lockedNodes = results.all()
                                     .stream()
                                     .filter(row -> row.getUUID("reaper_instance_id") != null)
                                     .map(row -> row.getString("node"))
                                     .collect(Collectors.toSet());
    return lockedNodes;
  }

  public enum CassandraMode {
    CASSANDRA,
    ASTRA
  }

  @Override
  public List<PercentRepairedMetric> getPercentRepairedMetrics(String clusterName, UUID repairScheduleId, Long since) {
    List<PercentRepairedMetric> metrics = Lists.newArrayList();
    List<ResultSetFuture> futures = Lists.newArrayList();
    List<String> timeBuckets = Lists.newArrayList();
    long now = DateTime.now().getMillis();
    long startTime = since;

    // Compute the ten minutes buckets since the requested lower bound timestamp
    while (startTime <= now) {
      timeBuckets.add(DateTime.now().withMillis(startTime).toString(TIME_BUCKET_FORMATTER).substring(0, 11) + "0");
      startTime += 600000;
    }

    Collections.reverse(timeBuckets);

    for (String timeBucket:timeBuckets) {
      futures.add(session.executeAsync(
          getPercentRepairedForSchedulePrepStmt.bind(
              clusterName,
              repairScheduleId,
              timeBucket)));
    }

    long maxTimeBucket = 0;
    for (ResultSetFuture future : futures) {
      for (Row row : future.getUninterruptibly()) {
        if (Long.parseLong(row.getString("time_bucket")) >= maxTimeBucket) {
          // we only want metrics from the latest bucket
          metrics.add(
              PercentRepairedMetric.builder()
                  .withCluster(clusterName)
                  .withRepairScheduleId(row.getUUID("repair_schedule_id"))
                  .withKeyspaceName(row.getString("keyspace_name"))
                  .withTableName(row.getString("table_name"))
                  .withNode(row.getString("node"))
                  .withPercentRepaired(row.getInt("percent_repaired"))
                  .build());
          maxTimeBucket = Math.max(maxTimeBucket, Long.parseLong(row.getString("time_bucket")));
        }
      }
      if (!metrics.isEmpty()) {
        break;
      }
    }

    return metrics;
  }

  @Override
  public void storePercentRepairedMetric(PercentRepairedMetric metric) {
    session.execute(storePercentRepairedForSchedulePrepStmt.bind(
        metric.getCluster(),
        metric.getRepairScheduleId(),
        DateTime.now().toString(TIME_BUCKET_FORMATTER).substring(0, 11) + "0",
        metric.getNode(),
        metric.getKeyspaceName(),
        metric.getTableName(),
        metric.getPercentRepaired(),
        DateTime.now().toDate())
    );
  }

  @Override
  public void start() {
    // no-op
  }

  @Override
  public void stop() {
    // Statements executed when the server shuts down.
    LOG.info("Reaper is stopping, removing this instance from running reapers...");
    session.execute(deleteHeartbeatPrepStmt.bind(reaperInstanceId));
  }
}
