/*
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
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.NodeMetrics;
import io.cassandrareaper.core.RepairRun;
import io.cassandrareaper.core.RepairRun.Builder;
import io.cassandrareaper.core.RepairRun.RunState;
import io.cassandrareaper.core.RepairSchedule;
import io.cassandrareaper.core.RepairSegment;
import io.cassandrareaper.core.RepairSegment.State;
import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.resources.view.RepairRunStatus;
import io.cassandrareaper.resources.view.RepairScheduleStatus;
import io.cassandrareaper.service.RepairParameters;
import io.cassandrareaper.service.RingRange;
import io.cassandrareaper.storage.cassandra.DateTimeCodec;
import io.cassandrareaper.storage.cassandra.Migration003;

import java.math.BigInteger;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
import com.datastax.driver.core.WriteType;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import io.dropwizard.setup.Environment;
import io.dropwizard.util.Duration;
import org.apache.cassandra.repair.RepairParallelism;
import org.cognitor.cassandra.migration.Database;
import org.cognitor.cassandra.migration.MigrationRepository;
import org.cognitor.cassandra.migration.MigrationTask;
import org.joda.time.DateTime;
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

  private static final Logger LOG = LoggerFactory.getLogger(CassandraStorage.class);

  private final com.datastax.driver.core.Cluster cassandra;
  private final Session session;


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
  private PreparedStatement insertRepairSegmentPrepStmt;
  private PreparedStatement insertRepairSegmentIncrementalPrepStmt;
  private PreparedStatement updateRepairSegmentPrepStmt;
  private PreparedStatement insertRepairSegmentEndTimePrepStmt;
  private PreparedStatement getRepairSegmentPrepStmt;
  private PreparedStatement getRepairSegmentsByRunIdPrepStmt;
  private PreparedStatement insertRepairSchedulePrepStmt;
  private PreparedStatement getRepairSchedulePrepStmt;
  private PreparedStatement getRepairScheduleByClusterAndKsPrepStmt;
  private PreparedStatement insertRepairScheduleByClusterAndKsPrepStmt;
  private PreparedStatement deleteRepairSchedulePrepStmt;
  private PreparedStatement deleteRepairScheduleByClusterAndKsPrepStmt;
  private PreparedStatement takeLeadPrepStmt;
  private PreparedStatement renewLeadPrepStmt;
  private PreparedStatement releaseLeadPrepStmt;
  private PreparedStatement getRunningReapersCountPrepStmt;
  private PreparedStatement saveHeartbeatPrepStmt;
  private PreparedStatement storeNodeMetricsPrepStmt;
  private PreparedStatement getNodeMetricsPrepStmt;
  private PreparedStatement getNodeMetricsByNodePrepStmt;

  public CassandraStorage(ReaperApplicationConfiguration config, Environment environment) {
    CassandraFactory cassandraFactory = config.getCassandraFactory();
    overrideQueryOptions(cassandraFactory);
    overrideRetryPolicy(cassandraFactory);
    overridePoolingOptions(cassandraFactory);
    cassandra = cassandraFactory.build(environment);
    if (config.getActivateQueryLogger()) {
      cassandra.register(QueryLogger.builder().build());
    }
    CodecRegistry codecRegistry = cassandra.getConfiguration().getCodecRegistry();
    codecRegistry.register(new DateTimeCodec());
    session = cassandra.connect(config.getCassandraFactory().getKeyspace());

    // initialize/upgrade db schema
    Database database = new Database(cassandra, config.getCassandraFactory().getKeyspace());
    MigrationTask migration = new MigrationTask(database, new MigrationRepository("db/cassandra"));
    migration.migrate();
    Migration003.migrate(session);
    prepareStatements();
  }

  private void prepareStatements() {
    insertClusterPrepStmt = session
        .prepare("INSERT INTO cluster(name, partitioner, seed_hosts) values(?, ?, ?)")
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
    insertRepairUnitPrepStmt =
        session.prepare(
            "INSERT INTO repair_unit_v1(id, cluster_name, keyspace_name, column_families, "
                + "incremental_repair, nodes, datacenters, blacklisted_tables) VALUES(?, ?, ?, ?, ?, ?, ?, ?)");
    getRepairUnitPrepStmt = session.prepare("SELECT * FROM repair_unit_v1 WHERE id = ?");
    insertRepairSegmentPrepStmt = session
        .prepare(
            "INSERT INTO repair_run"
                + "(id,segment_id,repair_unit_id,start_token,end_token,segment_state,fail_count)"
                + " VALUES(?, ?, ?, ?, ?, ?, ?)")
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
                + "segment_start_time,segment_end_time,fail_count FROM repair_run WHERE id = ? and segment_id = ?")
        .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
    getRepairSegmentsByRunIdPrepStmt = session.prepare(
        "SELECT id,repair_unit_id,segment_id,start_token,end_token,segment_state,coordinator_host,segment_start_time,"
            + "segment_end_time,fail_count FROM repair_run WHERE id = ?");
    insertRepairSchedulePrepStmt =
        session
            .prepare(
                "INSERT INTO repair_schedule_v1(id, repair_unit_id, state, days_between, next_activation, run_history, "
                    + "segment_count, repair_parallelism, intensity, "
                    + "creation_time, owner, pause_time, segment_count_per_node) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .setConsistencyLevel(ConsistencyLevel.QUORUM);
    getRepairSchedulePrepStmt
        = session.prepare("SELECT * FROM repair_schedule_v1 WHERE id = ?").setConsistencyLevel(ConsistencyLevel.QUORUM);
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
    takeLeadPrepStmt = session
        .prepare(
            "INSERT INTO leader(leader_id, reaper_instance_id, reaper_instance_host, last_heartbeat) "
                + "VALUES(?, ?, ?, dateof(now())) IF NOT EXISTS")
        .setIdempotent(false);
    renewLeadPrepStmt = session
        .prepare(
            "UPDATE leader SET reaper_instance_id = ?, reaper_instance_host = ?, last_heartbeat = dateof(now()) "
                + "WHERE leader_id = ? IF reaper_instance_id = ?")
        .setIdempotent(false);
    releaseLeadPrepStmt = session.prepare("DELETE FROM leader WHERE leader_id = ? IF reaper_instance_id = ?");
    getRunningReapersCountPrepStmt = session.prepare("SELECT count(*) as nb_reapers FROM running_reapers");
    saveHeartbeatPrepStmt = session
        .prepare(
            "INSERT INTO running_reapers(reaper_instance_id, reaper_instance_host, last_heartbeat)"
                + " VALUES(?,?,dateof(now()))")
        .setIdempotent(false);
    storeNodeMetricsPrepStmt = session
        .prepare(
            "INSERT INTO node_metrics_v1 (time_partition,run_id,node,datacenter,cluster,requested,pending_compactions,"
                + "has_repair_running,active_anticompactions) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .setIdempotent(false);
    getNodeMetricsPrepStmt = session.prepare("SELECT * FROM node_metrics_v1"
        + " WHERE time_partition = ? AND run_id = ?");
    getNodeMetricsByNodePrepStmt = session.prepare("SELECT * FROM node_metrics_v1"
        + " WHERE time_partition = ? AND run_id = ? AND node = ?");
  }

  @Override
  public boolean isStorageConnected() {
    return session != null && !session.isClosed();
  }

  @Override
  public Collection<Cluster> getClusters() {
    Collection<Cluster> clusters = Lists.<Cluster>newArrayList();
    Statement stmt = new SimpleStatement(SELECT_CLUSTER);
    stmt.setIdempotent(Boolean.TRUE);
    ResultSet clusterResults = session.execute(stmt);
    for (Row cluster : clusterResults) {
      clusters.add(
          new Cluster(
              cluster.getString("name"), cluster.getString("partitioner"), cluster.getSet("seed_hosts", String.class)));
    }

    return clusters;
  }

  @Override
  public boolean addCluster(Cluster cluster) {
    session.execute(insertClusterPrepStmt.bind(cluster.getName(), cluster.getPartitioner(), cluster.getSeedHosts()));
    return true;
  }

  @Override
  public boolean updateCluster(Cluster newCluster) {
    return addCluster(newCluster);
  }

  @Override
  public Optional<Cluster> getCluster(String clusterName) {
    Row row = session.execute(getClusterPrepStmt.bind(clusterName)).one();

    return row != null
        ? Optional.fromNullable(
            new Cluster(row.getString("name"), row.getString("partitioner"), row.getSet("seed_hosts", String.class)))
        : Optional.absent();
  }

  @Override
  public Optional<Cluster> deleteCluster(String clusterName) {
    session.executeAsync(deleteClusterPrepStmt.bind(clusterName));
    return Optional.fromNullable(new Cluster(clusterName, null, null));
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

    for (RepairSegment.Builder builder : newSegments) {
      RepairSegment segment = builder.withRunId(newRepairRun.getId()).build(UUIDs.timeBased());
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

        repairRunBatch.add(
            insertRepairSegmentPrepStmt.bind(
              segment.getRunId(),
              segment.getId(),
              segment.getRepairUnitId(),
              segment.getStartToken(),
              segment.getEndToken(),
              segment.getState().ordinal(),
              segment.getFailCount()));
      }

      if (100 <= repairRunBatch.size()) {
        futures.add(session.executeAsync(repairRunBatch));
        repairRunBatch = new BatchStatement(BatchStatement.Type.UNLOGGED);
      }
    }
    assert getRepairUnit(newRepairRun.getRepairUnitId()).get().getIncrementalRepair() == isIncremental.booleanValue();

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
    return Optional.fromNullable(repairRun);
  }

  @Override
  public Collection<RepairRun> getRepairRunsForCluster(String clusterName) {
    List<ResultSetFuture> repairRunFutures = Lists.<ResultSetFuture>newArrayList();

    // Grab all ids for the given cluster name
    Collection<UUID> repairRunIds = getRepairRunIdsForCluster(clusterName);
    // Grab repair runs asynchronously for all the ids returned by the index table
    for (UUID repairRunId : repairRunIds) {
      repairRunFutures.add(session.executeAsync(getRepairRunPrepStmt.bind(repairRunId)));
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
  public Collection<RepairRun> getRepairRunsWithState(RunState runState) {
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
            repairUnit.getBlacklistedTables()));
    return repairUnit;
  }

  @Override
  public Optional<RepairUnit> getRepairUnit(UUID id) {
    RepairUnit repairUnit = null;
    Row repairUnitRow = session.execute(getRepairUnitPrepStmt.bind(id)).one();
    if (repairUnitRow != null) {
      repairUnit =
          new RepairUnit.Builder(
                  repairUnitRow.getString("cluster_name"),
                  repairUnitRow.getString("keyspace_name"),
                  repairUnitRow.getSet("column_families", String.class),
                  repairUnitRow.getBool("incremental_repair"),
                  repairUnitRow.getSet("nodes", String.class),
                  repairUnitRow.getSet("datacenters", String.class),
                  repairUnitRow.getSet("blacklisted_tables", String.class))
              .build(id);
    }
    return Optional.fromNullable(repairUnit);
  }

  @Override
  public Optional<RepairUnit> getRepairUnit(
      String cluster,
      String keyspace,
      Set<String> columnFamilyNames,
      Set<String> nodes,
      Set<String> datacenters,
      Set<String> blacklistedTables) {
    // brute force again
    RepairUnit repairUnit = null;
    Statement stmt = new SimpleStatement(SELECT_REPAIR_UNIT);
    stmt.setIdempotent(Boolean.TRUE);
    ResultSet results = session.execute(stmt);
    for (Row repairUnitRow : results) {
      if (repairUnitRow.getString("cluster_name").equals(cluster)
          && repairUnitRow.getString("keyspace_name").equals(keyspace)
          && repairUnitRow.getSet("column_families", String.class).equals(columnFamilyNames)
          && repairUnitRow.getSet("nodes", String.class).equals(nodes)
          && repairUnitRow.getSet("datacenters", String.class).equals(datacenters)
          && repairUnitRow.getSet("blacklisted_tables", String.class).equals(blacklistedTables)) {
        repairUnit =
            new RepairUnit.Builder(
                    repairUnitRow.getString("cluster_name"),
                    repairUnitRow.getString("keyspace_name"),
                    repairUnitRow.getSet("column_families", String.class),
                    repairUnitRow.getBool("incremental_repair"),
                    repairUnitRow.getSet("nodes", String.class),
                    repairUnitRow.getSet("datacenters", String.class),
                    repairUnitRow.getSet("blacklisted_tables", String.class))
                .build(repairUnitRow.getUUID("id"));
        // exit the loop once we find a match
        break;
      }
    }

    return Optional.fromNullable(repairUnit);
  }

  @Override
  public boolean updateRepairSegment(RepairSegment segment) {

    assert hasLeadOnSegment(segment.getId())
        || (hasLeadOnSegment(segment.getRunId())
          && getRepairUnit(segment.getRepairUnitId()).get().getIncrementalRepair())
        : "non-leader trying to update repair segment " + segment.getId() + " of run " + segment.getRunId();

    BatchStatement updateRepairSegmentBatch = new BatchStatement(BatchStatement.Type.UNLOGGED);

    updateRepairSegmentBatch.add(
        updateRepairSegmentPrepStmt.bind(
            segment.getRunId(),
            segment.getId(),
            segment.getState().ordinal(),
            segment.getCoordinatorHost(),
            null != segment.getStartTime() ? segment.getStartTime().toDate() : null,
            segment.getFailCount()));

    if (null != segment.getEndTime()) {
      assert RepairSegment.State.DONE == segment.getState();

      updateRepairSegmentBatch.add(
          insertRepairSegmentEndTimePrepStmt.bind(
              segment.getRunId(),
              segment.getId(),
              segment.getEndTime().toDate()));
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

    return Optional.fromNullable(segment);
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

  @Override
  public Collection<RepairSegment> getRepairSegmentsForRunInLocalMode(UUID runId, List<RingRange> localRanges) {
    LOG.trace("Getting ranges for local node {}", localRanges);
    Collection<RepairSegment> segments = Lists.newArrayList();

    // First gather segments ids
    ResultSet segmentsResultSet = session.execute(getRepairSegmentsByRunIdPrepStmt.bind(runId));
    segmentsResultSet.forEach(
        segmentRow -> {
          RepairSegment seg = createRepairSegmentFromRow(segmentRow);
          RingRange range = new RingRange(seg.getStartToken(), seg.getEndToken());
          localRanges
              .stream()
              .forEach(
                  localRange -> {
                    if (localRange.encloses(range)) {
                      segments.add(seg);
                    }
                  });
        });

    return segments;
  }

  private static boolean segmentIsWithinRange(RepairSegment segment, RingRange range) {
    return range.encloses(new RingRange(segment.getStartToken(), segment.getEndToken()));
  }

  private static RepairSegment createRepairSegmentFromRow(Row segmentRow) {

    RepairSegment.Builder builder = RepairSegment.builder(
          new RingRange(
              new BigInteger(segmentRow.getVarint("start_token") + ""),
              new BigInteger(segmentRow.getVarint("end_token") + "")),
          segmentRow.getUUID("repair_unit_id"))
        .withRunId(segmentRow.getUUID("id"))
        .state(State.values()[segmentRow.getInt("segment_state")])
        .failCount(segmentRow.getInt("fail_count"));

    if (null != segmentRow.getString("coordinator_host")) {
      builder = builder.coordinatorHost(segmentRow.getString("coordinator_host"));
    }
    if (null != segmentRow.getTimestamp("segment_start_time")) {
      builder = builder.startTime(new DateTime(segmentRow.getTimestamp("segment_start_time")));
    }
    if (null != segmentRow.getTimestamp("segment_end_time")) {
      builder = builder.endTime(new DateTime(segmentRow.getTimestamp("segment_end_time")));
    }
    return builder.build(segmentRow.getUUID("segment_id"));
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
    return Optional.absent();
  }

  @Override
  public Collection<RepairSegment> getSegmentsWithState(UUID runId, State segmentState) {
    Collection<RepairSegment> foundSegments = Lists.<RepairSegment>newArrayList();
    List<RepairSegment> segments = Lists.<RepairSegment>newArrayList();

    segments.addAll(getRepairSegmentsForRun(runId));

    for (RepairSegment seg : segments) {
      if (seg.getState().equals(segmentState)) {
        foundSegments.add(seg);
      }
    }

    return foundSegments;
  }

  @Override
  public Collection<RepairParameters> getOngoingRepairsInCluster(String clusterName) {
    Collection<RepairParameters> repairs = Lists.<RepairParameters>newArrayList();

    Collection<RepairRun> repairRuns = getRepairRunsForCluster(clusterName);

    for (RepairRun repairRun : repairRuns) {
      Collection<RepairSegment> runningSegments = getSegmentsWithState(repairRun.getId(), State.RUNNING);
      for (RepairSegment segment : runningSegments) {
        Optional<RepairUnit> repairUnit = getRepairUnit(repairRun.getRepairUnitId());
        repairs.add(
            new RepairParameters(
                new RingRange(segment.getStartToken(), segment.getEndToken()),
                repairUnit.get().getKeyspaceName(),
                repairUnit.get().getColumnFamilies(),
                repairRun.getRepairParallelism()));
      }
    }

    LOG.trace("found ongoing repairs {} {}", repairs.size(), repairs);

    return repairs;
  }

  @Override
  public Collection<UUID> getRepairRunIdsForCluster(String clusterName) {
    Collection<UUID> repairRunIds = Lists.<UUID>newArrayList();
    ResultSet results = session.execute(getRepairRunForClusterPrepStmt.bind(clusterName));
    for (Row result : results) {
      repairRunIds.add(result.getUUID("id"));
    }

    LOG.trace("repairRunIds : {}", repairRunIds);
    return repairRunIds;
  }

  @Override
  public int getSegmentAmountForRepairRun(UUID runId) {
    return getRepairSegmentsForRun(runId).size();
  }

  @Override
  public int getSegmentAmountForRepairRunWithState(UUID runId, State state) {
    return getSegmentsWithState(runId, state).size();
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

    return sched != null ? Optional.fromNullable(createRepairScheduleFromRow(sched)) : Optional.absent();
  }

  private RepairSchedule createRepairScheduleFromRow(Row repairScheduleRow) {
    return new RepairSchedule.Builder(
            repairScheduleRow.getUUID("repair_unit_id"),
            RepairSchedule.State.valueOf(repairScheduleRow.getString("state")),
            repairScheduleRow.getInt("days_between"),
            new DateTime(repairScheduleRow.getTimestamp("next_activation")),
            ImmutableList.copyOf(repairScheduleRow.getSet("run_history", UUID.class)),
            repairScheduleRow.getInt("segment_count"),
            RepairParallelism.fromName(repairScheduleRow.getString("repair_parallelism")),
            repairScheduleRow.getDouble("intensity"),
            new DateTime(repairScheduleRow.getTimestamp("creation_time")),
            repairScheduleRow.getInt("segment_count_per_node"))
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
    RepairUnit repairUnit = getRepairUnit(newRepairSchedule.getRepairUnitId()).get();
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
      RepairUnit repairUnit = getRepairUnit(repairSchedule.get().getRepairUnitId()).get();

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
    Collection<RepairRun> repairRuns = getRepairRunsForCluster(clusterName);
    for (RepairRun repairRun : repairRuns) {
      Collection<RepairSegment> segments = getRepairSegmentsForRun(repairRun.getId());
      Optional<RepairUnit> repairUnit = getRepairUnit(repairRun.getRepairUnitId());

      int segmentsRepaired
          = (int) segments.stream().filter(seg -> seg.getState().equals(RepairSegment.State.DONE)).count();

      repairRunStatuses.add(new RepairRunStatus(repairRun, repairUnit.get(), segmentsRepaired));
    }

    return repairRunStatuses;
  }

  @Override
  public Collection<RepairScheduleStatus> getClusterScheduleStatuses(String clusterName) {
    Collection<RepairSchedule> repairSchedules = getRepairSchedulesForCluster(clusterName);

    Collection<RepairScheduleStatus> repairScheduleStatuses = repairSchedules
        .stream()
        .map(sched -> new RepairScheduleStatus(sched, getRepairUnit(sched.getRepairUnitId()).get()))
        .collect(Collectors.toList());

    return repairScheduleStatuses;
  }

  private RepairRun buildRepairRunFromRow(Row repairRunResult, UUID id) {
    LOG.trace("buildRepairRunFromRow {} / {}", id, repairRunResult);
    return new RepairRun.Builder(
        repairRunResult.getString("cluster_name"),
        repairRunResult.getUUID("repair_unit_id"),
        new DateTime(repairRunResult.getTimestamp("creation_time")),
        repairRunResult.getDouble("intensity"),
        repairRunResult.getInt("segment_count"),
        RepairParallelism.fromName(repairRunResult.getString("repair_parallelism")))
        .cause(repairRunResult.getString("cause"))
        .owner(repairRunResult.getString("owner"))
        .endTime(new DateTime(repairRunResult.getTimestamp("end_time")))
        .lastEvent(repairRunResult.getString("last_event"))
        .pauseTime(new DateTime(repairRunResult.getTimestamp("pause_time")))
        .runState(RunState.valueOf(repairRunResult.getString("state")))
        .startTime(new DateTime(repairRunResult.getTimestamp("start_time")))
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
    ResultSet lwtResult = session.execute(releaseLeadPrepStmt.bind(leaderId, AppContext.REAPER_INSTANCE_ID));

    if (lwtResult.wasApplied()) {
      LOG.debug("Released lead on segment {}", leaderId);
    } else {
      assert false : "Could not release lead on segment " + leaderId;
      LOG.error("Could not release lead on segment {}", leaderId);
    }
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
  public void storeNodeMetrics(UUID runId, NodeMetrics hostMetrics) {
    long minute = TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis());
    storeNodeMetricsImpl(runId, hostMetrics, minute);
    storeNodeMetricsImpl(runId, hostMetrics, minute + 1);
    storeNodeMetricsImpl(runId, hostMetrics, minute + 2);
  }

  private void storeNodeMetricsImpl(UUID runId, NodeMetrics hostMetrics, long minute) {
    session.executeAsync(
        storeNodeMetricsPrepStmt.bind(
            minute,
            runId,
            hostMetrics.getHostAddress(),
            hostMetrics.getDatacenter(),
            hostMetrics.getCluster(),
            hostMetrics.isRequested(),
            hostMetrics.getPendingCompactions(),
            hostMetrics.hasRepairRunning(),
            hostMetrics.getActiveAnticompactions()));
  }

  @Override
  public Collection<NodeMetrics> getNodeMetrics(UUID runId) {
    long minute = TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis());

    return session.execute(getNodeMetricsPrepStmt.bind(minute, runId)).all().stream()
        .map((row) -> createNodeMetrics(row))
        .collect(Collectors.toSet());
  }

  @Override
  public Optional<NodeMetrics> getNodeMetrics(UUID runId, String hostName) {
    long minute = TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis());
    Row row = session.execute(getNodeMetricsByNodePrepStmt.bind(minute, runId, hostName)).one();
    return null != row ? Optional.of(createNodeMetrics(row)) : Optional.absent();
  }

  private static NodeMetrics createNodeMetrics(Row row) {
    return NodeMetrics.builder()
        .withHostAddress(row.getString("node"))
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
    int runningReapers = (int) result.one().getLong("nb_reapers");
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
      return null != stmt && stmt.isIdempotent()
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

      return null != stmt && stmt.isIdempotent()
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

}
