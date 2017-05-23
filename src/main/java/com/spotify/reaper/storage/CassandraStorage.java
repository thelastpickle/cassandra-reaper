package com.spotify.reaper.storage;

import java.math.BigInteger;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.QueryLogger;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.spotify.reaper.ReaperApplication;
import com.spotify.reaper.ReaperApplicationConfiguration;
import com.spotify.reaper.core.Cluster;
import com.spotify.reaper.core.HostMetrics;
import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.core.RepairRun.Builder;
import com.spotify.reaper.core.RepairRun.RunState;
import com.spotify.reaper.core.RepairSchedule;
import com.spotify.reaper.core.RepairSegment;
import com.spotify.reaper.core.RepairSegment.State;
import com.spotify.reaper.core.RepairUnit;
import com.spotify.reaper.resources.view.RepairRunStatus;
import com.spotify.reaper.resources.view.RepairScheduleStatus;
import com.spotify.reaper.service.RepairParameters;
import com.spotify.reaper.service.RingRange;
import com.spotify.reaper.storage.cassandra.DateTimeCodec;
import com.spotify.reaper.storage.cassandra.Migration003;
import io.dropwizard.setup.Environment;
import org.apache.cassandra.repair.RepairParallelism;
import org.cognitor.cassandra.migration.Database;
import org.cognitor.cassandra.migration.MigrationRepository;
import org.cognitor.cassandra.migration.MigrationTask;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import systems.composable.dropwizard.cassandra.CassandraFactory;

public final class CassandraStorage implements IStorage, IDistributedStorage {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraStorage.class);
  com.datastax.driver.core.Cluster cassandra = null;
  Session session;

  /* Simple statements */
  private static final String SELECT_CLUSTER = "SELECT * FROM cluster";
  private static final String SELECT_REPAIR_SCHEDULE = "SELECT * FROM repair_schedule_v1";
  private static final String SELECT_REPAIR_UNIT = "SELECT * FROM repair_unit_v1";

  /* prepared statements */
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
  private PreparedStatement getRepairSegmentPrepStmt;
  private PreparedStatement getRepairSegmentsByRunIdPrepStmt;
  private PreparedStatement insertRepairSchedulePrepStmt;
  private PreparedStatement getRepairSchedulePrepStmt;
  private PreparedStatement getRepairScheduleByClusterAndKsPrepStmt;
  private PreparedStatement insertRepairScheduleByClusterAndKsPrepStmt;
  private PreparedStatement deleteRepairSchedulePrepStmt;
  private PreparedStatement deleteRepairScheduleByClusterAndKsPrepStmt;
  private PreparedStatement getLeadOnSegmentPrepStmt;
  private PreparedStatement renewLeadOnSegmentPrepStmt;
  private PreparedStatement releaseLeadOnSegmentPrepStmt;
  private PreparedStatement storeHostMetricsPrepStmt;
  private PreparedStatement getHostMetricsPrepStmt;
  private PreparedStatement getRunningReapersCountPrepStmt;
  private PreparedStatement saveHeartbeatPrepStmt;

  private DateTime lastHeartBeat = DateTime.now();

  public CassandraStorage(ReaperApplicationConfiguration config, Environment environment) {
    CassandraFactory cassandraFactory = config.getCassandraFactory();
    // all INSERT and DELETE statement prepared in this class are idempotent
    cassandraFactory.setQueryOptions(java.util.Optional.of(new QueryOptions().setDefaultIdempotence(true)));
    cassandra = cassandraFactory.build(environment);
    if(config.getActivateQueryLogger())
      cassandra.register(QueryLogger.builder().build());
    
    CodecRegistry codecRegistry = cassandra.getConfiguration().getCodecRegistry();
    codecRegistry.register(new DateTimeCodec());
    session = cassandra.connect(config.getCassandraFactory().getKeyspace());

    // initialize/upgrade db schema
    Database database = new Database(cassandra, config.getCassandraFactory().getKeyspace());
    MigrationTask migration = new MigrationTask(database, new MigrationRepository("db/cassandra"));
    migration.migrate();
    Migration003.migrate(session);
    prepareStatements();
    lastHeartBeat = lastHeartBeat.minusMinutes(1);
  }

  private void prepareStatements(){
    insertClusterPrepStmt = session.prepare("INSERT INTO cluster(name, partitioner, seed_hosts) values(?, ?, ?)").setConsistencyLevel(ConsistencyLevel.QUORUM);
    getClusterPrepStmt = session.prepare("SELECT * FROM cluster WHERE name = ?").setConsistencyLevel(ConsistencyLevel.QUORUM);
    deleteClusterPrepStmt = session.prepare("DELETE FROM cluster WHERE name = ?");
    insertRepairRunPrepStmt = session.prepare("INSERT INTO repair_run(id, cluster_name, repair_unit_id, cause, owner, state, creation_time, start_time, end_time, pause_time, intensity, last_event, segment_count, repair_parallelism) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").setConsistencyLevel(ConsistencyLevel.QUORUM);
    insertRepairRunClusterIndexPrepStmt = session.prepare("INSERT INTO repair_run_by_cluster(cluster_name, id) values(?, ?)");
    insertRepairRunUnitIndexPrepStmt = session.prepare("INSERT INTO repair_run_by_unit(repair_unit_id, id) values(?, ?)");
    getRepairRunPrepStmt = session.prepare("SELECT id,cluster_name,repair_unit_id,cause,owner,state,creation_time,start_time,end_time,pause_time,intensity,last_event,segment_count,repair_parallelism FROM repair_run WHERE id = ? LIMIT 1").setConsistencyLevel(ConsistencyLevel.QUORUM);
    getRepairRunForClusterPrepStmt = session.prepare("SELECT * FROM repair_run_by_cluster WHERE cluster_name = ?");
    getRepairRunForUnitPrepStmt = session.prepare("SELECT * FROM repair_run_by_unit WHERE repair_unit_id = ?");
    deleteRepairRunPrepStmt = session.prepare("DELETE FROM repair_run WHERE id = ?");
    deleteRepairRunByClusterPrepStmt = session.prepare("DELETE FROM repair_run_by_cluster WHERE id = ? and cluster_name = ?");
    deleteRepairRunByUnitPrepStmt = session.prepare("DELETE FROM repair_run_by_unit WHERE id = ? and repair_unit_id= ?");
    insertRepairUnitPrepStmt = session.prepare("INSERT INTO repair_unit_v1(id, cluster_name, keyspace_name, column_families, incremental_repair) VALUES(?, ?, ?, ?, ?)");
    getRepairUnitPrepStmt = session.prepare("SELECT * FROM repair_unit_v1 WHERE id = ?");
    insertRepairSegmentPrepStmt = session.prepare("INSERT INTO repair_run(id, segment_id, repair_unit_id, start_token, end_token, segment_state, coordinator_host, segment_start_time, segment_end_time, fail_count) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").setConsistencyLevel(ConsistencyLevel.QUORUM);
    getRepairSegmentPrepStmt = session.prepare("SELECT id,repair_unit_id,segment_id,start_token,end_token,segment_state,coordinator_host,segment_start_time,segment_end_time,fail_count FROM repair_run WHERE id = ? and segment_id = ?").setConsistencyLevel(ConsistencyLevel.QUORUM);
    getRepairSegmentsByRunIdPrepStmt = session.prepare("SELECT id,repair_unit_id,segment_id,start_token,end_token,segment_state,coordinator_host,segment_start_time,segment_end_time,fail_count FROM repair_run WHERE id = ?");
    insertRepairSchedulePrepStmt = session.prepare("INSERT INTO repair_schedule_v1(id, repair_unit_id, state, days_between, next_activation, run_history, segment_count, repair_parallelism, intensity, creation_time, owner, pause_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").setConsistencyLevel(ConsistencyLevel.QUORUM);
    getRepairSchedulePrepStmt = session.prepare("SELECT * FROM repair_schedule_v1 WHERE id = ?").setConsistencyLevel(ConsistencyLevel.QUORUM);
    insertRepairScheduleByClusterAndKsPrepStmt = session.prepare("INSERT INTO repair_schedule_by_cluster_and_keyspace(cluster_name, keyspace_name, repair_schedule_id) VALUES(?, ?, ?)");
    getRepairScheduleByClusterAndKsPrepStmt = session.prepare("SELECT repair_schedule_id FROM repair_schedule_by_cluster_and_keyspace WHERE cluster_name = ? and keyspace_name = ?");
    deleteRepairSchedulePrepStmt = session.prepare("DELETE FROM repair_schedule_v1 WHERE id = ?");
    deleteRepairScheduleByClusterAndKsPrepStmt = session.prepare("DELETE FROM repair_schedule_by_cluster_and_keyspace WHERE cluster_name = ? and keyspace_name = ? and repair_schedule_id = ?");
    getLeadOnSegmentPrepStmt = session.prepare("INSERT INTO segment_leader(segment_id, reaper_instance_id, reaper_instance_host, last_heartbeat) VALUES(?, ?, ?, dateof(now())) IF NOT EXISTS");
    renewLeadOnSegmentPrepStmt = session.prepare("UPDATE segment_leader SET reaper_instance_id = ?, reaper_instance_host = ?, last_heartbeat = dateof(now()) WHERE segment_id = ? IF reaper_instance_id = ?");
    releaseLeadOnSegmentPrepStmt = session.prepare("DELETE FROM segment_leader WHERE segment_id = ? IF reaper_instance_id = ?");
    storeHostMetricsPrepStmt = session.prepare("INSERT INTO host_metrics (host_address, ts, pending_compactions, has_repair_running, active_anticompactions) VALUES(?, dateof(now()), ?, ?, ?)");
    getHostMetricsPrepStmt = session.prepare("SELECT * FROM host_metrics WHERE host_address = ?");
    getRunningReapersCountPrepStmt = session.prepare("SELECT count(*) as nb_reapers FROM running_reapers");
    saveHeartbeatPrepStmt = session.prepare("INSERT INTO running_reapers(reaper_instance_id, reaper_instance_host, last_heartbeat) VALUES(?,?,dateof(now()))");
  }

  @Override
  public boolean isStorageConnected() {
    return session!=null && !session.isClosed();
  }

  @Override
  public Collection<Cluster> getClusters() {
    Collection<Cluster> clusters = Lists.<Cluster>newArrayList();
    ResultSet clusterResults = session.execute(SELECT_CLUSTER);
    for(Row cluster:clusterResults){
      clusters.add(new Cluster(cluster.getString("name"), cluster.getString("partitioner"), cluster.getSet("seed_hosts", String.class)));
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
    Row r = session.execute(getClusterPrepStmt.bind(clusterName)).one();
    
    return r != null
            ? Optional.fromNullable(
                new Cluster(r.getString("name"), r.getString("partitioner"), r.getSet("seed_hosts", String.class)))
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

    repairRunBatch.add(insertRepairRunPrepStmt.bind(
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

    for(RepairSegment.Builder builder:newSegments){
      RepairSegment segment = builder.withRunId(newRepairRun.getId()).build(UUIDs.timeBased());

      repairRunBatch.add(insertRepairSegmentPrepStmt.bind(
            segment.getRunId(),
            segment.getId(),
            segment.getRepairUnitId(),
            segment.getStartToken(),
            segment.getEndToken(),
            segment.getState().ordinal(),
            segment.getCoordinatorHost(),
            segment.getStartTime(),
            segment.getEndTime(),
            segment.getFailCount()));

      if(100 == repairRunBatch.size()){
          futures.add(session.executeAsync(repairRunBatch));
          repairRunBatch = new BatchStatement(BatchStatement.Type.UNLOGGED);
      }
    }
    futures.add(session.executeAsync(repairRunBatch));
    futures.add(session.executeAsync(insertRepairRunClusterIndexPrepStmt.bind(newRepairRun.getClusterName(), newRepairRun.getId())));
    futures.add(session.executeAsync(insertRepairRunUnitIndexPrepStmt.bind(newRepairRun.getRepairUnitId(), newRepairRun.getId())));

    try {
        Futures.allAsList(futures).get();
    } catch (InterruptedException | ExecutionException ex) {
        LOG.error("failed to quorum insert new repair run " + newRepairRun.getId(), ex);
    }
    return newRepairRun;
  }

  @Override
  public boolean updateRepairRun(RepairRun repairRun) {
    session.execute(insertRepairRunPrepStmt.bind(
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
    if(repairRunResult != null){
      repairRun = buildRepairRunFromRow(repairRunResult, id);
    }

    return Optional.fromNullable(repairRun);
  }

  @Override
  public Collection<RepairRun> getRepairRunsForCluster(String clusterName) {
    List<ResultSetFuture> repairRunFutures = Lists.<ResultSetFuture>newArrayList();

    // Grab all ids for the given cluster name
    Collection<UUID> repairRunIds = getRepairRunIdsForCluster(clusterName);
    // Grab repair runs asynchronously for all the ids returned by the index table
    for(UUID repairRunId:repairRunIds){
      repairRunFutures.add(session.executeAsync(getRepairRunPrepStmt.bind(repairRunId)));
    }

    return getRepairRunsAsync(repairRunFutures);
  }

  @Override
  public Collection<RepairRun> getRepairRunsForUnit(UUID repairUnitId) {
    Collection<RepairRun> repairRuns = Lists.<RepairRun>newArrayList();
    List<ResultSetFuture> repairRunFutures = Lists.<ResultSetFuture>newArrayList();

    // Grab all ids for the given cluster name
    ResultSet repairRunIds = session.execute(getRepairRunForUnitPrepStmt.bind(repairUnitId));

    // Grab repair runs asynchronously for all the ids returned by the index table
    for(Row repairRunId:repairRunIds){
      repairRunFutures.add(session.executeAsync(getRepairRunPrepStmt.bind(repairRunId.getUUID("id"))));
    }

    repairRuns = getRepairRunsAsync(repairRunFutures);

    return repairRuns;
  }


  /**
   * Create a collection of RepairRun objects out of a list of ResultSetFuture.
   * Used to handle async queries on the repair_run table with a list of ids.
   *
   * @param repairRunFutures
   * @return
   */
  private Collection<RepairRun> getRepairRunsAsync(List<ResultSetFuture> repairRunFutures){
    Collection<RepairRun> repairRuns = Lists.<RepairRun>newArrayList();

    for(ResultSetFuture repairRunFuture:repairRunFutures){
      Row repairRunResult = repairRunFuture.getUninterruptibly().one();
      if(repairRunResult != null){
        RepairRun repairRun = buildRepairRunFromRow(repairRunResult, repairRunResult.getUUID("id"));
        repairRuns.add(repairRun);
      }
    }

    return repairRuns;
  }

  @Override
  public Collection<RepairRun> getRepairRunsWithState(RunState runState) {

    return getClusters().stream()
            // Grab all ids for the given cluster name
            .map(cluster -> getRepairRunIdsForCluster(cluster.getName()))
            // Grab repair runs asynchronously for all the ids returned by the index table
            .flatMap(repairRunIds
                    -> repairRunIds.stream()
                            .map(repairRunId -> session.executeAsync(getRepairRunPrepStmt.bind(repairRunId))))
            // wait for results
            .map((ResultSetFuture future) -> {
                Row repairRunResult = future.getUninterruptibly().one();
                return buildRepairRunFromRow(repairRunResult, repairRunResult.getUUID("id"));})
            // filter on runState
            .filter(repairRun -> repairRun.getRunState() == runState)
            .collect(Collectors.toSet());
  }

  @Override
  public Optional<RepairRun> deleteRepairRun(UUID id) {
    Optional<RepairRun> repairRun = getRepairRun(id);
    if(repairRun.isPresent()){
      session.executeAsync(deleteRepairRunByUnitPrepStmt.bind(id, repairRun.get().getRepairUnitId()));
      session.executeAsync(deleteRepairRunByClusterPrepStmt.bind(id, repairRun.get().getClusterName()));
      session.executeAsync(deleteRepairRunPrepStmt.bind(id));
    }
    return repairRun;
  }

  @Override
  public RepairUnit addRepairUnit(RepairUnit.Builder newRepairUnit) {
    RepairUnit repairUnit = newRepairUnit.build(UUIDs.timeBased());
    session.execute(insertRepairUnitPrepStmt.bind(repairUnit.getId(), repairUnit.getClusterName(), repairUnit.getKeyspaceName(), repairUnit.getColumnFamilies(), repairUnit.getIncrementalRepair()));
    return repairUnit;
  }

  @Override
  public Optional<RepairUnit> getRepairUnit(UUID id) {
    RepairUnit repairUnit = null;
    Row repairUnitRow = session.execute(getRepairUnitPrepStmt.bind(id)).one();
    if(repairUnitRow!=null){
      repairUnit = new RepairUnit.Builder(repairUnitRow.getString("cluster_name"), repairUnitRow.getString("keyspace_name"), repairUnitRow.getSet("column_families", String.class), repairUnitRow.getBool("incremental_repair")).build(id);
    }
    return Optional.fromNullable(repairUnit);
  }

  @Override
  public Optional<RepairUnit> getRepairUnit(String cluster, String keyspace, Set<String> columnFamilyNames) {
    // brute force again
    RepairUnit repairUnit=null;
    ResultSet results = session.execute(SELECT_REPAIR_UNIT);
    for(Row repairUnitRow:results){
      if(repairUnitRow.getString("cluster_name").equals(cluster)
          && repairUnitRow.getString("keyspace_name").equals(keyspace)
          && repairUnitRow.getSet("column_families", String.class).equals(columnFamilyNames)){
        repairUnit = new RepairUnit.Builder(repairUnitRow.getString("cluster_name"), repairUnitRow.getString("keyspace_name"), repairUnitRow.getSet("column_families", String.class), repairUnitRow.getBool("incremental_repair")).build(repairUnitRow.getUUID("id"));
        // exit the loop once we find a match
        break;
      }
    }

    return Optional.fromNullable(repairUnit);
  }

  @Override
  public boolean updateRepairSegment(RepairSegment newRepairSegment) {
    assert hasLeadOnSegment(newRepairSegment.getId())
            : "non-leader trying to update repair segment " + newRepairSegment.getId();

    Date startTime = null;
    if (newRepairSegment.getStartTime() != null) {
       startTime = newRepairSegment.getStartTime().toDate();
    }
    session.execute(insertRepairSegmentPrepStmt.bind(
            newRepairSegment.getRunId(),
            newRepairSegment.getId(),
            newRepairSegment.getRepairUnitId(),
            newRepairSegment.getStartToken(),
            newRepairSegment.getEndToken(),
            newRepairSegment.getState().ordinal(),
            newRepairSegment.getCoordinatorHost(),
            startTime,
            newRepairSegment.getEndTime().toDate(),
            newRepairSegment.getFailCount()));

    return true;
  }

  @Override
  public Optional<RepairSegment> getRepairSegment(UUID runId, UUID segmentId) {
    RepairSegment segment = null;
    Row segmentRow = session.execute(getRepairSegmentPrepStmt.bind(runId, segmentId)).one();
    if(segmentRow != null){
      segment = createRepairSegmentFromRow(segmentRow);
    }

    return Optional.fromNullable(segment);
  }

  @Override
  public Collection<RepairSegment> getRepairSegmentsForRun(UUID runId) {
    Collection<RepairSegment> segments = Lists.newArrayList();
    // First gather segments ids
    ResultSet segmentsIdResultSet = session.execute(getRepairSegmentsByRunIdPrepStmt.bind(runId));
    for(Row segmentRow : segmentsIdResultSet) {
        segments.add(createRepairSegmentFromRow(segmentRow));
    }

    return segments;
  }

  @Override
  public Collection<RepairSegment> getRepairSegmentsForRunInLocalMode(UUID runId, List<RingRange> localRanges) {
    LOG.debug("Getting ranges for local node {}", localRanges);
    Collection<RepairSegment> segments = Lists.newArrayList();

    // First gather segments ids
    ResultSet segmentsResultSet = session.execute(getRepairSegmentsByRunIdPrepStmt.bind(runId));
    segmentsResultSet.forEach(segmentRow -> {
      RepairSegment seg = createRepairSegmentFromRow(segmentRow);
      RingRange range = new RingRange(seg.getStartToken(), seg.getEndToken());
      localRanges.stream().forEach(localRange -> {
        if(localRange.encloses(range)) {
          segments.add(seg);
        }
      });
    });

    return segments;
  }

  private static boolean segmentIsWithinRange(RepairSegment segment, RingRange range) {
    return range.encloses(new RingRange(segment.getStartToken(), segment.getEndToken()));
  }

  private static RepairSegment createRepairSegmentFromRow(Row segmentRow){
    return new RepairSegment.Builder(
            new RingRange(
                    new BigInteger(segmentRow.getVarint("start_token") +""),
                    new BigInteger(segmentRow.getVarint("end_token")+"")),
            segmentRow.getUUID("repair_unit_id"))
        .withRunId(segmentRow.getUUID("id"))
        .coordinatorHost(segmentRow.getString("coordinator_host"))
        .endTime(new DateTime(segmentRow.getTimestamp("segment_end_time")))
        .failCount(segmentRow.getInt("fail_count"))
        .startTime(new DateTime(segmentRow.getTimestamp("segment_start_time")))
        .state(State.values()[segmentRow.getInt("segment_state")])
        .build(segmentRow.getUUID("segment_id"));
  }


  @Override
  public Optional<RepairSegment> getNextFreeSegmentInRange(UUID runId, Optional<RingRange> range) {
    List<RepairSegment> segments = Lists.<RepairSegment>newArrayList(getRepairSegmentsForRun(runId));
    Collections.shuffle(segments);

    for(RepairSegment seg:segments){
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

    for(RepairSegment seg:segments){
      if(seg.getState().equals(segmentState)){
        foundSegments.add(seg);
      }
    }

    return foundSegments;
  }

  @Override
  public Collection<RepairParameters> getOngoingRepairsInCluster(String clusterName) {
    Collection<RepairParameters> repairs = Lists.<RepairParameters>newArrayList();

    Collection<RepairRun> repairRuns = getRepairRunsForCluster(clusterName);

    for(RepairRun repairRun:repairRuns){
      Collection<RepairSegment> runningSegments = getSegmentsWithState(repairRun.getId(), State.RUNNING);
      for(RepairSegment segment:runningSegments){
        Optional<RepairUnit> repairUnit = getRepairUnit(repairRun.getRepairUnitId());
        repairs.add(new RepairParameters(new RingRange(segment.getStartToken(), segment.getEndToken()), repairUnit.get().getKeyspaceName(), repairUnit.get().getColumnFamilies(), repairRun.getRepairParallelism()));
      }
    }

    LOG.debug("found ongoing repairs {} {}", repairs.size(), repairs);

    return repairs;
  }

  @Override
  public Collection<UUID> getRepairRunIdsForCluster(String clusterName) {
    Collection<UUID> repairRunIds = Lists.<UUID>newArrayList();
    ResultSet results = session.execute(getRepairRunForClusterPrepStmt.bind(clusterName));
    for(Row result:results){
      repairRunIds.add(result.getUUID("id"));
    }
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
  public RepairSchedule addRepairSchedule(com.spotify.reaper.core.RepairSchedule.Builder repairSchedule) {
    RepairSchedule schedule = repairSchedule.build(UUIDs.timeBased());
    updateRepairSchedule(schedule);

    return schedule;
  }



  @Override
  public Optional<RepairSchedule> getRepairSchedule(UUID repairScheduleId) {
    Row sched = session.execute(getRepairSchedulePrepStmt.bind(repairScheduleId)).one();

    return sched != null
            ? Optional.fromNullable(createRepairScheduleFromRow(sched))
            : Optional.absent();
  }

  private RepairSchedule createRepairScheduleFromRow(Row repairScheduleRow){
    return new RepairSchedule.Builder(repairScheduleRow.getUUID("repair_unit_id"),
        RepairSchedule.State.valueOf(repairScheduleRow.getString("state")),
        repairScheduleRow.getInt("days_between"),
        new DateTime(repairScheduleRow.getTimestamp("next_activation")),
        ImmutableList.copyOf(repairScheduleRow.getSet("run_history", UUID.class)),
        repairScheduleRow.getInt("segment_count"),
        RepairParallelism.fromName(repairScheduleRow.getString("repair_parallelism")),
        repairScheduleRow.getDouble("intensity"),
        new DateTime(repairScheduleRow.getTimestamp("creation_time")))
        .owner(repairScheduleRow.getString("owner"))
        .pauseTime(new DateTime(repairScheduleRow.getTimestamp("pause_time"))).build(repairScheduleRow.getUUID("id"));


  }

  @Override
  public Collection<RepairSchedule> getRepairSchedulesForCluster(String clusterName) {
    Collection<RepairSchedule> schedules = Lists.<RepairSchedule>newArrayList();
    ResultSet scheduleIds = session.execute(getRepairScheduleByClusterAndKsPrepStmt.bind(clusterName, " "));
    for(Row scheduleId:scheduleIds){
      Optional<RepairSchedule> schedule = getRepairSchedule(scheduleId.getUUID("repair_schedule_id"));
      if(schedule.isPresent()){
        schedules.add(schedule.get());
      }
    }

    return schedules;
  }

  @Override
  public Collection<RepairSchedule> getRepairSchedulesForKeyspace(String keyspaceName) {
    Collection<RepairSchedule> schedules = Lists.<RepairSchedule>newArrayList();
    ResultSet scheduleIds = session.execute(getRepairScheduleByClusterAndKsPrepStmt.bind(" ", keyspaceName));
    for(Row scheduleId:scheduleIds){
      Optional<RepairSchedule> schedule = getRepairSchedule(scheduleId.getUUID("repair_schedule_id"));
      if(schedule.isPresent()){
        schedules.add(schedule.get());
      }
    }

    return schedules;
  }

  @Override
  public Collection<RepairSchedule> getRepairSchedulesForClusterAndKeyspace(String clusterName, String keyspaceName) {
    Collection<RepairSchedule> schedules = Lists.<RepairSchedule>newArrayList();
    ResultSet scheduleIds = session.execute(getRepairScheduleByClusterAndKsPrepStmt.bind(clusterName, keyspaceName));
    for(Row scheduleId:scheduleIds){
      Optional<RepairSchedule> schedule = getRepairSchedule(scheduleId.getUUID("repair_schedule_id"));
      if(schedule.isPresent()){
        schedules.add(schedule.get());
      }
    }

    return schedules;
  }

  @Override
  public Collection<RepairSchedule> getAllRepairSchedules() {
    Collection<RepairSchedule> schedules = Lists.<RepairSchedule>newArrayList();
    ResultSet scheduleResults = session.execute(SELECT_REPAIR_SCHEDULE);
    for(Row scheduleRow:scheduleResults){
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

    futures.add(session.executeAsync(insertRepairSchedulePrepStmt.bind(newRepairSchedule.getId(),
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
        newRepairSchedule.getPauseTime())));

    futures.add(session.executeAsync(insertRepairScheduleByClusterAndKsPrepStmt
            .bind(repairUnit.getClusterName(), repairUnit.getKeyspaceName(), newRepairSchedule.getId())));

    futures.add(session.executeAsync(insertRepairScheduleByClusterAndKsPrepStmt
            .bind(repairUnit.getClusterName(), " ", newRepairSchedule.getId())));

    futures.add(session.executeAsync(insertRepairScheduleByClusterAndKsPrepStmt
            .bind(" ", repairUnit.getKeyspaceName(), newRepairSchedule.getId())));

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
    if(repairSchedule.isPresent()){
      RepairUnit repairUnit = getRepairUnit(repairSchedule.get().getRepairUnitId()).get();

      session.executeAsync(deleteRepairScheduleByClusterAndKsPrepStmt
              .bind(repairUnit.getClusterName(), repairUnit.getKeyspaceName(), repairSchedule.get().getId()));

      session.executeAsync(deleteRepairScheduleByClusterAndKsPrepStmt
              .bind(repairUnit.getClusterName(), " ", repairSchedule.get().getId()));

      session.executeAsync(deleteRepairScheduleByClusterAndKsPrepStmt
              .bind(" ", repairUnit.getKeyspaceName(), repairSchedule.get().getId()));
      
      session.executeAsync(deleteRepairSchedulePrepStmt.bind(repairSchedule.get().getId()));
    }

    return repairSchedule;
  }

  @Override
  public Collection<RepairRunStatus> getClusterRunStatuses(String clusterName, int limit) {
    Collection<RepairRunStatus> repairRunStatuses = Lists.<RepairRunStatus>newArrayList();
    Collection<RepairRun> repairRuns = getRepairRunsForCluster(clusterName);
    for (RepairRun repairRun:repairRuns){
      Collection<RepairSegment> segments = getRepairSegmentsForRun(repairRun.getId());
      Optional<RepairUnit> repairUnit = getRepairUnit(repairRun.getRepairUnitId());

      int segmentsRepaired = (int) segments.stream()
                                      .filter(seg -> seg.getState().equals(RepairSegment.State.DONE))
                                      .count();

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



  private RepairRun buildRepairRunFromRow(Row repairRunResult, UUID id){
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
  public boolean takeLeadOnSegment(UUID segmentId) {
    LOG.debug("Trying to take lead on segment {}", segmentId);
    ResultSet lwtResult = session.execute(getLeadOnSegmentPrepStmt
            .bind(segmentId, ReaperApplication.REAPER_INSTANCE_ID, ReaperApplication.getInstanceAddress()));

    if (lwtResult.wasApplied()) {
      LOG.debug("Took lead on segment {}", segmentId);
      return true;
    }

    // Another instance took the lead on the segment
    LOG.debug("Could not take lead on segment {}", segmentId);
    return false;
  }

  @Override
  public boolean renewLeadOnSegment(UUID segmentId) {
    ResultSet lwtResult = session.execute(renewLeadOnSegmentPrepStmt.bind(ReaperApplication.REAPER_INSTANCE_ID, ReaperApplication.getInstanceAddress(), segmentId, ReaperApplication.REAPER_INSTANCE_ID));
    if (lwtResult.wasApplied()) {
      LOG.debug("Renewed lead on segment {}", segmentId);
      return true;
    }
    assert false : "Could not renew lead on segment " + segmentId;
    LOG.error("Failed to renew lead on segment {}", segmentId);
    return false;
  }

  @Override
  public void releaseLeadOnSegment(UUID segmentId) {
    ResultSet lwtResult = session.execute(releaseLeadOnSegmentPrepStmt.bind(segmentId, ReaperApplication.REAPER_INSTANCE_ID));
    if (lwtResult.wasApplied()) {
      LOG.debug("Released lead on segment {}", segmentId);
    } else {
      assert false : "Could not release lead on segment " + segmentId;
      LOG.error("Could not release lead on segment {}", segmentId);
    }
  }

  private boolean hasLeadOnSegment(UUID segmentId) {
    ResultSet lwtResult = session.execute(renewLeadOnSegmentPrepStmt.bind(
            ReaperApplication.REAPER_INSTANCE_ID,
            ReaperApplication.getInstanceAddress(),
            segmentId,
            ReaperApplication.REAPER_INSTANCE_ID));

    return lwtResult.wasApplied();
  }

  @Override
  public void storeHostMetrics(HostMetrics hostMetrics) {
    session.execute(storeHostMetricsPrepStmt.bind(hostMetrics.getHostAddress(), hostMetrics.getPendingCompactions(), hostMetrics.hasRepairRunning(), hostMetrics.getActiveAnticompactions()));
  }

  @Override
  public Optional<HostMetrics> getHostMetrics(String hostName) {
    ResultSet result = session.execute(getHostMetricsPrepStmt.bind(hostName));
    for(Row metrics:result) {
      return Optional.of(HostMetrics.builder().withHostAddress(hostName)
                                  .withPendingCompactions(metrics.getInt("pending_compactions"))
                                  .withHasRepairRunning(metrics.getBool("has_repair_running"))
                                  .withActiveAnticompactions(metrics.getInt("active_anticompactions"))
                                  .build());
     }

    return Optional.absent();
  }

  @Override
  public int countRunningReapers() {
    ResultSet result = session.execute(getRunningReapersCountPrepStmt.bind());
    int runningReapers = (int) result.one().getLong("nb_reapers");
    LOG.debug("Running reapers = {}", runningReapers);
    return runningReapers>0?runningReapers:1;
  }

  @Override
  public void saveHeartbeat() {
    DateTime now = DateTime.now();
    // Send heartbeats every minute
    if(now.minusSeconds(60).getMillis() >= lastHeartBeat.getMillis()) {
      session.executeAsync(saveHeartbeatPrepStmt.bind(ReaperApplication.REAPER_INSTANCE_ID, ReaperApplication.getInstanceAddress()));
      lastHeartBeat = now;
    }
  }

  private static boolean withinRange(RepairSegment segment, Optional<RingRange> range) {
    return !range.isPresent() || segmentIsWithinRange(segment, range.get());
  }
}
