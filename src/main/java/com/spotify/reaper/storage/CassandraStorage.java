package com.spotify.reaper.storage;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.spotify.reaper.ReaperApplicationConfiguration;
import com.spotify.reaper.core.Cluster;
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

import org.apache.cassandra.repair.RepairParallelism;
import org.cognitor.cassandra.migration.Database;
import org.cognitor.cassandra.migration.MigrationRepository;
import org.cognitor.cassandra.migration.MigrationTask;
import org.joda.time.DateTime;

import io.dropwizard.setup.Environment;

public class CassandraStorage implements IStorage {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraStorage.class);
  com.datastax.driver.core.Cluster cassandra = null;
  Session session;

  /** simple cache of repair_id.
   * not accurate, only provides a floor value to shortcut looking for next appropriate id */
  private final ConcurrentMap<String,Long> repairIds = new ConcurrentHashMap<>();

  /* Simple statements */
  private final String getClustersStmt = "SELECT * FROM cluster";

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
  private PreparedStatement insertRepairSegmentByRunPrepStmt;
  private PreparedStatement getRepairSegmentByRunIdPrepStmt;
  private PreparedStatement insertRepairSchedulePrepStmt;
  private PreparedStatement getRepairSchedulePrepStmt;
  private PreparedStatement getRepairScheduleByClusterAndKsPrepStmt;
  private PreparedStatement insertRepairScheduleByClusterAndKsPrepStmt;
  private PreparedStatement deleteRepairSchedulePrepStmt;
  private PreparedStatement deleteRepairScheduleByClusterAndKsPrepStmt;
  private PreparedStatement deleteRepairSegmentPrepStmt;
  private PreparedStatement deleteRepairSegmentByRunId;
  private PreparedStatement insertRepairId;
  private PreparedStatement selectRepairId;
  private PreparedStatement updateRepairId;

  public CassandraStorage(ReaperApplicationConfiguration config, Environment environment) {
    cassandra = config.getCassandraFactory().build(environment);
    CodecRegistry codecRegistry = cassandra.getConfiguration().getCodecRegistry();
    codecRegistry.register(new DateTimeCodec());
    session = cassandra.connect(config.getCassandraFactory().getKeyspace());

    // initialize/upgrade db schema
    Database database = new Database(cassandra, config.getCassandraFactory().getKeyspace());
    MigrationTask migration = new MigrationTask(database, new MigrationRepository("db/cassandra"));
    migration.migrate();
        
    prepareStatements();
  }

  private void prepareStatements(){
    insertClusterPrepStmt = session.prepare("INSERT INTO cluster(name, partitioner, seed_hosts) values(?, ?, ?)");
    getClusterPrepStmt = session.prepare("SELECT * FROM cluster WHERE name = ?");
    deleteClusterPrepStmt = session.prepare("DELETE FROM cluster WHERE name = ?");
    insertRepairRunPrepStmt = session.prepare("INSERT INTO repair_run(id, cluster_name, repair_unit_id, cause, owner, state, creation_time, start_time, end_time, pause_time, intensity, last_event, segment_count, repair_parallelism) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
    insertRepairRunClusterIndexPrepStmt = session.prepare("INSERT INTO repair_run_by_cluster(cluster_name, id) values(?, ?)");
    insertRepairRunUnitIndexPrepStmt = session.prepare("INSERT INTO repair_run_by_unit(repair_unit_id, id) values(?, ?)");
    getRepairRunPrepStmt = session.prepare("SELECT * FROM repair_run WHERE id = ?");
    getRepairRunForClusterPrepStmt = session.prepare("SELECT * FROM repair_run_by_cluster WHERE cluster_name = ?");
    getRepairRunForUnitPrepStmt = session.prepare("SELECT * FROM repair_run_by_unit WHERE repair_unit_id = ?");
    deleteRepairRunPrepStmt = session.prepare("DELETE FROM repair_run WHERE id = ?");
    deleteRepairRunByClusterPrepStmt = session.prepare("DELETE FROM repair_run_by_cluster WHERE id = ? and cluster_name = ?");
    deleteRepairRunByUnitPrepStmt = session.prepare("DELETE FROM repair_run_by_unit WHERE id = ? and repair_unit_id= ?");
    deleteRepairSegmentPrepStmt = session.prepare("DELETE FROM repair_segment WHERE id = ?");
    deleteRepairSegmentByRunId = session.prepare("DELETE FROM repair_segment_by_run_id WHERE run_id = ?");
    insertRepairUnitPrepStmt = session.prepare("INSERT INTO repair_unit(id, cluster_name, keyspace_name, column_families, incremental_repair) VALUES(?, ?, ?, ?, ?)");
    getRepairUnitPrepStmt = session.prepare("SELECT * FROM repair_unit WHERE id = ?");
    insertRepairSegmentPrepStmt = session.prepare("INSERT INTO repair_segment(id, repair_unit_id, run_id, start_token, end_token, state, coordinator_host, start_time, end_time, fail_count) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
    getRepairSegmentPrepStmt = session.prepare("SELECT * FROM repair_segment WHERE id = ?");
    insertRepairSegmentByRunPrepStmt = session.prepare("INSERT INTO repair_segment_by_run_id(run_id, segment_id) VALUES(?, ?)");
    getRepairSegmentByRunIdPrepStmt = session.prepare("SELECT * FROM repair_segment_by_run_id WHERE run_id = ?");
    insertRepairSchedulePrepStmt = session.prepare("INSERT INTO repair_schedule(id, repair_unit_id, state, days_between, next_activation, run_history, segment_count, repair_parallelism, intensity, creation_time, owner, pause_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
    getRepairSchedulePrepStmt = session.prepare("SELECT * FROM repair_schedule WHERE id = ?");
    insertRepairScheduleByClusterAndKsPrepStmt = session.prepare("INSERT INTO repair_schedule_by_cluster_and_keyspace(cluster_name, keyspace_name, repair_schedule_id) VALUES(?, ?, ?)"); 
    getRepairScheduleByClusterAndKsPrepStmt = session.prepare("SELECT repair_schedule_id FROM repair_schedule_by_cluster_and_keyspace WHERE cluster_name = ? and keyspace_name = ?");
    deleteRepairSchedulePrepStmt = session.prepare("DELETE FROM repair_schedule WHERE id = ?");
    deleteRepairScheduleByClusterAndKsPrepStmt = session.prepare("DELETE FROM repair_schedule_by_cluster_and_keyspace WHERE cluster_name = ? and keyspace_name = ? and repair_schedule_id = ?");
    insertRepairId = session.prepare("INSERT INTO repair_id (id_type, id) VALUES(?, 0) IF NOT EXISTS");
    selectRepairId = session.prepare("SELECT id FROM repair_id WHERE id_type = ?");
    updateRepairId = session.prepare("UPDATE repair_id SET id=? WHERE id_type =? IF id = ?");
  }

  @Override
  public boolean isStorageConnected() {
    return session!=null && !session.isClosed();
  }

  @Override
  public Collection<Cluster> getClusters() {
    Collection<Cluster> clusters = Lists.<Cluster>newArrayList();
    ResultSet clusterResults = session.execute(getClustersStmt);
    for(Row cluster:clusterResults){
      clusters.add(new Cluster(cluster.getString("name"), cluster.getString("partitioner"), cluster.getSet("seed_hosts", String.class)));
    }

    return clusters;
  }

  @Override
  public boolean addCluster(Cluster cluster) {
    try {
      session.execute(insertClusterPrepStmt.bind(cluster.getName(), cluster.getPartitioner(), cluster.getSeedHosts()));
    } catch (Exception e) {
      LOG.warn("failed inserting cluster with name: {}", cluster.getName(), e);
      return false;
    }
    return true;
  }

  @Override
  public boolean updateCluster(Cluster newCluster) {
    return addCluster(newCluster);
  }

  @Override
  public Optional<Cluster> getCluster(String clusterName) {
    Cluster cluster = null;
    for(Row clusterRow : session.execute(getClusterPrepStmt.bind(clusterName))){
      cluster = new Cluster(clusterRow.getString("name"), clusterRow.getString("partitioner"), clusterRow.getSet("seed_hosts", String.class));
    }

    return Optional.fromNullable(cluster);
  }

  @Override
  public Optional<Cluster> deleteCluster(String clusterName) {
    session.execute(deleteClusterPrepStmt.bind(clusterName));
    return Optional.fromNullable(new Cluster(clusterName, null, null));
  }

  @Override
  public RepairRun addRepairRun(Builder repairRun) {
    RepairRun newRepairRun = repairRun.build(getNewRepairId("repair_run"));
    BatchStatement batch = new BatchStatement();
    batch.add(insertRepairRunPrepStmt.bind(newRepairRun.getId(), 
        newRepairRun.getClusterName(), 
        newRepairRun.getRepairUnitId(), 
        newRepairRun.getCause(), 
        newRepairRun.getOwner(), 
        newRepairRun.getRunState().toString(),
        newRepairRun.getCreationTime()==null?null:newRepairRun.getCreationTime(), 
            newRepairRun.getStartTime()==null?null:newRepairRun.getStartTime(), 
                newRepairRun.getEndTime()==null?null:newRepairRun.getEndTime(), 
                    newRepairRun.getPauseTime()==null?null:newRepairRun.getPauseTime(), 
                        newRepairRun.getIntensity(), 
                        newRepairRun.getLastEvent(), 
                        newRepairRun.getSegmentCount(), 
                        newRepairRun.getRepairParallelism().toString())
        );
    batch.add(insertRepairRunClusterIndexPrepStmt.bind(newRepairRun.getClusterName(), newRepairRun.getId()));
    batch.add(insertRepairRunUnitIndexPrepStmt.bind(newRepairRun.getRepairUnitId(), newRepairRun.getId()));
    session.execute(batch);
    return newRepairRun;
  }

  @Override
  public boolean updateRepairRun(RepairRun repairRun) {
    session.execute(insertRepairRunPrepStmt.bind(repairRun.getId(), repairRun.getClusterName(), repairRun.getRepairUnitId(), repairRun.getCause(), repairRun.getOwner(), repairRun.getRunState().toString(), repairRun.getCreationTime(), repairRun.getStartTime(), repairRun.getEndTime(), repairRun.getPauseTime(), repairRun.getIntensity(), repairRun.getLastEvent(), repairRun.getSegmentCount(), repairRun.getRepairParallelism().toString()));
    return true;
  }

  @Override
  public Optional<RepairRun> getRepairRun(long id) {
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
    Collection<Long> repairRunIds = getRepairRunIdsForCluster(clusterName);
    // Grab repair runs asynchronously for all the ids returned by the index table
    for(Long repairRunId:repairRunIds){
      repairRunFutures.add(session.executeAsync(getRepairRunPrepStmt.bind(repairRunId)));
    }

    return getRepairRunsAsync(repairRunFutures);
  }

  @Override
  public Collection<RepairRun> getRepairRunsForUnit(long repairUnitId) {
    Collection<RepairRun> repairRuns = Lists.<RepairRun>newArrayList();
    List<ResultSetFuture> repairRunFutures = Lists.<ResultSetFuture>newArrayList();

    // Grab all ids for the given cluster name
    ResultSet repairRunIds = session.execute(getRepairRunForUnitPrepStmt.bind(repairUnitId));

    // Grab repair runs asynchronously for all the ids returned by the index table
    for(Row repairRunId:repairRunIds){
      repairRunFutures.add(session.executeAsync(getRepairRunPrepStmt.bind(repairRunId.getLong("id"))));
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
        RepairRun repairRun = buildRepairRunFromRow(repairRunResult, repairRunResult.getLong("id"));
        repairRuns.add(repairRun);
      }
    }

    return repairRuns;
  }

  @Override
  public Collection<RepairRun> getRepairRunsWithState(RunState runState) {
    // There shouldn't be many repair runs, so we'll brute force this one
    // We'll switch to 2i if performance sucks IRL
    Collection<RepairRun> repairRuns = Lists.<RepairRun>newArrayList();
    ResultSet repairRunResults = session.execute("SELECT * FROM repair_run");
    for(Row repairRun:repairRunResults){
      if(RunState.valueOf(repairRun.getString("state")).equals(runState)){
        repairRuns.add(buildRepairRunFromRow(repairRun, repairRun.getLong("id")));
      }
    }

    return repairRuns;
  }

  @Override
  public Optional<RepairRun> deleteRepairRun(long id) {
    Optional<RepairRun> repairRun = getRepairRun(id);
    if(repairRun.isPresent()){
      BatchStatement batch = new BatchStatement();
      batch.add(deleteRepairRunPrepStmt.bind(id));
      batch.add(deleteRepairRunByClusterPrepStmt.bind(id, repairRun.get().getClusterName()));
      batch.add(deleteRepairRunByUnitPrepStmt.bind(id, repairRun.get().getRepairUnitId()));
      session.execute(batch);
    }

    // Delete all segments for the run we've deleted
    List<ResultSetFuture> futures= Lists.newArrayList(); 
    Collection<RepairSegment> segments = getRepairSegmentsForRun(id);
    int i=0;
    final int nbSegments = segments.size();
    futures.add(session.executeAsync(deleteRepairSegmentByRunId.bind(id)));
    for(RepairSegment segment:segments){
      futures.add(session.executeAsync(deleteRepairSegmentPrepStmt.bind(segment.getId())));
      i++;
      if(i%100==0 || i==nbSegments-1){
        futures.stream().forEach(f -> f.getUninterruptibly());
      }
    }

    return repairRun;
  }

  @Override
  public RepairUnit addRepairUnit(com.spotify.reaper.core.RepairUnit.Builder newRepairUnit) {
    RepairUnit repairUnit = newRepairUnit.build(getNewRepairId("repair_unit"));
    session.execute(insertRepairUnitPrepStmt.bind(repairUnit.getId(), repairUnit.getClusterName(), repairUnit.getKeyspaceName(), repairUnit.getColumnFamilies(), repairUnit.getIncrementalRepair()));
    return repairUnit;
  }

  @Override
  public Optional<RepairUnit> getRepairUnit(long id) {
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
    ResultSet results = session.execute("SELECT * FROM repair_unit");		
    for(Row repairUnitRow:results){
      if(repairUnitRow.getString("cluster_name").equals(cluster)
          && repairUnitRow.getString("keyspace_name").equals(keyspace)
          && repairUnitRow.getSet("column_families", String.class).equals(columnFamilyNames)){
        repairUnit = new RepairUnit.Builder(repairUnitRow.getString("cluster_name"), repairUnitRow.getString("keyspace_name"), repairUnitRow.getSet("column_families", String.class), repairUnitRow.getBool("incremental_repair")).build(repairUnitRow.getLong("id"));
        // exit the loop once we find a match
        break;
      }
    }

    return Optional.fromNullable(repairUnit);
  }

  @Override
  public void addRepairSegments(Collection<com.spotify.reaper.core.RepairSegment.Builder> newSegments, long runId) {
    List<ResultSetFuture> insertFutures = Lists.<ResultSetFuture>newArrayList();
    BatchStatement batch = new BatchStatement();
    for(com.spotify.reaper.core.RepairSegment.Builder builder:newSegments){
      RepairSegment segment = builder.build(getNewRepairId("repair_segment"));
      insertFutures.add(session.executeAsync(insertRepairSegmentPrepStmt.bind(segment.getId(), segment.getRepairUnitId(), segment.getRunId(), segment.getStartToken(), segment.getEndToken(), segment.getState().ordinal(), segment.getCoordinatorHost(), segment.getStartTime(), segment.getEndTime(), segment.getFailCount())));
      batch.add(insertRepairSegmentByRunPrepStmt.bind(segment.getRunId(), segment.getId()));
      if(insertFutures.size()%100==0){
        // cluster ddos protection
        session.execute(batch);
        batch.clear();
        for(ResultSetFuture insertFuture:insertFutures){
          insertFuture.getUninterruptibly();
        }
        insertFutures = Lists.newArrayList();
      }
    }

    // Wait for last queries to ack
    if(batch.size()>0) {
      session.execute(batch);
    }
    
    for(ResultSetFuture insertFuture:insertFutures){
      insertFuture.getUninterruptibly();
    }
  }

  @Override
  public boolean updateRepairSegment(RepairSegment newRepairSegment) {
    Date startTime = null;
    if (newRepairSegment.getStartTime() != null) {
       startTime = newRepairSegment.getStartTime().toDate();
    }
    session.executeAsync(insertRepairSegmentPrepStmt.bind(newRepairSegment.getId(), newRepairSegment.getRepairUnitId(), newRepairSegment.getRunId(), newRepairSegment.getStartToken(), newRepairSegment.getEndToken(), newRepairSegment.getState().ordinal(), newRepairSegment.getCoordinatorHost(), startTime, newRepairSegment.getEndTime().toDate(), newRepairSegment.getFailCount()));
    return true;
  }

  @Override
  public Optional<RepairSegment> getRepairSegment(long id) {
    RepairSegment segment = null;
    Row segmentRow = session.execute(getRepairSegmentPrepStmt.bind(id)).one();
    if(segmentRow != null){
      segment = createRepairSegmentFromRow(segmentRow);
    }

    return Optional.fromNullable(segment);
  }

  @Override
  public Collection<RepairSegment> getRepairSegmentsForRun(long runId) {
    List<ResultSetFuture> segmentsFuture = Lists.newArrayList();
    Collection<RepairSegment> segments = Lists.newArrayList();

    // First gather segments ids
    ResultSet segmentsIdResultSet = session.execute(getRepairSegmentByRunIdPrepStmt.bind(runId));
    int i=0;
    for(Row segmentIdResult:segmentsIdResultSet) {
      // Then get segments by id
      segmentsFuture.add(session.executeAsync(getRepairSegmentPrepStmt.bind(segmentIdResult.getLong("segment_id"))));
      i++;
      if(i%100==0 || segmentsIdResultSet.isFullyFetched()) {
        segments.addAll(fetchRepairSegmentFromFutures(segmentsFuture));
        segmentsFuture = Lists.newArrayList();
      }
    }

    return segments;
  }
  
  private Collection<RepairSegment> fetchRepairSegmentFromFutures(List<ResultSetFuture> segmentsFuture){
    Collection<RepairSegment> segments = Lists.newArrayList();
    
    for(ResultSetFuture segmentResult:segmentsFuture) {
      Row segmentRow = segmentResult.getUninterruptibly().one();
      if(segmentRow!=null){
        segments.add(createRepairSegmentFromRow(segmentRow));
      }
    }    
    
    return segments;
    
  }

  private RepairSegment createRepairSegmentFromRow(Row segmentRow){
    return createRepairSegmentFromRow(segmentRow, segmentRow.getLong("id"));
  }
  private RepairSegment createRepairSegmentFromRow(Row segmentRow, long segmentId){
    return new RepairSegment.Builder(segmentRow.getLong("run_id"), new RingRange(new BigInteger(segmentRow.getVarint("start_token") +""), new BigInteger(segmentRow.getVarint("end_token")+"")), segmentRow.getLong("repair_unit_id"))
        .coordinatorHost(segmentRow.getString("coordinator_host"))
        .endTime(new DateTime(segmentRow.getTimestamp("end_time")))
        .failCount(segmentRow.getInt("fail_count"))
        .startTime(new DateTime(segmentRow.getTimestamp("start_time")))
        .state(State.values()[segmentRow.getInt("state")])
        .build(segmentRow.getLong("id"));
  }


  public Optional<RepairSegment> getSegment(long runId, Optional<RingRange> range){
    RepairSegment segment = null;
    List<RepairSegment> segments = Lists.<RepairSegment>newArrayList();
    segments.addAll(getRepairSegmentsForRun(runId));

    // Sort segments by fail count and start token (in order to try those who haven't failed first, in start token order)
    Collections.sort( segments, new Comparator<RepairSegment>(){
      public int compare(RepairSegment seg1, RepairSegment seg2) {
        return ComparisonChain.start()
            .compare(seg1.getFailCount(), seg2.getFailCount())
            .compare(seg1.getStartToken(), seg2.getStartToken())
            .result();
      }
    });

    for(RepairSegment seg:segments){
      if(seg.getState().equals(State.NOT_STARTED) // State condition
          && ((range.isPresent() && 
              (range.get().getStart().compareTo(seg.getStartToken())>=0 || range.get().getEnd().compareTo(seg.getEndToken())<=0)
              ) || !range.isPresent()) // Token range condition
          ){
        segment = seg;
        break;
      }
    }
    return Optional.fromNullable(segment);
  }

  @Override
  public Optional<RepairSegment> getNextFreeSegment(long runId) {
    return getSegment(runId, Optional.<RingRange>absent());
  }

  @Override
  public Optional<RepairSegment> getNextFreeSegmentInRange(long runId, RingRange range) {
    return getSegment(runId, Optional.fromNullable(range));
  }

  @Override
  public Collection<RepairSegment> getSegmentsWithState(long runId, State segmentState) {
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
  public Collection<Long> getRepairRunIdsForCluster(String clusterName) {
    Collection<Long> repairRunIds = Lists.<Long>newArrayList();
    ResultSet results = session.execute(getRepairRunForClusterPrepStmt.bind(clusterName));
    for(Row result:results){
      repairRunIds.add(result.getLong("id"));
    }
    return repairRunIds;
  }

  @Override
  public int getSegmentAmountForRepairRun(long runId) {
    return getRepairSegmentsForRun(runId).size();

  }

  @Override
  public int getSegmentAmountForRepairRunWithState(long runId, State state) {
    return getSegmentsWithState(runId, state).size();
  }

  @Override
  public RepairSchedule addRepairSchedule(com.spotify.reaper.core.RepairSchedule.Builder repairSchedule) {
    RepairSchedule schedule = repairSchedule.build(getNewRepairId("repairSchedule"));
    updateRepairSchedule(schedule);

    return schedule;
  }



  @Override
  public Optional<RepairSchedule> getRepairSchedule(long repairScheduleId) {
    RepairSchedule schedule = null;
    Row sched = session.execute(getRepairSchedulePrepStmt.bind(repairScheduleId)).one();
    if(sched!=null){
      schedule = createRepairScheduleFromRow(sched);
    }
    return Optional.fromNullable(schedule);
  }

  private RepairSchedule createRepairScheduleFromRow(Row repairScheduleRow){
    return new RepairSchedule.Builder(repairScheduleRow.getLong("repair_unit_id"), 
        RepairSchedule.State.valueOf(repairScheduleRow.getString("state")), 
        repairScheduleRow.getInt("days_between"), 
        new DateTime(repairScheduleRow.getTimestamp("next_activation")), 
        ImmutableList.copyOf(repairScheduleRow.getSet("run_history", Long.class)), 
        repairScheduleRow.getInt("segment_count"), 
        RepairParallelism.fromName(repairScheduleRow.getString("repair_parallelism")), 
        repairScheduleRow.getDouble("intensity"), 
        new DateTime(repairScheduleRow.getTimestamp("creation_time")))
        .owner(repairScheduleRow.getString("owner"))
        .pauseTime(new DateTime(repairScheduleRow.getTimestamp("pause_time"))).build(repairScheduleRow.getLong("id"));


  }

  @Override
  public Collection<RepairSchedule> getRepairSchedulesForCluster(String clusterName) {
    Collection<RepairSchedule> schedules = Lists.<RepairSchedule>newArrayList();
    ResultSet scheduleIds = session.execute(getRepairScheduleByClusterAndKsPrepStmt.bind(clusterName, " "));
    for(Row scheduleId:scheduleIds){
      Optional<RepairSchedule> schedule = getRepairSchedule(scheduleId.getLong("repair_schedule_id"));
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
      Optional<RepairSchedule> schedule = getRepairSchedule(scheduleId.getLong("repair_schedule_id"));
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
      Optional<RepairSchedule> schedule = getRepairSchedule(scheduleId.getLong("repair_schedule_id"));
      if(schedule.isPresent()){
        schedules.add(schedule.get());
      }
    }

    return schedules;
  }

  @Override
  public Collection<RepairSchedule> getAllRepairSchedules() {
    Collection<RepairSchedule> schedules = Lists.<RepairSchedule>newArrayList();
    ResultSet scheduleResults = session.execute("SELECT * FROM repair_schedule");
    for(Row scheduleRow:scheduleResults){
      schedules.add(createRepairScheduleFromRow(scheduleRow));

    }

    return schedules;
  }

  @Override
  public boolean updateRepairSchedule(RepairSchedule newRepairSchedule) {
    BatchStatement batch = new BatchStatement();
    final Set<Long> repairHistory = Sets.newHashSet();
    repairHistory.addAll(newRepairSchedule.getRunHistory());

    batch.add(insertRepairSchedulePrepStmt.bind(newRepairSchedule.getId(), 
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
        newRepairSchedule.getPauseTime())
        );
    RepairUnit repairUnit = getRepairUnit(newRepairSchedule.getRepairUnitId()).get();
    batch.add(insertRepairScheduleByClusterAndKsPrepStmt.bind(repairUnit.getClusterName(), repairUnit.getKeyspaceName(), newRepairSchedule.getId()));
    batch.add(insertRepairScheduleByClusterAndKsPrepStmt.bind(repairUnit.getClusterName(), " ", newRepairSchedule.getId()));
    batch.add(insertRepairScheduleByClusterAndKsPrepStmt.bind(" ", repairUnit.getKeyspaceName(), newRepairSchedule.getId()));
    session.execute(batch);

    return true;
  }

  @Override
  public Optional<RepairSchedule> deleteRepairSchedule(long id) {
    Optional<RepairSchedule> repairSchedule = getRepairSchedule(id);
    if(repairSchedule.isPresent()){
      RepairUnit repairUnit = getRepairUnit(repairSchedule.get().getRepairUnitId()).get();
      BatchStatement batch = new BatchStatement();
      batch.add(deleteRepairSchedulePrepStmt.bind(repairSchedule.get().getId()));
      batch.add(deleteRepairScheduleByClusterAndKsPrepStmt.bind(repairUnit.getClusterName(), repairUnit.getKeyspaceName(), repairSchedule.get().getId()));
      batch.add(deleteRepairScheduleByClusterAndKsPrepStmt.bind(repairUnit.getClusterName(), " ", repairSchedule.get().getId()));
      batch.add(deleteRepairScheduleByClusterAndKsPrepStmt.bind(" ", repairUnit.getKeyspaceName(), repairSchedule.get().getId()));
      session.execute(batch);
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
    
    Collection<RepairScheduleStatus> repairScheduleStatuses = repairSchedules.stream()
                                                                             .map(sched -> new RepairScheduleStatus(sched, getRepairUnit(sched.getRepairUnitId()).get()))
                                                                             .collect(Collectors.toList());
   
    return repairScheduleStatuses;
  }

  public long getNewRepairId(String idType){
    if (!repairIds.containsKey(idType)){
        repairIds.putIfAbsent(idType, 0L);
        // Create id counter if it doesn't exist yet
        session.execute(insertRepairId.bind(idType));
    }
    long idValue = repairIds.get(idType);
    int attempts = 0;

    // Increment and perform CAS, if it fails then fetch current value of the counter and repeat
    while(true){
      idValue++;
      ResultSet casResult = session.execute(updateRepairId.bind(idValue, idType, (idValue-1)));
      if(casResult.wasApplied()){
          break;
      }else{
          idValue = session.execute(selectRepairId.bind(idType)).one().getLong("id");
          Preconditions.checkState(idValue < Long.MAX_VALUE);
          attempts++;
          if(10 <= attempts && 0 == attempts % 10){
              LOG.warn("still cant find a new repairId after " + attempts + " attempts");
          }
      }
    }
    repairIds.put(idType, Math.max(idValue, repairIds.get(idType)));
    return idValue;
  }

  private RepairRun buildRepairRunFromRow(Row repairRunResult, long id){
    return new RepairRun.Builder(repairRunResult.getString("cluster_name"), repairRunResult.getLong("repair_unit_id"), new DateTime(repairRunResult.getTimestamp("creation_time")), repairRunResult.getDouble("intensity"), repairRunResult.getInt("segment_count"), RepairParallelism.fromName(repairRunResult.getString("repair_parallelism")))
        .cause(repairRunResult.getString("cause"))
        .owner(repairRunResult.getString("owner"))
        .endTime(new DateTime(repairRunResult.getTimestamp("end_time")))
        .lastEvent(repairRunResult.getString("last_event"))
        .pauseTime(new DateTime(repairRunResult.getTimestamp("pause_time")))
        .runState(RunState.valueOf(repairRunResult.getString("state")))
        .startTime(new DateTime(repairRunResult.getTimestamp("start_time")))
        .build(id);
  }

}
