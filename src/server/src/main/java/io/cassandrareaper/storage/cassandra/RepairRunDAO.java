package io.cassandrareaper.storage.cassandra;

import io.cassandrareaper.core.RepairRun;
import io.cassandrareaper.core.RepairSegment;

import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import org.apache.cassandra.repair.RepairParallelism;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RepairRunDAO {
  private final CassandraStorage cassandraStorage;
  private final Session session;
  private static final Logger LOG = LoggerFactory.getLogger(CassandraStorage.class);

  private static final int MAX_RETURNED_REPAIR_RUNS = 1000;
  PreparedStatement insertRepairRunPrepStmt;
  PreparedStatement insertRepairRunNoStatePrepStmt;
  PreparedStatement insertRepairRunClusterIndexPrepStmt;
  PreparedStatement insertRepairRunUnitIndexPrepStmt;
  PreparedStatement getRepairRunPrepStmt;
  PreparedStatement getRepairRunForClusterPrepStmt;
  PreparedStatement getRepairRunForClusterWhereStatusPrepStmt;
  PreparedStatement getRepairRunForUnitPrepStmt;
  PreparedStatement deleteRepairRunByClusterPrepStmt;
  PreparedStatement deleteRepairRunPrepStmt;
  PreparedStatement deleteRepairRunByClusterByIdPrepStmt;
  PreparedStatement deleteRepairRunByUnitPrepStmt;

  

  public RepairRunDAO(CassandraStorage cassandraStorage, Session session ) {

    this.cassandraStorage = cassandraStorage;
    this.session = session;
    
    

  }

  private void prepareStatements() {
    deleteRepairRunPrepStmt = session.prepare("DELETE FROM repair_run WHERE id = ?");
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
    insertRepairRunClusterIndexPrepStmt = session.prepare("INSERT INTO repair_run_by_cluster_v2(cluster_name, id, repair_run_state) values(?, ?, ?)");
    insertRepairRunUnitIndexPrepStmt = session.prepare("INSERT INTO repair_run_by_unit(repair_unit_id, id) values(?, ?)");
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

    deleteRepairRunByClusterPrepStmt = session.prepare("DELETE FROM repair_run_by_cluster_v2 WHERE cluster_name = ?");
    deleteRepairRunByClusterByIdPrepStmt = session.prepare("DELETE FROM repair_run_by_cluster_v2 WHERE id = ? and cluster_name = ?");
    deleteRepairRunByUnitPrepStmt = session.prepare("DELETE FROM repair_run_by_unit "
        + "WHERE id = ? and repair_unit_id= ?");
  }
  
  public RepairRun addRepairRun(RepairRun.Builder repairRun, Collection<RepairSegment.Builder> newSegments, ObjectMapper objectMapper) {
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
            cassandraStorage.repairSegmentDAO.insertRepairSegmentIncrementalPrepStmt.bind(
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
              cassandraStorage.repairSegmentDAO.insertRepairSegmentPrepStmt.bind(
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
        futures.add(this.session.executeAsync(repairRunBatch));
        repairRunBatch = new BatchStatement(BatchStatement.Type.UNLOGGED);
        nbRanges = 0;
      }
    }
    assert cassandraStorage.getRepairUnit(newRepairRun.getRepairUnitId()).getIncrementalRepair() == isIncremental;

    futures.add(this.session.executeAsync(repairRunBatch));
    futures.add(
        this.session.executeAsync(
            insertRepairRunClusterIndexPrepStmt.bind(
                newRepairRun.getClusterName(),
                newRepairRun.getId(),
                newRepairRun.getRunState().toString())));
    futures.add(
        this.session.executeAsync(
            insertRepairRunUnitIndexPrepStmt.bind(newRepairRun.getRepairUnitId(), newRepairRun.getId())));

    try {
      Futures.allAsList(futures).get();
    } catch (InterruptedException | ExecutionException ex) {
      LOG.error("failed to quorum insert new repair run " + newRepairRun.getId(), ex);
    }
    return newRepairRun;
  }

  
  public boolean updateRepairRun(RepairRun repairRun) {
    return updateRepairRun(repairRun, Optional.of(true));
  }

  
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
      this.session.execute(updateRepairRunBatch);
    } else {
      this.session.execute(
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

  
  public Optional<RepairRun> getRepairRun(UUID id) {
    RepairRun repairRun = null;
    Row repairRunResult = this.session.execute(getRepairRunPrepStmt.bind(id)).one();
    if (repairRunResult != null) {
      try {
        repairRun = buildRepairRunFromRow(repairRunResult, id);
      } catch (RuntimeException ignore) {
        // has been since deleted, but zombie segments has been re-inserted
      }
    }
    return Optional.ofNullable(repairRun);
  }
  public RepairRun buildRepairRunFromRow(Row repairRunResult, UUID id) {
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
        .runState(RepairRun.RunState.valueOf(repairRunResult.getString("state")))
        .tables(repairRunResult.getSet("tables", String.class))
        .adaptiveSchedule(repairRunResult.isNull("adaptive_schedule")
            ? false
            : repairRunResult.getBool("adaptive_schedule"))
        .build(id);
  }
  
  public Collection<RepairRun> getRepairRunsForCluster(String clusterName, Optional<Integer> limit) {
    List<ResultSetFuture> repairRunFutures = Lists.<ResultSetFuture>newArrayList();

    // Grab all ids for the given cluster name
    Collection<UUID> repairRunIds = cassandraStorage.getRepairRunIdsForCluster(clusterName, limit);
    // Grab repair runs asynchronously for all the ids returned by the index table
    for (UUID repairRunId : repairRunIds) {
      repairRunFutures.add(this.session.executeAsync(getRepairRunPrepStmt.bind(repairRunId)));
      if (repairRunFutures.size() == limit.orElse(1000)) {
        break;
      }
    }

    return getRepairRunsAsync(repairRunFutures);
  }

  
  public List<RepairRun> getRepairRunsForClusterPrioritiseRunning(String clusterName, Optional<Integer> limit) {
    List<ResultSetFuture> repairUuidFuturesByState = Lists.<ResultSetFuture>newArrayList();
    // We've set up the RunState enum so that values are declared in order of "interestingness",
    // we iterate over the table via the secondary index according to that ordering.
    for (String state : Arrays.asList("RUNNING", "PAUSED", "NOT_STARTED")) {
      repairUuidFuturesByState.add(
          // repairUUIDFutures will be a List of resultSetFutures, each of which contains a ResultSet of
          // UUIDs for one status.
          this.session
              .executeAsync(getRepairRunForClusterWhereStatusPrepStmt
                  .bind(clusterName, state.toString(), limit.orElse(MAX_RETURNED_REPAIR_RUNS)
                  )
              )
      );
    }
    ResultSetFuture repairUuidFuturesNoState = this.session
        .executeAsync(getRepairRunForClusterPrepStmt
            .bind(clusterName, limit.orElse(MAX_RETURNED_REPAIR_RUNS)
            )
        );

    List<UUID> flattenedUuids = Lists.<UUID>newArrayList();
    // Flatten the UUIDs from each status down into a single array.
    for (ResultSetFuture idResSetFuture : repairUuidFuturesByState) {
      idResSetFuture
          .getUninterruptibly()
          .forEach(
              row -> flattenedUuids.add(row.getUUID("id"))
          );
    }
    // Merge the two lists and trim.
    repairUuidFuturesNoState.getUninterruptibly().forEach(row -> {
          UUID uuid = row.getUUID("id");
          if (!flattenedUuids.contains(uuid)) {
            flattenedUuids.add(uuid);
          }
        }
    );
    flattenedUuids.subList(0, Math.min(flattenedUuids.size(), limit.orElse(MAX_RETURNED_REPAIR_RUNS)));

    // Run an async query on each UUID in the flattened list, against the main repair_run table with
    // all columns required as an input to `buildRepairRunFromRow`.
    List<ResultSetFuture> repairRunFutures = Lists.<ResultSetFuture>newArrayList();
    flattenedUuids.forEach(uuid ->
        repairRunFutures.add(
            this.session
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

  
  public Collection<RepairRun> getRepairRunsForUnit(UUID repairUnitId) {
    List<ResultSetFuture> repairRunFutures = Lists.<ResultSetFuture>newArrayList();

    // Grab all ids for the given cluster name
    ResultSet repairRunIds = this.session.execute(getRepairRunForUnitPrepStmt.bind(repairUnitId));

    // Grab repair runs asynchronously for all the ids returned by the index table
    for (Row repairRunId : repairRunIds) {
      repairRunFutures.add(this.session.executeAsync(getRepairRunPrepStmt.bind(repairRunId.getUUID("id"))));
    }

    return getRepairRunsAsync(repairRunFutures);
  }

  /**
   * Create a collection of RepairRun objects out of a list of ResultSetFuture. Used to handle async queries on the
   * repair_run table with a list of ids.
   */
  Collection<RepairRun> getRepairRunsAsync(List<ResultSetFuture> repairRunFutures) {
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

  
  public Collection<RepairRun> getRepairRunsWithState(RepairRun.RunState runState) {
    Set<RepairRun> repairRunsWithState = Sets.newHashSet();

    List<Collection<UUID>> repairRunIds = cassandraStorage.getClusters()
        .stream()
        // Grab all ids for the given cluster name
        .map(cluster -> getRepairRunIdsForClusterWithState(cluster.getName(), runState))
        .collect(Collectors.toList());

    for (Collection<UUID> clusterRepairRunIds : repairRunIds) {
      repairRunsWithState.addAll(getRepairRunsWithStateForCluster(clusterRepairRunIds, runState));
    }

    return repairRunsWithState;
  }

  Collection<? extends RepairRun> getRepairRunsWithStateForCluster(
      Collection<UUID> clusterRepairRunsId,
      RepairRun.RunState runState) {

    Collection<RepairRun> repairRuns = Sets.newHashSet();
    List<ResultSetFuture> futures = Lists.newArrayList();

    for (UUID repairRunId : clusterRepairRunsId) {
      futures.add(this.session.executeAsync(getRepairRunPrepStmt.bind(repairRunId)));
    }

    for (ResultSetFuture future : futures) {
      ResultSet repairRunResult = future.getUninterruptibly();
      for (Row row : repairRunResult) {
        repairRuns.add(buildRepairRunFromRow(row, row.getUUID("id")));
      }
    }

    return repairRuns.stream().filter(repairRun -> repairRun.getRunState() == runState).collect(Collectors.toSet());
  }

  
  public Optional<RepairRun> deleteRepairRun(UUID id) {
    Optional<RepairRun> repairRun = cassandraStorage.getRepairRun(id);
    if (repairRun.isPresent()) {
      this.session.execute(deleteRepairRunByUnitPrepStmt.bind(id, repairRun.get().getRepairUnitId()));
      this.session.execute(deleteRepairRunByClusterByIdPrepStmt.bind(id, repairRun.get().getClusterName()));
    }
    this.session.execute(deleteRepairRunPrepStmt.bind(id));
    return repairRun;
  }

  
  public SortedSet<UUID> getRepairRunIdsForCluster(String clusterName, Optional<Integer> limit) {
    SortedSet<UUID> repairRunIds = Sets.newTreeSet((u0, u1) -> (int) (u0.timestamp() - u1.timestamp()));
    ResultSet results = this.session.execute(getRepairRunForClusterPrepStmt.bind(clusterName, limit.orElse(
        MAX_RETURNED_REPAIR_RUNS)));
    for (Row result : results) {
      repairRunIds.add(result.getUUID("id"));
    }

    LOG.trace("repairRunIds : {}", repairRunIds);
    return repairRunIds;
  }

  SortedSet<UUID> getRepairRunIdsForClusterWithState(String clusterName, RepairRun.RunState runState) {
    SortedSet<UUID> repairRunIds = Sets.newTreeSet((u0, u1) -> (int) (u0.timestamp() - u1.timestamp()));
    ResultSet results = this.session.execute(getRepairRunForClusterPrepStmt.bind(clusterName, MAX_RETURNED_REPAIR_RUNS));
    results.all()
        .stream()
        .filter(run -> run.getString("repair_run_state").equals(runState.toString()))
        .map(run -> run.getUUID("id"))
        .forEach(runId -> repairRunIds.add(runId));


    LOG.trace("repairRunIds : {}", repairRunIds);
    return repairRunIds;
  }

}