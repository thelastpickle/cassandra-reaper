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

package io.cassandrareaper.storage.repairrun;

import io.cassandrareaper.core.RepairRun;
import io.cassandrareaper.core.RepairSegment;
import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.resources.view.RepairRunStatus;
import io.cassandrareaper.storage.cluster.CassandraClusterDao;
import io.cassandrareaper.storage.repairsegment.CassandraRepairSegmentDao;
import io.cassandrareaper.storage.repairunit.CassandraRepairUnitDao;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.cassandra.repair.RepairParallelism;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraRepairRunDao implements IRepairRunDao {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraRepairRunDao.class);
  private static final int MAX_RETURNED_REPAIR_RUNS = 1000;

  ObjectMapper objectMapper;
  PreparedStatement insertRepairRunPrepStmt;
  PreparedStatement insertRepairRunNoStatePrepStmt;
  PreparedStatement insertRepairRunClusterIndexPrepStmt;
  PreparedStatement insertRepairRunUnitIndexPrepStmt;
  PreparedStatement getRepairRunPrepStmt;
  PreparedStatement getRepairRunForClusterPrepStmt;
  PreparedStatement getRepairRunForClusterWhereStatusPrepStmt;
  PreparedStatement getRepairRunForUnitPrepStmt;

  PreparedStatement deleteRepairRunPrepStmt;
  PreparedStatement deleteRepairRunByClusterByIdPrepStmt;
  PreparedStatement deleteRepairRunByUnitPrepStmt;
  private final CassandraRepairUnitDao cassRepairUnitDao;
  private final CassandraClusterDao cassClusterDao;

  private final CassandraRepairSegmentDao cassRepairSegmentDao;
  private final CqlSession session;


  public CassandraRepairRunDao(CassandraRepairUnitDao cassRepairUnitDao,
                               CassandraClusterDao cassClusterDao,
                               CassandraRepairSegmentDao cassRepairSegmentDao,
                               CqlSession session,
                               ObjectMapper objectMapper) {
    this.session = session;
    this.cassRepairSegmentDao = cassRepairSegmentDao;
    this.cassRepairUnitDao = cassRepairUnitDao;
    this.cassClusterDao = cassClusterDao;
    this.objectMapper = objectMapper;
    prepareStatements();
  }

  private void prepareStatements() {
    deleteRepairRunPrepStmt = session.prepare("DELETE FROM repair_run WHERE id = ?");
    insertRepairRunPrepStmt = session
        .prepare(
            SimpleStatement.builder("INSERT INTO repair_run(id, cluster_name, repair_unit_id,"
                + " cause, owner, state, creation_time, "
                + "start_time, end_time, pause_time, intensity, last_event, segment_count, repair_parallelism, "
                + "tables, adaptive_schedule) "
                + "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
              .setConsistencyLevel(ConsistencyLevel.QUORUM).build());
    insertRepairRunNoStatePrepStmt = session
        .prepare(
          SimpleStatement.builder("INSERT INTO repair_run(id, cluster_name,"
                + "repair_unit_id, cause, owner, creation_time, "
                + "intensity, last_event, segment_count, repair_parallelism, tables, adaptive_schedule) "
                + "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
              .setConsistencyLevel(ConsistencyLevel.QUORUM).build());
    insertRepairRunClusterIndexPrepStmt = session.prepare(
        "INSERT INTO repair_run_by_cluster_v2(cluster_name, id, repair_run_state) values(?, ?, ?)");
    insertRepairRunUnitIndexPrepStmt = session.prepare(
        "INSERT INTO repair_run_by_unit(repair_unit_id, id) values(?, ?)");
    getRepairRunPrepStmt = session
        .prepare(
          SimpleStatement.builder("SELECT id,cluster_name,repair_unit_id,cause,owner,"
                + "state,creation_time,start_time,end_time,"
                + "pause_time,intensity,last_event,segment_count,repair_parallelism,tables,adaptive_schedule "
                + "FROM repair_run WHERE id = ? LIMIT 1")
              .setConsistencyLevel(ConsistencyLevel.QUORUM).build());
    getRepairRunForClusterPrepStmt = session.prepare(
        "SELECT * FROM repair_run_by_cluster_v2 WHERE cluster_name = ? limit ?");
    getRepairRunForClusterWhereStatusPrepStmt = session.prepare(
        "SELECT id FROM repair_run_by_cluster_v2 WHERE cluster_name = ? AND repair_run_state = ? limit ?");
    getRepairRunForUnitPrepStmt = session.prepare("SELECT * FROM repair_run_by_unit WHERE repair_unit_id = ?");


    deleteRepairRunByClusterByIdPrepStmt = session.prepare(
        "DELETE FROM repair_run_by_cluster_v2 WHERE id = ? and cluster_name = ?");
    deleteRepairRunByUnitPrepStmt = session.prepare("DELETE FROM repair_run_by_unit "
        + "WHERE id = ? and repair_unit_id= ?");
  }

  @Override
  public RepairRun addRepairRun(
      RepairRun.Builder repairRun,
      Collection<RepairSegment.Builder> newSegments) {
    RepairRun newRepairRun = repairRun.build(Uuids.timeBased());
    BatchStatementBuilder repairRunBatch = BatchStatement.builder(BatchType.UNLOGGED);
    Boolean isIncremental = null;

    Instant creationTime = newRepairRun.getCreationTime() != null
        ? Instant.ofEpochMilli(newRepairRun.getCreationTime().getMillis())
        : null;
    Instant startTime = newRepairRun.getStartTime() != null
        ? Instant.ofEpochMilli(newRepairRun.getStartTime().getMillis())
        : null;
    Instant endTime = newRepairRun.getEndTime() != null
        ? Instant.ofEpochMilli(newRepairRun.getEndTime().getMillis())
        : null;
    Instant pauseTime = newRepairRun.getPauseTime() != null
        ? Instant.ofEpochMilli(newRepairRun.getPauseTime().getMillis())
        : null;
    List<CompletionStage<AsyncResultSet>> futures = Lists.newArrayList();
    repairRunBatch.addStatement(
        insertRepairRunPrepStmt.bind(
            newRepairRun.getId(),
            newRepairRun.getClusterName(),
            newRepairRun.getRepairUnitId(),
            newRepairRun.getCause(),
            newRepairRun.getOwner(),
            newRepairRun.getRunState().toString(),
            creationTime,
            startTime,
            endTime,
            pauseTime,
            newRepairRun.getIntensity(),
            newRepairRun.getLastEvent(),
            newRepairRun.getSegmentCount(),
            newRepairRun.getRepairParallelism().toString(),
            newRepairRun.getTables(),
            newRepairRun.getAdaptiveSchedule()));

    int nbRanges = 0;
    for (RepairSegment.Builder builder : newSegments) {
      RepairSegment segment = builder.withRunId(newRepairRun.getId()).withId(Uuids.timeBased()).build();
      isIncremental = null == isIncremental ? null != segment.getCoordinatorHost() : isIncremental;

      assert RepairSegment.State.NOT_STARTED == segment.getState();
      assert null == segment.getStartTime();
      assert null == segment.getEndTime();
      assert 0 == segment.getFailCount();
      assert (null != segment.getCoordinatorHost()) == isIncremental;

      if (isIncremental) {
        repairRunBatch.addStatement(
            cassRepairSegmentDao.insertRepairSegmentIncrementalPrepStmt.bind(
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
          repairRunBatch.addStatement(
              cassRepairSegmentDao.insertRepairSegmentPrepStmt.bind(
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
        futures.add(this.session.executeAsync(repairRunBatch.build()));
        repairRunBatch = BatchStatement.builder(BatchType.UNLOGGED);
        nbRanges = 0;
      }
    }

    futures.add(this.session.executeAsync(repairRunBatch.build()));
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
      for (CompletionStage<AsyncResultSet> future : futures) {
        // Wait for all inserts to complete
        future.toCompletableFuture().join();
      }
    } catch (RuntimeException ex) {
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
    Instant creationTime = repairRun.getCreationTime() != null
        ? Instant.ofEpochMilli(repairRun.getCreationTime().getMillis())
        : null;
    Instant startTime = repairRun.getStartTime() != null
        ? Instant.ofEpochMilli(repairRun.getStartTime().getMillis())
        : null;
    Instant endTime = repairRun.getEndTime() != null
        ? Instant.ofEpochMilli(repairRun.getEndTime().getMillis())
        : null;
    Instant pauseTime = repairRun.getPauseTime() != null
        ? Instant.ofEpochMilli(repairRun.getPauseTime().getMillis())
        : null;
    if (updateRepairState.orElse(true)) {
      BatchStatementBuilder updateRepairRunBatch = BatchStatement.builder(BatchType.LOGGED);
      // Updates of the last event impact the repair state.
      // We want to limit overwrites in this case.
      updateRepairRunBatch.addStatement(
          insertRepairRunClusterIndexPrepStmt.bind(
              repairRun.getClusterName(), repairRun.getId(), repairRun.getRunState().toString()));
      updateRepairRunBatch.addStatement(
          insertRepairRunPrepStmt.bind(
              repairRun.getId(),
              repairRun.getClusterName(),
              repairRun.getRepairUnitId(),
              repairRun.getCause(),
              repairRun.getOwner(),
              repairRun.getRunState().toString(),
              creationTime,
              startTime,
              endTime,
              pauseTime,
              repairRun.getIntensity(),
              repairRun.getLastEvent(),
              repairRun.getSegmentCount(),
              repairRun.getRepairParallelism().toString(),
              repairRun.getTables(),
              repairRun.getAdaptiveSchedule()));
      this.session.execute(updateRepairRunBatch.build());
    } else {
      this.session.execute(
          insertRepairRunNoStatePrepStmt.bind(
              repairRun.getId(),
              repairRun.getClusterName(),
              repairRun.getRepairUnitId(),
              repairRun.getCause(),
              repairRun.getOwner(),
              creationTime,
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

    Instant startTime = repairRunResult.getInstant("start_time");
    Instant pauseTime = repairRunResult.getInstant("pause_time");
    Instant endTime = repairRunResult.getInstant("end_time");

    return RepairRun.builder(repairRunResult.getString("cluster_name"), repairRunResult.getUuid("repair_unit_id"))
        .creationTime(new DateTime(repairRunResult.getInstant("creation_time").toEpochMilli()))
        .intensity(repairRunResult.getDouble("intensity"))
        .segmentCount(repairRunResult.getInt("segment_count"))
        .repairParallelism(RepairParallelism.fromName(repairRunResult.getString("repair_parallelism")))
        .cause(repairRunResult.getString("cause"))
        .owner(repairRunResult.getString("owner"))
        .startTime(null != startTime ? new DateTime(startTime.toEpochMilli()) : null)
        .pauseTime(null != pauseTime ? new DateTime(pauseTime.toEpochMilli()) : null)
        .endTime(null != endTime ? new DateTime(endTime.toEpochMilli()) : null)
        .lastEvent(repairRunResult.getString("last_event"))
        .runState(RepairRun.RunState.valueOf(repairRunResult.getString("state")))
        .tables(repairRunResult.getSet("tables", String.class))
        .adaptiveSchedule(repairRunResult.isNull("adaptive_schedule")
            ? false
            : repairRunResult.getBool("adaptive_schedule"))
        .build(id);
  }

  @Override
  public Collection<RepairRun> getRepairRunsForCluster(String clusterName, Optional<Integer> limit) {
    List<CompletionStage<AsyncResultSet>> repairRunFutures = Lists.newArrayList();

    // Grab all ids for the given cluster name
    Collection<UUID> repairRunIds = getRepairRunIdsForCluster(clusterName, limit);
    // Grab repair runs asynchronously for all the ids returned by the index table
    for (UUID repairRunId : repairRunIds) {
      repairRunFutures.add(this.session.executeAsync(getRepairRunPrepStmt.bind(repairRunId)));
      if (repairRunFutures.size() == limit.orElse(1000)) {
        break;
      }
    }

    return getRepairRunsAsync(repairRunFutures);
  }

  @Override
  public List<RepairRun> getRepairRunsForClusterPrioritiseRunning(String clusterName, Optional<Integer> limit) {
    List<CompletionStage<AsyncResultSet>> repairUuidFuturesByState = Lists.newArrayList();
    // We've set up the RunState enum so that values are declared in order of "interestingness",
    // we iterate over the table via the secondary index according to that ordering.
    for (String state : Arrays.asList("RUNNING", "PAUSED", "NOT_STARTED")) {
      repairUuidFuturesByState.add(
          // repairUUIDFutures will be a List of resultSetFutures, each of which contains a ResultSet of
          // UUIDs for one status.
          this.session
            .executeAsync(getRepairRunForClusterWhereStatusPrepStmt
              .bind(clusterName, state, limit.orElse(MAX_RETURNED_REPAIR_RUNS)
              )
            )
      );
    }
    CompletionStage<AsyncResultSet> repairUuidFuturesNoState = this.session
        .executeAsync(getRepairRunForClusterPrepStmt
          .bind(clusterName, limit.orElse(MAX_RETURNED_REPAIR_RUNS)
          )
        );

    List<UUID> flattenedUuids = Lists.<UUID>newArrayList();
    // Flatten the UUIDs from each status down into a single array.
    for (CompletionStage<AsyncResultSet> idResSetFuture : repairUuidFuturesByState) {
      AsyncResultSet results = idResSetFuture.toCompletableFuture().join();
      while (true) {
        for (Row row : results.currentPage()) {
          flattenedUuids.add(row.getUuid("id"));
        }
        if (!results.hasMorePages()) {
          break;
        }
        results = results.fetchNextPage().toCompletableFuture().join();
      }
    }
    // Merge the two lists and trim.
    AsyncResultSet results = repairUuidFuturesNoState.toCompletableFuture().join();
    while (true) {
      for (Row row : results.currentPage()) {
        UUID uuid = row.getUuid("id");
        if (!flattenedUuids.contains(uuid) && flattenedUuids.size() < limit.orElse(MAX_RETURNED_REPAIR_RUNS)) {
          flattenedUuids.add(uuid);
        }
      }
      if (!results.hasMorePages()) {
        break;
      }
      results = results.fetchNextPage().toCompletableFuture().join();
    }

    // Run an async query on each UUID in the flattened list, against the main repair_run table with
    // all columns required as an input to `buildRepairRunFromRow`.
    List<CompletionStage<AsyncResultSet>> repairRunFutures = Lists.newArrayList();
    flattenedUuids.forEach(uuid ->
        repairRunFutures.add(
          this.session
            .executeAsync(getRepairRunPrepStmt.bind(uuid)
            )
        )
    );

    // Defuture the repair_run rows and build the strongly typed RepairRun objects from the contents.
    List<RepairRun> repairRuns = Lists.newArrayList();
    for (CompletionStage<AsyncResultSet> future : repairRunFutures) {
      AsyncResultSet repairRunResults = future.toCompletableFuture().join();
      while (true) {
        for (Row row : repairRunResults.currentPage()) {
          if (row != null) {
            repairRuns.add(buildRepairRunFromRow(row, row.getUuid("id")));
          }
        }
        if (!repairRunResults.hasMorePages()) {
          break;
        }
        repairRunResults = repairRunResults.fetchNextPage().toCompletableFuture().join();
      }
    }
    return Collections.unmodifiableList(repairRuns);
  }

  @Override
  public Collection<RepairRun> getRepairRunsForUnit(UUID repairUnitId) {
    List<CompletionStage<AsyncResultSet>> repairRunFutures = Lists.newArrayList();

    // Grab all ids for the given cluster name
    ResultSet repairRunIds = this.session.execute(getRepairRunForUnitPrepStmt.bind(repairUnitId));

    // Grab repair runs asynchronously for all the ids returned by the index table
    for (Row repairRunId : repairRunIds) {
      repairRunFutures.add(this.session.executeAsync(getRepairRunPrepStmt.bind(repairRunId.getUuid("id"))));
    }

    return getRepairRunsAsync(repairRunFutures);
  }

  /**
   * Create a collection of RepairRun objects out of a list of ResultSetFuture. Used to handle async queries on the
   * repair_run table with a list of ids.
   */
  private Collection<RepairRun> getRepairRunsAsync(List<CompletionStage<AsyncResultSet>> repairRunFutures) {
    Collection<RepairRun> repairRuns = Lists.<RepairRun>newArrayList();

    for (CompletionStage<AsyncResultSet> future : repairRunFutures) {
      AsyncResultSet results = future.toCompletableFuture().join();
      Row repairRunResult = results.one();
      if (repairRunResult != null) {
        RepairRun repairRun = buildRepairRunFromRow(repairRunResult, repairRunResult.getUuid("id"));
        repairRuns.add(repairRun);
      }
    }

    return repairRuns;
  }


  @Override
  public Collection<RepairRun> getRepairRunsWithState(RepairRun.RunState runState) {
    Set<RepairRun> repairRunsWithState = Sets.newHashSet();

    List<Collection<UUID>> repairRunIds = cassClusterDao.getClusters()
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
      RepairRun.RunState runState) {

    Collection<RepairRun> repairRuns = Sets.newHashSet();
    List<CompletionStage<AsyncResultSet>> futures = Lists.newArrayList();

    for (UUID repairRunId : clusterRepairRunsId) {
      futures.add(this.session.executeAsync(getRepairRunPrepStmt.bind(repairRunId)));
    }

    for (CompletionStage<AsyncResultSet> future : futures) {
      AsyncResultSet results = future.toCompletableFuture().join();
      while (true) {
        for (Row row : results.currentPage()) {
          repairRuns.add(buildRepairRunFromRow(row, row.getUuid("id")));
        }
        if (!results.hasMorePages()) {
          break;
        }
        results = results.fetchNextPage().toCompletableFuture().join();
      }
    }
    return repairRuns.stream().filter(repairRun -> repairRun.getRunState() == runState).collect(Collectors.toSet());
  }


  @Override
  public Optional<RepairRun> deleteRepairRun(UUID id) {
    Optional<RepairRun> repairRun = getRepairRun(id);
    if (repairRun.isPresent()) {
      this.session.execute(deleteRepairRunByUnitPrepStmt.bind(id, repairRun.get().getRepairUnitId()));
      this.session.execute(deleteRepairRunByClusterByIdPrepStmt.bind(id, repairRun.get().getClusterName()));
    }
    this.session.execute(deleteRepairRunPrepStmt.bind(id));
    return repairRun;
  }

  // Grab all ids for the given cluster name
  @Override
  public SortedSet<UUID> getRepairRunIdsForCluster(String clusterName, Optional<Integer> limit) {
    SortedSet<UUID> repairRunIds = Sets.newTreeSet((u0, u1) -> (int) (u0.timestamp() - u1.timestamp()));
    ResultSet results = this.session.execute(getRepairRunForClusterPrepStmt.bind(clusterName, limit.orElse(
        MAX_RETURNED_REPAIR_RUNS)));
    for (Row result : results) {
      repairRunIds.add(result.getUuid("id"));
    }

    LOG.trace("repairRunIds : {}", repairRunIds);
    return repairRunIds;
  }

  private SortedSet<UUID> getRepairRunIdsForClusterWithState(String clusterName, RepairRun.RunState runState) {
    SortedSet<UUID> repairRunIds = Sets.newTreeSet((u0, u1) -> (int) (u0.timestamp() - u1.timestamp()));
    ResultSet results = this.session.execute(
        getRepairRunForClusterPrepStmt.bind(clusterName, MAX_RETURNED_REPAIR_RUNS)
    );
    results.all()
        .stream()
        .filter(run -> run.getString("repair_run_state").equals(runState.toString()))
        .map(run -> run.getUuid("id"))
        .forEach(repairRunIds::add);


    LOG.trace("repairRunIds : {}", repairRunIds);
    return repairRunIds;
  }

  @Override
  public Collection<RepairRunStatus> getClusterRunStatuses(String clusterName, int limit) {
    Collection<RepairRunStatus> repairRunStatuses = Lists.<RepairRunStatus>newArrayList();
    Collection<RepairRun> repairRuns = getRepairRunsForCluster(clusterName, Optional.of(limit));
    for (RepairRun repairRun : repairRuns) {
      Collection<RepairSegment> segments = cassRepairSegmentDao.getRepairSegmentsForRun(repairRun.getId());
      RepairUnit repairUnit = cassRepairUnitDao.getRepairUnit(repairRun.getRepairUnitId());

      int segmentsRepaired
            = (int) segments.stream().filter(seg -> seg.getState().equals(RepairSegment.State.DONE)).count();

      repairRunStatuses.add(new RepairRunStatus(repairRun, repairUnit, segmentsRepaired));
    }

    return repairRunStatuses;
  }
}