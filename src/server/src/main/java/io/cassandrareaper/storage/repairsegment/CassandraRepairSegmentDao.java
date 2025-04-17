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

package io.cassandrareaper.storage.repairsegment;

import io.cassandrareaper.core.RepairSegment;
import io.cassandrareaper.core.Segment;
import io.cassandrareaper.service.RingRange;
import io.cassandrareaper.storage.JsonParseUtils;
import io.cassandrareaper.storage.cassandra.CassandraConcurrencyDao;
import io.cassandrareaper.storage.repairunit.CassandraRepairUnitDao;

import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.joda.time.DateTime;

public class CassandraRepairSegmentDao implements IRepairSegmentDao {
  public PreparedStatement insertRepairSegmentPrepStmt;
  public PreparedStatement insertRepairSegmentIncrementalPrepStmt;
  PreparedStatement updateRepairSegmentPrepStmt;
  PreparedStatement insertRepairSegmentEndTimePrepStmt;
  PreparedStatement getRepairSegmentPrepStmt;
  PreparedStatement getRepairSegmentsByRunIdPrepStmt;
  PreparedStatement getRepairSegmentCountByRunIdPrepStmt;
  @Nullable // null on Cassandra-2 as it's not supported syntax
  PreparedStatement getRepairSegmentsByRunIdAndStatePrepStmt = null;
  @Nullable // null on Cassandra-2 as it's not supported syntax
  PreparedStatement getRepairSegmentCountByRunIdAndStatePrepStmt = null;
  private final CassandraConcurrencyDao cassandraConcurrencyDao;
  private final CassandraRepairUnitDao cassRepairUnitDao;
  private final CqlSession session;

  //TODO: Consider removing Cassandra 2 support so we don't need to look at the version.
  public CassandraRepairSegmentDao(CassandraConcurrencyDao cassandraConcurrencyDao,
                                   CassandraRepairUnitDao cassRepairUnitDao,
                                   CqlSession session) {
    this.session = session;
    this.cassandraConcurrencyDao = cassandraConcurrencyDao;
    this.cassRepairUnitDao = cassRepairUnitDao;
    prepareStatements();
  }

  public static boolean segmentIsWithinRange(RepairSegment segment, RingRange range) {
    return range.encloses(new RingRange(segment.getStartToken(), segment.getEndToken()));
  }

  static RepairSegment createRepairSegmentFromRow(Row segmentRow) {

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
            Objects.requireNonNull(segmentRow.getBigInteger("start_token")),
            Objects.requireNonNull(segmentRow.getBigInteger("end_token"))
          )
      );
    }

    RepairSegment.Builder builder
        = RepairSegment.builder(segmentBuilder.build(), segmentRow.getUuid("repair_unit_id"))
        .withRunId(segmentRow.getUuid("id"))
        .withState(RepairSegment.State.values()[segmentRow.getInt("segment_state")])
        .withFailCount(segmentRow.getInt("fail_count"));

    if (null != segmentRow.getString("coordinator_host")) {
      builder = builder.withCoordinatorHost(segmentRow.getString("coordinator_host"));
    }
    if (null != segmentRow.getInstant("segment_start_time")) {
      builder = builder.withStartTime(new DateTime(segmentRow.getInstant("segment_start_time").toEpochMilli()));
    }
    if (null != segmentRow.getInstant("segment_end_time")) {
      builder = builder.withEndTime(new DateTime(segmentRow.getInstant("segment_end_time").toEpochMilli()));
    }
    if (null != segmentRow.getMap("replicas", String.class, String.class)) {
      builder = builder.withReplicas(segmentRow.getMap("replicas", String.class, String.class));
    }

    if (null != segmentRow.getUuid("host_id")) {
      builder = builder.withHostID(segmentRow.getUuid("host_id"));
    }

    return builder.withId(segmentRow.getUuid("segment_id")).build();
  }

  private void prepareStatements() {
    insertRepairSegmentPrepStmt = session
        .prepare(
            SimpleStatement.builder("INSERT INTO repair_run"
                + "(id,segment_id,repair_unit_id,start_token,end_token,"
                + " segment_state,fail_count, token_ranges, replicas,host_id)"
                + " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
              .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM).build());
    insertRepairSegmentIncrementalPrepStmt = session
        .prepare(
          SimpleStatement.builder("INSERT INTO repair_run"
                + "(id,segment_id,repair_unit_id,start_token,end_token,"
                + "segment_state,coordinator_host,fail_count,replicas,host_id)"
                + " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM).build());
    updateRepairSegmentPrepStmt = session
        .prepare(
          SimpleStatement.builder("INSERT INTO repair_run"
                + "(id,segment_id,segment_state,coordinator_host,segment_start_time,fail_count,host_id)"
                + " VALUES(?, ?, ?, ?, ?, ?, ?)")
            .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM).build());
    insertRepairSegmentEndTimePrepStmt = session
        .prepare(
          SimpleStatement.builder("INSERT INTO repair_run(id, segment_id, segment_end_time) VALUES(?, ?, ?)")
            .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM).build());
    getRepairSegmentPrepStmt = session
        .prepare(
          SimpleStatement.builder("SELECT id,repair_unit_id,segment_id,start_token,end_token,segment_state,"
                + "coordinator_host,segment_start_time,segment_end_time,fail_count, token_ranges, replicas, host_id"
                + " FROM repair_run WHERE id = ? and segment_id = ?")
            .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM).build());
    getRepairSegmentsByRunIdPrepStmt = session.prepare(
        "SELECT id,repair_unit_id,segment_id,start_token,end_token,segment_state,coordinator_host,segment_start_time,"
            + "segment_end_time,fail_count, token_ranges, replicas, host_id FROM repair_run WHERE id = ?");
    getRepairSegmentCountByRunIdPrepStmt = session.prepare(
        "SELECT count(*) FROM repair_run WHERE id = ?"
    );
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

  @Override
  public int getSegmentAmountForRepairRun(UUID runId) {
    return (int) session
        .execute(getRepairSegmentCountByRunIdPrepStmt.bind(runId))
        .one()
        .getLong(0);
  }

  @Override
  public int getSegmentAmountForRepairRunWithState(UUID runId, RepairSegment.State state) {
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
  public boolean updateRepairSegment(RepairSegment segment) {

    assert cassandraConcurrencyDao.hasLeadOnSegment(segment)
        || (cassandraConcurrencyDao.hasLeadOnSegment(segment.getRunId())
        && cassRepairUnitDao.getRepairUnit(segment.getRepairUnitId()).getIncrementalRepair())
        : "non-leader trying to update repair segment " + segment.getId() + " of run " + segment.getRunId();

    return updateRepairSegmentUnsafe(segment);
  }

  @Override
  public boolean updateRepairSegmentUnsafe(RepairSegment segment) {

    BatchStatementBuilder updateRepairSegmentBatch = BatchStatement.builder(BatchType.UNLOGGED);

    updateRepairSegmentBatch.addStatement(
        updateRepairSegmentPrepStmt.bind(
            segment.getRunId(),
            segment.getId(),
            segment.getState().ordinal(),
            segment.getCoordinatorHost(),
            segment.hasStartTime() ? Instant.ofEpochMilli(segment.getStartTime().getMillis()) : null,
            segment.getFailCount(),
            segment.getHostID()
        )
    );

    if (null != segment.getEndTime() || RepairSegment.State.NOT_STARTED == segment.getState()) {

      Preconditions.checkArgument(
          RepairSegment.State.RUNNING != segment.getState(),
          "un/setting endTime not permitted when state is RUNNING");

      Preconditions.checkArgument(
          RepairSegment.State.NOT_STARTED != segment.getState() || !segment.hasEndTime(),
          "endTime can only be nulled when state is NOT_STARTED");

      Preconditions.checkArgument(
          RepairSegment.State.DONE != segment.getState() || segment.hasEndTime(),
          "endTime can't be null when state is DONE");

      updateRepairSegmentBatch.addStatement(
          insertRepairSegmentEndTimePrepStmt.bind(
              segment.getRunId(),
              segment.getId(),
              segment.hasEndTime() ? Instant.ofEpochMilli(segment.getEndTime().getMillis()) : null));
    } else if (RepairSegment.State.STARTED == segment.getState()) {
      updateRepairSegmentBatch.setConsistencyLevel(ConsistencyLevel.EACH_QUORUM);
    }
    session.execute(updateRepairSegmentBatch.build());
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

  @Override
  public List<RepairSegment> getNextFreeSegments(UUID runId) {
    List<RepairSegment> segments = Lists.<RepairSegment>newArrayList(getRepairSegmentsForRun(runId));
    Collections.shuffle(segments);

    Set<String> lockedNodes = cassandraConcurrencyDao.getLockedNodesForRun(runId);
    List<RepairSegment> candidates = segments.stream()
        .filter(seg -> segmentIsCandidate(seg, lockedNodes))
        .collect(Collectors.toList());
    return candidates;
  }

  // TODO: this comes from IDistributed storage and probably shouldn't be here, despite being segment related.
  public List<RepairSegment> getNextFreeSegmentsForRanges(
      UUID runId,
      List<RingRange> ranges) {
    List<RepairSegment> segments
        = Lists.<RepairSegment>newArrayList(getRepairSegmentsForRun(runId));
    Collections.shuffle(segments);
    Set<String> lockedNodes = cassandraConcurrencyDao.getLockedNodesForRun(runId);
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
    return seg.getState().equals(RepairSegment.State.NOT_STARTED)
        && Sets.intersection(lockedNodes, seg.getReplicas().keySet()).isEmpty();
  }

  @Override
  public Collection<RepairSegment> getSegmentsWithState(UUID runId, RepairSegment.State segmentState) {
    Collection<RepairSegment> segments = Lists.newArrayList();

    BoundStatement statement = null != getRepairSegmentsByRunIdAndStatePrepStmt
        ? getRepairSegmentsByRunIdAndStatePrepStmt.bind(runId, segmentState.ordinal())
        // legacy mode for Cassandra-2 backends
        : getRepairSegmentsByRunIdPrepStmt.bind(runId);

    if (RepairSegment.State.STARTED == segmentState) {
      statement = statement.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
    }
    for (Row segmentRow : session.execute(statement)) {
      if (segmentRow.getInt("segment_state") == segmentState.ordinal()) {
        segments.add(createRepairSegmentFromRow(segmentRow));
      }
    }
    return segments;
  }
}