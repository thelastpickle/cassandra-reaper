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
import io.cassandrareaper.storage.MemoryStorageFacade;
import io.cassandrareaper.storage.sqlite.SqliteHelper;
import io.cassandrareaper.storage.sqlite.UuidUtil;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.datastax.oss.driver.api.core.uuid.Uuids;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryRepairSegmentDao implements IRepairSegmentDao {

  private static final Logger LOG = LoggerFactory.getLogger(MemoryRepairSegmentDao.class);

  private final MemoryStorageFacade memoryStorageFacade;
  private final Connection connection;

  private final PreparedStatement insertSegmentStmt;
  private final PreparedStatement getSegmentByIdStmt;
  private final PreparedStatement getSegmentsByRunIdStmt;
  private final PreparedStatement getSegmentsWithStateStmt;
  private final PreparedStatement deleteSegmentsByRunIdStmt;
  private final PreparedStatement countSegmentsByRunIdStmt;
  private final PreparedStatement countSegmentsWithStateStmt;
  private final PreparedStatement updateSegmentStmt;

  public MemoryRepairSegmentDao(MemoryStorageFacade memoryStorageFacade) {
    this.memoryStorageFacade = memoryStorageFacade;
    this.connection = memoryStorageFacade.getSqliteConnection();

    try {
      this.insertSegmentStmt =
          connection.prepareStatement(
              "INSERT OR REPLACE INTO repair_segment (id, run_id, repair_unit_id, start_token, "
                  + "end_token, token_ranges, state, coordinator_host, start_time, end_time, "
                  + "fail_count, replicas, host_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
      this.getSegmentByIdStmt =
          connection.prepareStatement("SELECT * FROM repair_segment WHERE id = ?");
      this.getSegmentsByRunIdStmt =
          connection.prepareStatement("SELECT * FROM repair_segment WHERE run_id = ?");
      this.getSegmentsWithStateStmt =
          connection.prepareStatement(
              "SELECT * FROM repair_segment WHERE run_id = ? AND state = ?");
      this.deleteSegmentsByRunIdStmt =
          connection.prepareStatement("DELETE FROM repair_segment WHERE run_id = ?");
      this.countSegmentsByRunIdStmt =
          connection.prepareStatement("SELECT COUNT(*) FROM repair_segment WHERE run_id = ?");
      this.countSegmentsWithStateStmt =
          connection.prepareStatement(
              "SELECT COUNT(*) FROM repair_segment WHERE run_id = ? AND state = ?");
      this.updateSegmentStmt =
          connection.prepareStatement(
              "UPDATE repair_segment SET state = ?, coordinator_host = ?, start_time = ?, "
                  + "end_time = ?, fail_count = ? WHERE id = ?");
    } catch (SQLException e) {
      LOG.error("Failed to prepare statements for MemoryRepairSegmentDao", e);
      throw new RuntimeException(e);
    }
  }

  public int deleteRepairSegmentsForRun(UUID runId) {
    try {
      deleteSegmentsByRunIdStmt.setBytes(1, UuidUtil.toBytes(runId));
      return deleteSegmentsByRunIdStmt.executeUpdate();
    } catch (SQLException e) {
      LOG.error("Failed to delete repair segments for run {}", runId, e);
      throw new RuntimeException(e);
    }
  }

  public void addRepairSegments(Collection<RepairSegment.Builder> segments, UUID runId) {
    try {
      for (RepairSegment.Builder segment : segments) {
        RepairSegment newRepairSegment = segment.withRunId(runId).withId(Uuids.timeBased()).build();
        insertSegment(newRepairSegment);
      }
    } catch (SQLException e) {
      LOG.error("Failed to add repair segments for run {}", runId, e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Add a repair segment with a predetermined ID (for testing/migration). Use addRepairSegments for
   * normal operation.
   */
  public void addRepairSegmentWithId(RepairSegment segment) {
    try {
      insertSegment(segment);
    } catch (SQLException e) {
      LOG.error("Failed to add repair segment {}", segment.getId(), e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean updateRepairSegment(RepairSegment newRepairSegment) {
    try {
      // Check if segment exists
      if (!getRepairSegment(newRepairSegment.getRunId(), newRepairSegment.getId()).isPresent()) {
        return false;
      }

      // For efficiency, we can use a lighter UPDATE instead of INSERT OR REPLACE
      synchronized (updateSegmentStmt) {
        updateSegmentStmt.clearParameters();
        updateSegmentStmt.setString(1, newRepairSegment.getState().name());
        updateSegmentStmt.setString(2, newRepairSegment.getCoordinatorHost());
        updateSegmentStmt.setObject(3, SqliteHelper.toEpochMilli(newRepairSegment.getStartTime()));
        updateSegmentStmt.setObject(4, SqliteHelper.toEpochMilli(newRepairSegment.getEndTime()));
        updateSegmentStmt.setInt(5, newRepairSegment.getFailCount());
        updateSegmentStmt.setBytes(6, UuidUtil.toBytes(newRepairSegment.getId()));

        int updated = updateSegmentStmt.executeUpdate();
        return updated > 0;
      }
    } catch (SQLException e) {
      LOG.error("Failed to update repair segment {}", newRepairSegment.getId(), e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean updateRepairSegmentUnsafe(RepairSegment newRepairSegment) {
    // For memory storage, unsafe update is the same as regular update
    return updateRepairSegment(newRepairSegment);
  }

  @Override
  public Optional<RepairSegment> getRepairSegment(UUID runId, UUID segmentId) {
    try {
      synchronized (getSegmentByIdStmt) {
        getSegmentByIdStmt.clearParameters();
        getSegmentByIdStmt.setBytes(1, UuidUtil.toBytes(segmentId));
        try (ResultSet rs = getSegmentByIdStmt.executeQuery()) {
          if (rs.next()) {
            return Optional.of(mapRowToRepairSegment(rs));
          }
        }
      }
    } catch (SQLException e) {
      LOG.error("Failed to get repair segment {}", segmentId, e);
      throw new RuntimeException(e);
    }
    return Optional.empty();
  }

  @Override
  public Collection<RepairSegment> getRepairSegmentsForRun(UUID runId) {
    try {
      getSegmentsByRunIdStmt.setBytes(1, UuidUtil.toBytes(runId));
      try (ResultSet rs = getSegmentsByRunIdStmt.executeQuery()) {
        List<RepairSegment> segments = new ArrayList<>();
        while (rs.next()) {
          segments.add(mapRowToRepairSegment(rs));
        }
        return segments;
      }
    } catch (SQLException e) {
      LOG.error("Failed to get repair segments for run {}", runId, e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<RepairSegment> getNextFreeSegments(UUID runId) {
    // Get all NOT_STARTED segments
    Collection<RepairSegment> segments =
        getSegmentsWithState(runId, RepairSegment.State.NOT_STARTED);

    // Get locked nodes for this run
    Set<String> lockedNodes =
        memoryStorageFacade.getLockedNodesForRun(runId).stream()
            .map(lockKey -> lockKey.substring(0, lockKey.indexOf(runId.toString())))
            .collect(Collectors.toSet());

    // Filter out segments which have a node that is locked by another segment in this run
    // We only allow one segment per node per run.
    return segments.stream()
        .filter(seg -> Collections.disjoint(lockedNodes, seg.getReplicas().keySet()))
        .collect(Collectors.toList());
  }

  @Override
  public Collection<RepairSegment> getSegmentsWithState(
      UUID runId, RepairSegment.State segmentState) {
    try {
      getSegmentsWithStateStmt.setBytes(1, UuidUtil.toBytes(runId));
      getSegmentsWithStateStmt.setString(2, segmentState.name());
      try (ResultSet rs = getSegmentsWithStateStmt.executeQuery()) {
        List<RepairSegment> segments = new ArrayList<>();
        while (rs.next()) {
          segments.add(mapRowToRepairSegment(rs));
        }
        return segments;
      }
    } catch (SQLException e) {
      LOG.error("Failed to get segments with state {} for run {}", segmentState, runId, e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public int getSegmentAmountForRepairRun(UUID runId) {
    try {
      countSegmentsByRunIdStmt.setBytes(1, UuidUtil.toBytes(runId));
      try (ResultSet rs = countSegmentsByRunIdStmt.executeQuery()) {
        if (rs.next()) {
          return rs.getInt(1);
        }
      }
    } catch (SQLException e) {
      LOG.error("Failed to count segments for run {}", runId, e);
      throw new RuntimeException(e);
    }
    return 0;
  }

  @Override
  public int getSegmentAmountForRepairRunWithState(UUID runId, RepairSegment.State state) {
    try {
      countSegmentsWithStateStmt.setBytes(1, UuidUtil.toBytes(runId));
      countSegmentsWithStateStmt.setString(2, state.name());
      try (ResultSet rs = countSegmentsWithStateStmt.executeQuery()) {
        if (rs.next()) {
          return rs.getInt(1);
        }
      }
    } catch (SQLException e) {
      LOG.error("Failed to count segments with state {} for run {}", state, runId, e);
      throw new RuntimeException(e);
    }
    return 0;
  }

  private void insertSegment(RepairSegment segment) throws SQLException {
    insertSegmentStmt.setBytes(1, UuidUtil.toBytes(segment.getId()));
    insertSegmentStmt.setBytes(2, UuidUtil.toBytes(segment.getRunId()));
    insertSegmentStmt.setBytes(3, UuidUtil.toBytes(segment.getRepairUnitId()));
    insertSegmentStmt.setString(4, segment.getStartToken().toString());
    insertSegmentStmt.setString(5, segment.getEndToken().toString());
    insertSegmentStmt.setString(
        6, SqliteHelper.toJson(Collections.emptySet())); // token_ranges not used
    insertSegmentStmt.setString(7, segment.getState().name());
    insertSegmentStmt.setString(8, segment.getCoordinatorHost());
    insertSegmentStmt.setObject(9, SqliteHelper.toEpochMilli(segment.getStartTime()));
    insertSegmentStmt.setObject(10, SqliteHelper.toEpochMilli(segment.getEndTime()));
    insertSegmentStmt.setInt(11, segment.getFailCount());
    insertSegmentStmt.setString(12, SqliteHelper.toJson(segment.getReplicas()));
    insertSegmentStmt.setBytes(
        13, segment.getHostID() != null ? UuidUtil.toBytes(segment.getHostID()) : null);
    insertSegmentStmt.executeUpdate();
  }

  private RepairSegment mapRowToRepairSegment(ResultSet rs) throws SQLException {
    UUID id = UuidUtil.fromBytes(rs.getBytes("id"));
    UUID runId = UuidUtil.fromBytes(rs.getBytes("run_id"));
    UUID repairUnitId = UuidUtil.fromBytes(rs.getBytes("repair_unit_id"));
    BigInteger startToken = new BigInteger(rs.getString("start_token"));
    BigInteger endToken = new BigInteger(rs.getString("end_token"));
    RepairSegment.State state = RepairSegment.State.valueOf(rs.getString("state"));
    String coordinatorHost = rs.getString("coordinator_host");
    Long startTimeMilli = SqliteHelper.toLong(rs.getObject("start_time"));
    Long endTimeMilli = SqliteHelper.toLong(rs.getObject("end_time"));
    int failCount = rs.getInt("fail_count");
    Map<String, String> replicas = SqliteHelper.fromJsonStringMap(rs.getString("replicas"));
    byte[] hostIdBytes = rs.getBytes("host_id");
    UUID hostId = hostIdBytes != null ? UuidUtil.fromBytes(hostIdBytes) : null;

    // Create a Segment for the token range
    RingRange range = new RingRange(startToken, endToken);
    Segment tokenRange =
        Segment.builder()
            .withTokenRange(range)
            .withReplicas(replicas != null ? replicas : Collections.emptyMap())
            .build();

    // Create the RepairSegment builder
    RepairSegment.Builder builder = RepairSegment.builder(tokenRange, repairUnitId);

    builder
        .withRunId(runId)
        .withState(state)
        .withCoordinatorHost(coordinatorHost)
        .withStartTime(SqliteHelper.fromEpochMilli(startTimeMilli))
        .withEndTime(SqliteHelper.fromEpochMilli(endTimeMilli))
        .withFailCount(failCount)
        .withHostID(hostId);

    return builder.withId(id).build();
  }
}
