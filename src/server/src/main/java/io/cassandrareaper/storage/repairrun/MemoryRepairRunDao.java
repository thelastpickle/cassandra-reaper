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
import io.cassandrareaper.service.RepairRunService;
import io.cassandrareaper.storage.MemoryStorageFacade;
import io.cassandrareaper.storage.repairsegment.MemoryRepairSegmentDao;
import io.cassandrareaper.storage.repairunit.MemoryRepairUnitDao;
import io.cassandrareaper.storage.sqlite.SqliteHelper;
import io.cassandrareaper.storage.sqlite.UuidUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.UUID;

import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.cassandra.repair.RepairParallelism;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryRepairRunDao implements IRepairRunDao {

  private static final Logger LOG = LoggerFactory.getLogger(MemoryRepairRunDao.class);

  private final MemoryRepairSegmentDao memRepairSegment;
  private final MemoryRepairUnitDao memoryRepairUnitDao;
  private final MemoryStorageFacade storage;
  private final Connection connection;

  private final PreparedStatement insertRepairRunStmt;
  private final PreparedStatement updateRepairRunStmt;
  private final PreparedStatement getRepairRunByIdStmt;
  private final PreparedStatement getRepairRunsForClusterStmt;
  private final PreparedStatement getRepairRunsForUnitStmt;
  private final PreparedStatement getRepairRunsWithStateStmt;
  private final PreparedStatement deleteRepairRunStmt;
  private final PreparedStatement getAllRepairRunsStmt;

  public MemoryRepairRunDao(
      MemoryStorageFacade storage,
      MemoryRepairSegmentDao memRepairSegment,
      MemoryRepairUnitDao memoryRepairUnitDao) {
    this.memRepairSegment = memRepairSegment;
    this.memoryRepairUnitDao = memoryRepairUnitDao;
    this.storage = storage;
    this.connection = storage.getSqliteConnection();

    try {
      this.insertRepairRunStmt =
          connection.prepareStatement(
              "INSERT OR REPLACE INTO repair_run (id, cluster_name, repair_unit_id, cause, owner, state, "
                  + "creation_time, start_time, end_time, pause_time, intensity, last_event, segment_count, "
                  + "repair_parallelism, tables, adaptive_schedule) "
                  + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
      this.updateRepairRunStmt =
          connection.prepareStatement(
              "UPDATE repair_run SET cluster_name = ?, repair_unit_id = ?, cause = ?, owner = ?, state = ?, "
                  + "creation_time = ?, start_time = ?, end_time = ?, pause_time = ?, intensity = ?, "
                  + "last_event = ?, segment_count = ?, repair_parallelism = ?, tables = ?, "
                  + "adaptive_schedule = ? WHERE id = ?");
      this.getRepairRunByIdStmt =
          connection.prepareStatement("SELECT * FROM repair_run WHERE id = ?");
      this.getRepairRunsForClusterStmt =
          connection.prepareStatement(
              "SELECT * FROM repair_run WHERE cluster_name = ? ORDER BY creation_time DESC LIMIT ?");
      this.getRepairRunsForUnitStmt =
          connection.prepareStatement("SELECT * FROM repair_run WHERE repair_unit_id = ?");
      this.getRepairRunsWithStateStmt =
          connection.prepareStatement("SELECT * FROM repair_run WHERE state = ?");
      this.deleteRepairRunStmt = connection.prepareStatement("DELETE FROM repair_run WHERE id = ?");
      this.getAllRepairRunsStmt = connection.prepareStatement("SELECT * FROM repair_run");
    } catch (SQLException e) {
      LOG.error("Failed to prepare statements for MemoryRepairRunDao", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public Collection<RepairRunStatus> getClusterRunStatuses(String clusterName, int limit) {
    List<RepairRunStatus> runStatuses = Lists.newArrayList();
    for (RepairRun run : getRepairRunsForCluster(clusterName, Optional.of(limit))) {
      RepairUnit unit = memoryRepairUnitDao.getRepairUnit(run.getRepairUnitId());
      int segmentsRepaired =
          memRepairSegment.getSegmentAmountForRepairRunWithState(
              run.getId(), RepairSegment.State.DONE);
      int totalSegments = memRepairSegment.getSegmentAmountForRepairRun(run.getId());
      runStatuses.add(
          new RepairRunStatus(
              run.getId(),
              clusterName,
              unit.getKeyspaceName(),
              run.getTables(),
              segmentsRepaired,
              totalSegments,
              run.getRunState(),
              run.getStartTime(),
              run.getEndTime(),
              run.getCause(),
              run.getOwner(),
              run.getLastEvent(),
              run.getCreationTime(),
              run.getPauseTime(),
              run.getIntensity(),
              unit.getIncrementalRepair(),
              unit.getSubrangeIncrementalRepair(),
              run.getRepairParallelism(),
              unit.getNodes(),
              unit.getDatacenters(),
              unit.getBlacklistedTables(),
              unit.getRepairThreadCount(),
              unit.getId(),
              unit.getTimeout(),
              run.getAdaptiveSchedule()));
    }
    return runStatuses;
  }

  @Override
  public RepairRun addRepairRun(
      RepairRun.Builder repairRun, Collection<RepairSegment.Builder> newSegments) {
    RepairRun newRepairRun = repairRun.build(Uuids.timeBased());
    try {
      insertRepairRunStmt.setBytes(1, UuidUtil.toBytes(newRepairRun.getId()));
      insertRepairRunStmt.setString(2, newRepairRun.getClusterName());
      insertRepairRunStmt.setBytes(3, UuidUtil.toBytes(newRepairRun.getRepairUnitId()));
      insertRepairRunStmt.setString(4, newRepairRun.getCause());
      insertRepairRunStmt.setString(5, newRepairRun.getOwner());
      insertRepairRunStmt.setString(6, newRepairRun.getRunState().name());
      insertRepairRunStmt.setObject(7, SqliteHelper.toEpochMilli(newRepairRun.getCreationTime()));
      insertRepairRunStmt.setObject(8, SqliteHelper.toEpochMilli(newRepairRun.getStartTime()));
      insertRepairRunStmt.setObject(9, SqliteHelper.toEpochMilli(newRepairRun.getEndTime()));
      insertRepairRunStmt.setObject(10, SqliteHelper.toEpochMilli(newRepairRun.getPauseTime()));
      insertRepairRunStmt.setDouble(11, newRepairRun.getIntensity());
      insertRepairRunStmt.setString(12, newRepairRun.getLastEvent());
      insertRepairRunStmt.setInt(13, newRepairRun.getSegmentCount());
      insertRepairRunStmt.setString(14, newRepairRun.getRepairParallelism().name());
      insertRepairRunStmt.setString(15, SqliteHelper.toJson(newRepairRun.getTables()));
      insertRepairRunStmt.setInt(16, newRepairRun.getAdaptiveSchedule() ? 1 : 0);
      insertRepairRunStmt.executeUpdate();
    } catch (SQLException e) {
      LOG.error("Failed to add repair run {}", newRepairRun.getId(), e);
      throw new RuntimeException(e);
    }
    memRepairSegment.addRepairSegments(newSegments, newRepairRun.getId());
    return newRepairRun;
  }

  public boolean updateRepairRun(RepairRun repairRun) {
    return updateRepairRun(repairRun, Optional.of(true));
  }

  public boolean updateRepairRun(RepairRun repairRun, Optional<Boolean> updateRepairState) {
    if (!getRepairRun(repairRun.getId()).isPresent()) {
      return false;
    }
    try {
      updateRepairRunStmt.setString(1, repairRun.getClusterName());
      updateRepairRunStmt.setBytes(2, UuidUtil.toBytes(repairRun.getRepairUnitId()));
      updateRepairRunStmt.setString(3, repairRun.getCause());
      updateRepairRunStmt.setString(4, repairRun.getOwner());
      updateRepairRunStmt.setString(5, repairRun.getRunState().name());
      updateRepairRunStmt.setObject(6, SqliteHelper.toEpochMilli(repairRun.getCreationTime()));
      updateRepairRunStmt.setObject(7, SqliteHelper.toEpochMilli(repairRun.getStartTime()));
      updateRepairRunStmt.setObject(8, SqliteHelper.toEpochMilli(repairRun.getEndTime()));
      updateRepairRunStmt.setObject(9, SqliteHelper.toEpochMilli(repairRun.getPauseTime()));
      updateRepairRunStmt.setDouble(10, repairRun.getIntensity());
      updateRepairRunStmt.setString(11, repairRun.getLastEvent());
      updateRepairRunStmt.setInt(12, repairRun.getSegmentCount());
      updateRepairRunStmt.setString(13, repairRun.getRepairParallelism().name());
      updateRepairRunStmt.setString(14, SqliteHelper.toJson(repairRun.getTables()));
      updateRepairRunStmt.setInt(15, repairRun.getAdaptiveSchedule() ? 1 : 0);
      updateRepairRunStmt.setBytes(16, UuidUtil.toBytes(repairRun.getId()));
      int affectedRows = updateRepairRunStmt.executeUpdate();
      return affectedRows > 0;
    } catch (SQLException e) {
      LOG.error("Failed to update repair run {}", repairRun.getId(), e);
      throw new RuntimeException(e);
    }
  }

  public Optional<RepairRun> getRepairRun(UUID id) {
    try {
      getRepairRunByIdStmt.setBytes(1, UuidUtil.toBytes(id));
      try (ResultSet rs = getRepairRunByIdStmt.executeQuery()) {
        if (rs.next()) {
          return Optional.of(mapRowToRepairRun(rs));
        }
      }
    } catch (SQLException e) {
      LOG.error("Failed to get repair run {}", id, e);
      throw new RuntimeException(e);
    }
    return Optional.empty();
  }

  public List<RepairRun> getRepairRunsForCluster(String clusterName, Optional<Integer> limit) {
    List<RepairRun> foundRepairRuns = new ArrayList<>();
    try {
      getRepairRunsForClusterStmt.setString(1, clusterName);
      getRepairRunsForClusterStmt.setInt(2, limit.orElse(1000));
      try (ResultSet rs = getRepairRunsForClusterStmt.executeQuery()) {
        while (rs.next()) {
          foundRepairRuns.add(mapRowToRepairRun(rs));
        }
      }
    } catch (SQLException e) {
      LOG.error("Failed to get repair runs for cluster {}", clusterName, e);
      throw new RuntimeException(e);
    }
    return foundRepairRuns;
  }

  public List<RepairRun> getRepairRunsForClusterPrioritiseRunning(
      String clusterName, Optional<Integer> limit) {
    // Get all runs for cluster, then sort by run state
    try {
      List<RepairRun> foundRepairRuns = new ArrayList<>();
      PreparedStatement stmt =
          connection.prepareStatement(
              "SELECT * FROM repair_run WHERE cluster_name = ? ORDER BY state ASC, creation_time DESC");
      stmt.setString(1, clusterName.toLowerCase(Locale.ROOT));
      try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
          foundRepairRuns.add(mapRowToRepairRun(rs));
        }
      }
      RepairRunService.sortByRunState(foundRepairRuns);
      return foundRepairRuns.subList(0, Math.min(foundRepairRuns.size(), limit.orElse(1000)));
    } catch (SQLException e) {
      LOG.error("Failed to get repair runs for cluster {} prioritized", clusterName, e);
      throw new RuntimeException(e);
    }
  }

  public Collection<RepairRun> getRepairRunsForUnit(UUID repairUnitId) {
    List<RepairRun> foundRepairRuns = new ArrayList<>();
    try {
      getRepairRunsForUnitStmt.setBytes(1, UuidUtil.toBytes(repairUnitId));
      try (ResultSet rs = getRepairRunsForUnitStmt.executeQuery()) {
        while (rs.next()) {
          foundRepairRuns.add(mapRowToRepairRun(rs));
        }
      }
    } catch (SQLException e) {
      LOG.error("Failed to get repair runs for unit {}", repairUnitId, e);
      throw new RuntimeException(e);
    }
    return foundRepairRuns;
  }

  public Collection<RepairRun> getRepairRunsWithState(RepairRun.RunState runState) {
    List<RepairRun> foundRepairRuns = new ArrayList<>();
    try {
      getRepairRunsWithStateStmt.setString(1, runState.name());
      try (ResultSet rs = getRepairRunsWithStateStmt.executeQuery()) {
        while (rs.next()) {
          foundRepairRuns.add(mapRowToRepairRun(rs));
        }
      }
    } catch (SQLException e) {
      LOG.error("Failed to get repair runs with state {}", runState, e);
      throw new RuntimeException(e);
    }
    return foundRepairRuns;
  }

  public Optional<RepairRun> deleteRepairRun(UUID id) {
    Optional<RepairRun> deletedRunOpt = getRepairRun(id);
    if (deletedRunOpt.isPresent()) {
      RepairRun deletedRun = deletedRunOpt.get();
      if (memRepairSegment.getSegmentAmountForRepairRunWithState(id, RepairSegment.State.RUNNING)
          == 0) {
        memRepairSegment.deleteRepairSegmentsForRun(id);

        try {
          deleteRepairRunStmt.setBytes(1, UuidUtil.toBytes(id));
          deleteRepairRunStmt.executeUpdate();
        } catch (SQLException e) {
          LOG.error("Failed to delete repair run {}", id, e);
          throw new RuntimeException(e);
        }

        deletedRun =
            deletedRun
                .with()
                .runState(RepairRun.RunState.DELETED)
                .endTime(DateTime.now())
                .build(id);
      }
      return Optional.of(deletedRun);
    }
    return Optional.empty();
  }

  public SortedSet<UUID> getRepairRunIdsForCluster(String clusterName, Optional<Integer> limit) {
    SortedSet<UUID> repairRunIds =
        Sets.newTreeSet((u0, u1) -> (int) (u0.timestamp() - u1.timestamp()));
    try {
      getRepairRunsForClusterStmt.setString(1, clusterName);
      getRepairRunsForClusterStmt.setInt(2, limit.orElse(1000));
      try (ResultSet rs = getRepairRunsForClusterStmt.executeQuery()) {
        while (rs.next()) {
          repairRunIds.add(UuidUtil.fromBytes(rs.getBytes("id")));
        }
      }
    } catch (SQLException e) {
      LOG.error("Failed to get repair run IDs for cluster {}", clusterName, e);
      throw new RuntimeException(e);
    }
    return repairRunIds;
  }

  /**
   * Maps a ResultSet row to a RepairRun object.
   *
   * @param rs The ResultSet positioned at the row to map
   * @return The mapped RepairRun object
   * @throws SQLException if a database error occurs
   */
  private RepairRun mapRowToRepairRun(ResultSet rs) throws SQLException {
    UUID id = UuidUtil.fromBytes(rs.getBytes("id"));
    String clusterName = rs.getString("cluster_name");
    UUID repairUnitId = UuidUtil.fromBytes(rs.getBytes("repair_unit_id"));
    String cause = rs.getString("cause");
    String owner = rs.getString("owner");
    RepairRun.RunState state = RepairRun.RunState.valueOf(rs.getString("state"));
    DateTime creationTime = SqliteHelper.fromEpochMilli((Long) rs.getObject("creation_time"));
    DateTime startTime = SqliteHelper.fromEpochMilli((Long) rs.getObject("start_time"));
    DateTime endTime = SqliteHelper.fromEpochMilli((Long) rs.getObject("end_time"));
    DateTime pauseTime = SqliteHelper.fromEpochMilli((Long) rs.getObject("pause_time"));
    double intensity = rs.getDouble("intensity");
    String lastEvent = rs.getString("last_event");
    int segmentCount = rs.getInt("segment_count");
    RepairParallelism repairParallelism =
        RepairParallelism.valueOf(rs.getString("repair_parallelism"));
    Set<String> tables =
        SqliteHelper.fromJsonStringCollection(rs.getString("tables"), HashSet.class);
    boolean adaptiveSchedule = rs.getInt("adaptive_schedule") == 1;

    return RepairRun.builder(clusterName, repairUnitId)
        .cause(cause)
        .owner(owner)
        .runState(state)
        .creationTime(creationTime)
        .startTime(startTime)
        .endTime(endTime)
        .pauseTime(pauseTime)
        .intensity(intensity)
        .lastEvent(lastEvent)
        .segmentCount(segmentCount)
        .repairParallelism(repairParallelism)
        .tables(tables)
        .adaptiveSchedule(adaptiveSchedule)
        .build(id);
  }
}
