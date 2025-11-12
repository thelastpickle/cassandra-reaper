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

package io.cassandrareaper.storage.repairschedule;

import io.cassandrareaper.core.RepairSchedule;
import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.resources.view.RepairScheduleStatus;
import io.cassandrareaper.storage.MemoryStorageFacade;
import io.cassandrareaper.storage.repairunit.MemoryRepairUnitDao;
import io.cassandrareaper.storage.sqlite.SqliteHelper;
import io.cassandrareaper.storage.sqlite.UuidUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.cassandra.repair.RepairParallelism;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryRepairScheduleDao implements IRepairScheduleDao {

  private static final Logger LOG = LoggerFactory.getLogger(MemoryRepairScheduleDao.class);

  private final MemoryRepairUnitDao memoryRepairUnitDao;
  private final MemoryStorageFacade storage;
  private final Connection connection;

  private final PreparedStatement insertScheduleStmt;
  private final PreparedStatement updateScheduleStmt;
  private final PreparedStatement getScheduleByIdStmt;
  private final PreparedStatement getAllSchedulesStmt;
  private final PreparedStatement deleteScheduleStmt;

  public MemoryRepairScheduleDao(
      MemoryStorageFacade storage, MemoryRepairUnitDao memoryRepairUnitDao) {
    this.memoryRepairUnitDao = memoryRepairUnitDao;
    this.storage = storage;
    this.connection = storage.getSqliteConnection();

    try {
      this.insertScheduleStmt =
          connection.prepareStatement(
              "INSERT OR REPLACE INTO repair_schedule (id, repair_unit_id, owner, state, "
                  + "days_between, next_activation, creation_time, pause_time, intensity, "
                  + "segment_count_per_node, repair_parallelism, adaptive, "
                  + "percent_unrepaired_threshold, run_history, last_run) "
                  + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
      this.updateScheduleStmt =
          connection.prepareStatement(
              "UPDATE repair_schedule SET repair_unit_id = ?, owner = ?, state = ?, "
                  + "days_between = ?, next_activation = ?, creation_time = ?, pause_time = ?, "
                  + "intensity = ?, segment_count_per_node = ?, repair_parallelism = ?, "
                  + "adaptive = ?, percent_unrepaired_threshold = ?, run_history = ?, "
                  + "last_run = ? WHERE id = ?");
      this.getScheduleByIdStmt =
          connection.prepareStatement("SELECT * FROM repair_schedule WHERE id = ?");
      this.getAllSchedulesStmt = connection.prepareStatement("SELECT * FROM repair_schedule");
      this.deleteScheduleStmt =
          connection.prepareStatement("DELETE FROM repair_schedule WHERE id = ?");
    } catch (SQLException e) {
      LOG.error("Failed to prepare statements for MemoryRepairScheduleDao", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public RepairSchedule addRepairSchedule(RepairSchedule.Builder repairSchedule) {
    RepairSchedule newRepairSchedule = repairSchedule.build(Uuids.timeBased());
    try {
      insertSchedule(newRepairSchedule);
    } catch (SQLException e) {
      LOG.error("Failed to add repair schedule {}", newRepairSchedule.getId(), e);
      throw new RuntimeException(e);
    }
    return newRepairSchedule;
  }

  @Override
  public Optional<RepairSchedule> getRepairSchedule(UUID id) {
    try {
      getScheduleByIdStmt.setBytes(1, UuidUtil.toBytes(id));
      try (ResultSet rs = getScheduleByIdStmt.executeQuery()) {
        if (rs.next()) {
          return Optional.of(mapRowToRepairSchedule(rs));
        }
      }
    } catch (SQLException e) {
      LOG.error("Failed to get repair schedule {}", id, e);
      throw new RuntimeException(e);
    }
    return Optional.empty();
  }

  @Override
  public Collection<RepairSchedule> getRepairSchedulesForCluster(String clusterName) {
    // JOIN with repair_unit to filter by cluster_name
    String sql =
        "SELECT s.* FROM repair_schedule s "
            + "INNER JOIN repair_unit u ON s.repair_unit_id = u.id "
            + "WHERE u.cluster_name = ?";
    try (PreparedStatement stmt = connection.prepareStatement(sql)) {
      stmt.setString(1, clusterName);
      try (ResultSet rs = stmt.executeQuery()) {
        List<RepairSchedule> schedules = new ArrayList<>();
        while (rs.next()) {
          schedules.add(mapRowToRepairSchedule(rs));
        }
        return schedules;
      }
    } catch (SQLException e) {
      LOG.error("Failed to get repair schedules for cluster {}", clusterName, e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public Collection<RepairSchedule> getRepairSchedulesForCluster(
      String clusterName, boolean incremental) {
    // JOIN with repair_unit to filter by cluster_name and incremental_repair
    String sql =
        "SELECT s.* FROM repair_schedule s "
            + "INNER JOIN repair_unit u ON s.repair_unit_id = u.id "
            + "WHERE u.cluster_name = ? AND u.incremental_repair = ?";
    try (PreparedStatement stmt = connection.prepareStatement(sql)) {
      stmt.setString(1, clusterName);
      stmt.setInt(2, incremental ? 1 : 0);
      try (ResultSet rs = stmt.executeQuery()) {
        List<RepairSchedule> schedules = new ArrayList<>();
        while (rs.next()) {
          schedules.add(mapRowToRepairSchedule(rs));
        }
        return schedules;
      }
    } catch (SQLException e) {
      LOG.error(
          "Failed to get repair schedules for cluster {} with incremental={}",
          clusterName,
          incremental,
          e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public Collection<RepairSchedule> getRepairSchedulesForKeyspace(String keyspaceName) {
    // JOIN with repair_unit to filter by keyspace_name
    String sql =
        "SELECT s.* FROM repair_schedule s "
            + "INNER JOIN repair_unit u ON s.repair_unit_id = u.id "
            + "WHERE u.keyspace_name = ?";
    try (PreparedStatement stmt = connection.prepareStatement(sql)) {
      stmt.setString(1, keyspaceName);
      try (ResultSet rs = stmt.executeQuery()) {
        List<RepairSchedule> schedules = new ArrayList<>();
        while (rs.next()) {
          schedules.add(mapRowToRepairSchedule(rs));
        }
        return schedules;
      }
    } catch (SQLException e) {
      LOG.error("Failed to get repair schedules for keyspace {}", keyspaceName, e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public Collection<RepairSchedule> getRepairSchedulesForClusterAndKeyspace(
      String clusterName, String keyspaceName) {
    // JOIN with repair_unit to filter by both cluster_name and keyspace_name
    String sql =
        "SELECT s.* FROM repair_schedule s "
            + "INNER JOIN repair_unit u ON s.repair_unit_id = u.id "
            + "WHERE u.cluster_name = ? AND u.keyspace_name = ?";
    try (PreparedStatement stmt = connection.prepareStatement(sql)) {
      stmt.setString(1, clusterName);
      stmt.setString(2, keyspaceName);
      try (ResultSet rs = stmt.executeQuery()) {
        List<RepairSchedule> schedules = new ArrayList<>();
        while (rs.next()) {
          schedules.add(mapRowToRepairSchedule(rs));
        }
        return schedules;
      }
    } catch (SQLException e) {
      LOG.error(
          "Failed to get repair schedules for cluster {} and keyspace {}",
          clusterName,
          keyspaceName,
          e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public Collection<RepairSchedule> getAllRepairSchedules() {
    try (ResultSet rs = getAllSchedulesStmt.executeQuery()) {
      List<RepairSchedule> schedules = new ArrayList<>();
      while (rs.next()) {
        schedules.add(mapRowToRepairSchedule(rs));
      }
      return schedules;
    } catch (SQLException e) {
      LOG.error("Failed to get all repair schedules", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean updateRepairSchedule(RepairSchedule newRepairSchedule) {
    try {
      // Check if the schedule exists
      if (!getRepairSchedule(newRepairSchedule.getId()).isPresent()) {
        return false;
      }
      updateScheduleInDb(newRepairSchedule);
      return true;
    } catch (SQLException e) {
      LOG.error("Failed to update repair schedule {}", newRepairSchedule.getId(), e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public Collection<RepairScheduleStatus> getClusterScheduleStatuses(String clusterName) {
    List<RepairScheduleStatus> scheduleStatuses = Lists.newArrayList();
    Collection<RepairSchedule> schedules = getRepairSchedulesForCluster(clusterName);
    for (RepairSchedule schedule : schedules) {
      RepairUnit unit = memoryRepairUnitDao.getRepairUnit(schedule.getRepairUnitId());
      scheduleStatuses.add(new RepairScheduleStatus(schedule, unit));
    }
    return scheduleStatuses;
  }

  @Override
  public Optional<RepairSchedule> deleteRepairSchedule(UUID id) {
    try {
      Optional<RepairSchedule> scheduleOpt = getRepairSchedule(id);
      if (!scheduleOpt.isPresent()) {
        return Optional.empty();
      }

      RepairSchedule schedule = scheduleOpt.get();
      deleteScheduleStmt.setBytes(1, UuidUtil.toBytes(id));
      deleteScheduleStmt.executeUpdate();

      // Return the schedule with state set to DELETED
      RepairSchedule deletedSchedule =
          schedule.with().state(RepairSchedule.State.DELETED).build(id);
      return Optional.of(deletedSchedule);
    } catch (SQLException e) {
      LOG.error("Failed to delete repair schedule {}", id, e);
      throw new RuntimeException(e);
    }
  }

  private void insertSchedule(RepairSchedule schedule) throws SQLException {
    insertScheduleStmt.setBytes(1, UuidUtil.toBytes(schedule.getId()));
    insertScheduleStmt.setBytes(2, UuidUtil.toBytes(schedule.getRepairUnitId()));
    insertScheduleStmt.setString(3, schedule.getOwner());
    insertScheduleStmt.setString(4, schedule.getState().name());
    insertScheduleStmt.setInt(5, schedule.getDaysBetween());
    insertScheduleStmt.setObject(6, SqliteHelper.toEpochMilli(schedule.getNextActivation()));
    insertScheduleStmt.setObject(7, SqliteHelper.toEpochMilli(schedule.getCreationTime()));
    insertScheduleStmt.setObject(8, SqliteHelper.toEpochMilli(schedule.getPauseTime()));
    insertScheduleStmt.setDouble(9, schedule.getIntensity());
    insertScheduleStmt.setInt(10, schedule.getSegmentCountPerNode());
    insertScheduleStmt.setString(11, schedule.getRepairParallelism().name());
    insertScheduleStmt.setInt(12, schedule.getAdaptive() ? 1 : 0);
    insertScheduleStmt.setObject(
        13,
        schedule.getPercentUnrepairedThreshold() != -1
            ? schedule.getPercentUnrepairedThreshold()
            : null);
    insertScheduleStmt.setString(14, SqliteHelper.toJson(schedule.getRunHistory()));
    insertScheduleStmt.setBytes(
        15, schedule.getLastRun() != null ? UuidUtil.toBytes(schedule.getLastRun()) : null);
    insertScheduleStmt.executeUpdate();
  }

  private void updateScheduleInDb(RepairSchedule schedule) throws SQLException {
    updateScheduleStmt.setBytes(1, UuidUtil.toBytes(schedule.getRepairUnitId()));
    updateScheduleStmt.setString(2, schedule.getOwner());
    updateScheduleStmt.setString(3, schedule.getState().name());
    updateScheduleStmt.setInt(4, schedule.getDaysBetween());
    updateScheduleStmt.setObject(5, SqliteHelper.toEpochMilli(schedule.getNextActivation()));
    updateScheduleStmt.setObject(6, SqliteHelper.toEpochMilli(schedule.getCreationTime()));
    updateScheduleStmt.setObject(7, SqliteHelper.toEpochMilli(schedule.getPauseTime()));
    updateScheduleStmt.setDouble(8, schedule.getIntensity());
    updateScheduleStmt.setInt(9, schedule.getSegmentCountPerNode());
    updateScheduleStmt.setString(10, schedule.getRepairParallelism().name());
    updateScheduleStmt.setInt(11, schedule.getAdaptive() ? 1 : 0);
    updateScheduleStmt.setObject(
        12,
        schedule.getPercentUnrepairedThreshold() != -1
            ? schedule.getPercentUnrepairedThreshold()
            : null);
    updateScheduleStmt.setString(13, SqliteHelper.toJson(schedule.getRunHistory()));
    updateScheduleStmt.setBytes(
        14, schedule.getLastRun() != null ? UuidUtil.toBytes(schedule.getLastRun()) : null);
    updateScheduleStmt.setBytes(15, UuidUtil.toBytes(schedule.getId()));
    updateScheduleStmt.executeUpdate();
  }

  private RepairSchedule mapRowToRepairSchedule(ResultSet rs) throws SQLException {
    UUID id = UuidUtil.fromBytes(rs.getBytes("id"));
    UUID repairUnitId = UuidUtil.fromBytes(rs.getBytes("repair_unit_id"));

    // Deserialize run_history JSON to List<UUID>
    String runHistoryJson = rs.getString("run_history");
    ImmutableList<UUID> runHistory;
    if (runHistoryJson != null && !runHistoryJson.isEmpty()) {
      List<UUID> historyList =
          SqliteHelper.fromJson(runHistoryJson, new TypeReference<List<UUID>>() {});
      runHistory = historyList != null ? ImmutableList.copyOf(historyList) : ImmutableList.of();
    } else {
      runHistory = ImmutableList.of();
    }

    byte[] lastRunBytes = rs.getBytes("last_run");
    UUID lastRun = lastRunBytes != null ? UuidUtil.fromBytes(lastRunBytes) : null;

    RepairSchedule.Builder builder = RepairSchedule.builder(repairUnitId);
    builder
        .owner(rs.getString("owner"))
        .state(RepairSchedule.State.valueOf(rs.getString("state")))
        .daysBetween(rs.getInt("days_between"))
        .nextActivation(SqliteHelper.fromEpochMilli(rs.getLong("next_activation")))
        .creationTime(SqliteHelper.fromEpochMilli(rs.getLong("creation_time")))
        .pauseTime(SqliteHelper.fromEpochMilli(rs.getLong("pause_time")))
        .intensity(rs.getDouble("intensity"))
        .segmentCountPerNode(rs.getInt("segment_count_per_node"))
        .repairParallelism(RepairParallelism.valueOf(rs.getString("repair_parallelism")))
        .adaptive(rs.getInt("adaptive") == 1)
        .runHistory(runHistory)
        .lastRun(lastRun);

    // Handle percent_unrepaired_threshold - could be null
    Object percentUnrepairedObj = rs.getObject("percent_unrepaired_threshold");
    if (percentUnrepairedObj != null) {
      builder.percentUnrepairedThreshold(((Number) percentUnrepairedObj).intValue());
    }

    return builder.build(id);
  }
}
