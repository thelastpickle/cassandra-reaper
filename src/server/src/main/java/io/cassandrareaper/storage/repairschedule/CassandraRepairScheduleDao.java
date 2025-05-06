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
import io.cassandrareaper.storage.repairunit.CassandraRepairUnitDao;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.cassandra.repair.RepairParallelism;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraRepairScheduleDao implements IRepairScheduleDao {
  private static final String SELECT_REPAIR_SCHEDULE = "SELECT * FROM repair_schedule_v1";
  private static final Logger LOG = LoggerFactory.getLogger(CassandraRepairScheduleDao.class);
  PreparedStatement insertRepairSchedulePrepStmt;
  PreparedStatement getRepairSchedulePrepStmt;
  PreparedStatement getRepairScheduleByClusterAndKsPrepStmt;
  PreparedStatement insertRepairScheduleByClusterAndKsPrepStmt;
  PreparedStatement deleteRepairSchedulePrepStmt;
  PreparedStatement deleteRepairScheduleByClusterAndKsByIdPrepStmt;
  private final CassandraRepairUnitDao cassRepairUnitDao;
  private final CqlSession session;


  public CassandraRepairScheduleDao(CassandraRepairUnitDao cassRepairUnitDao, CqlSession session) {
    this.cassRepairUnitDao = cassRepairUnitDao;
    this.session = session;
    prepareStatements();
  }

  private void prepareStatements() {
    insertRepairSchedulePrepStmt = session
        .prepare(
            SimpleStatement.builder("INSERT INTO repair_schedule_v1(id, repair_unit_id, state,"
                + "days_between, next_activation, "
                + "repair_parallelism, intensity, "
                + "creation_time, owner, pause_time, segment_count_per_node, "
                + "adaptive, percent_unrepaired_threshold, last_run) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
              .setConsistencyLevel(ConsistencyLevel.QUORUM).build());
    getRepairSchedulePrepStmt = session
        .prepare(SimpleStatement.builder("SELECT * FROM repair_schedule_v1 WHERE id = ?")
          .setConsistencyLevel(ConsistencyLevel.QUORUM).build());
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

  @Override
  public RepairSchedule addRepairSchedule(RepairSchedule.Builder repairSchedule) {
    RepairSchedule schedule = repairSchedule.build(Uuids.timeBased());
    updateRepairSchedule(schedule);

    return schedule;
  }

  @Override
  public Optional<RepairSchedule> getRepairSchedule(UUID repairScheduleId) {
    Row sched = session.execute(getRepairSchedulePrepStmt.bind(repairScheduleId)).one();

    return sched != null ? Optional.ofNullable(createRepairScheduleFromRow(sched)) : Optional.empty();
  }

  private RepairSchedule createRepairScheduleFromRow(Row repairScheduleRow) {
    DateTime nextActivation = repairScheduleRow.getInstant("next_activation") != null
        ? new DateTime(repairScheduleRow.getInstant("next_activation").toEpochMilli())
        : null;
    DateTime creationTime = repairScheduleRow.getInstant("creation_time") != null
        ? new DateTime(repairScheduleRow.getInstant("creation_time").toEpochMilli())
        : null;
    DateTime pauseTime = repairScheduleRow.getInstant("pause_time") != null
        ? new DateTime(repairScheduleRow.getInstant("pause_time").toEpochMilli())
        : null;
    return RepairSchedule.builder(repairScheduleRow.getUuid("repair_unit_id"))
        .state(RepairSchedule.State.valueOf(repairScheduleRow.getString("state")))
        .daysBetween(repairScheduleRow.getInt("days_between"))
        .nextActivation(nextActivation)
        .runHistory(ImmutableList.copyOf(repairScheduleRow.getSet("run_history", UUID.class)))
        .repairParallelism(RepairParallelism.fromName(repairScheduleRow.getString("repair_parallelism")))
        .intensity(repairScheduleRow.getDouble("intensity"))
        .creationTime(creationTime)
        .segmentCountPerNode(repairScheduleRow.getInt("segment_count_per_node"))
        .owner(repairScheduleRow.getString("owner"))
        .pauseTime(pauseTime)
        .adaptive(repairScheduleRow.isNull("adaptive") ? false : repairScheduleRow.getBool("adaptive"))
        .percentUnrepairedThreshold(repairScheduleRow.isNull("percent_unrepaired_threshold")
            ? -1
            : repairScheduleRow.getInt("percent_unrepaired_threshold"))
        .lastRun(repairScheduleRow.getUuid("last_run"))
        .build(repairScheduleRow.getUuid("id"));
  }


  @Override
  public Collection<RepairSchedule> getRepairSchedulesForCluster(String clusterName) {
    Collection<RepairSchedule> schedules = Lists.<RepairSchedule>newArrayList();
    ResultSet scheduleIds = session.execute(getRepairScheduleByClusterAndKsPrepStmt.bind(clusterName, " "));
    for (Row scheduleId : scheduleIds) {
      Optional<RepairSchedule> schedule = getRepairSchedule(scheduleId.getUuid("repair_schedule_id"));
      schedule.ifPresent(schedules::add);
    }

    return schedules;
  }


  @Override
  public Collection<RepairSchedule> getRepairSchedulesForCluster(String clusterName, boolean incremental) {
    return getRepairSchedulesForCluster(clusterName).stream()
        .filter(schedule -> cassRepairUnitDao.getRepairUnit(
            schedule.getRepairUnitId()).getIncrementalRepair() == incremental
        )
        .collect(Collectors.toList());
  }


  @Override
  public Collection<RepairSchedule> getRepairSchedulesForKeyspace(String keyspaceName) {
    Collection<RepairSchedule> schedules = Lists.<RepairSchedule>newArrayList();
    ResultSet scheduleIds = session.execute(getRepairScheduleByClusterAndKsPrepStmt.bind(" ", keyspaceName));
    for (Row scheduleId : scheduleIds) {
      Optional<RepairSchedule> schedule = getRepairSchedule(scheduleId.getUuid("repair_schedule_id"));
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
      Optional<RepairSchedule> schedule = getRepairSchedule(scheduleId.getUuid("repair_schedule_id"));
      if (schedule.isPresent()) {
        schedules.add(schedule.get());
      }
    }

    return schedules;
  }


  @Override
  public Collection<RepairSchedule> getAllRepairSchedules() {
    Collection<RepairSchedule> schedules = Lists.<RepairSchedule>newArrayList();
    SimpleStatement stmt = SimpleStatement.builder(SELECT_REPAIR_SCHEDULE).setIdempotence(Boolean.TRUE).build();
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
    RepairUnit repairUnit = cassRepairUnitDao.getRepairUnit(newRepairSchedule.getRepairUnitId());
    List<CompletionStage<AsyncResultSet>> futures = Lists.newArrayList();
    Instant nextActivation = newRepairSchedule.getNextActivation() != null
        ? Instant.ofEpochMilli(newRepairSchedule.getNextActivation().getMillis())
        : null;

    Instant creationTime = newRepairSchedule.getCreationTime() != null
        ? Instant.ofEpochMilli(newRepairSchedule.getCreationTime().getMillis())
        : null;

    Instant pauseTime = newRepairSchedule.getPauseTime() != null
        ? Instant.ofEpochMilli(newRepairSchedule.getPauseTime().getMillis())
        : null;

    futures.add(
        session.executeAsync(
            insertRepairSchedulePrepStmt.bind(
                newRepairSchedule.getId(),
                newRepairSchedule.getRepairUnitId(),
                newRepairSchedule.getState().toString(),
                newRepairSchedule.getDaysBetween(),
                nextActivation,
                newRepairSchedule.getRepairParallelism().toString(),
                newRepairSchedule.getIntensity(),
                creationTime,
                newRepairSchedule.getOwner(),
                pauseTime,
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
      for (CompletionStage<AsyncResultSet> future : futures) {
        future.toCompletableFuture().join();
      }
    } catch (RuntimeException ex) {
      LOG.error("failed to quorum update repair schedule " + newRepairSchedule.getId(), ex);
    }

    return true;
  }


  @Override
  public Optional<RepairSchedule> deleteRepairSchedule(UUID id) {
    Optional<RepairSchedule> repairSchedule = getRepairSchedule(id);
    if (repairSchedule.isPresent()) {
      RepairUnit repairUnit = cassRepairUnitDao.getRepairUnit(repairSchedule.get().getRepairUnitId());

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
  public Collection<RepairScheduleStatus> getClusterScheduleStatuses(String clusterName) {
    Collection<RepairSchedule> repairSchedules = getRepairSchedulesForCluster(clusterName);

    Collection<RepairScheduleStatus> repairScheduleStatuses = repairSchedules
          .stream()
          .map(sched -> new RepairScheduleStatus(sched, cassRepairUnitDao.getRepairUnit(sched.getRepairUnitId())))
          .collect(Collectors.toList());

    return repairScheduleStatuses;
  }
}