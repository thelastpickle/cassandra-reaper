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

package io.cassandrareaper.storage.postgresql;

import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.RepairRun;
import io.cassandrareaper.core.RepairSchedule;
import io.cassandrareaper.core.RepairSegment;
import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.core.Snapshot;
import io.cassandrareaper.resources.view.RepairRunStatus;
import io.cassandrareaper.resources.view.RepairScheduleStatus;
import io.cassandrareaper.service.RepairParameters;

import java.math.BigInteger;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.BindBean;
import org.skife.jdbi.v2.sqlobject.GetGeneratedKeys;
import org.skife.jdbi.v2.sqlobject.SqlBatch;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.customizers.BatchChunkSize;
import org.skife.jdbi.v2.sqlobject.customizers.Mapper;

/**
 * JDBI based PostgreSQL interface.
 *
 * <p>See following specification for more info: http://jdbi.org/sql_object_api_dml/
 *
 */
public interface IStoragePostgreSql {

  // Cluster
  //
  String SQL_CLUSTER_ALL_FIELDS = "name, partitioner, seed_hosts";
  String SQL_GET_ALL_CLUSTERS = "SELECT " + SQL_CLUSTER_ALL_FIELDS + " FROM cluster";
  String SQL_GET_CLUSTER = "SELECT " + SQL_CLUSTER_ALL_FIELDS + " FROM cluster WHERE name = :name";
  String SQL_INSERT_CLUSTER = "INSERT INTO cluster (" + SQL_CLUSTER_ALL_FIELDS
      + ") VALUES (:name, :partitioner, :seedHosts)";
  String SQL_UPDATE_CLUSTER = "UPDATE cluster SET partitioner = :partitioner, "
      + "seed_hosts = :seedHosts WHERE name = :name";
  String SQL_DELETE_CLUSTER = "DELETE FROM cluster WHERE name = :name";

  // RepairRun
  //
  String SQL_REPAIR_RUN_ALL_FIELDS_NO_ID = "cluster_name, repair_unit_id, cause, owner, state, creation_time, "
      + "start_time, end_time, pause_time, intensity, last_event, "
      + "segment_count, repair_parallelism";
  String SQL_REPAIR_RUN_ALL_FIELDS = "repair_run.id, " + SQL_REPAIR_RUN_ALL_FIELDS_NO_ID;
  String SQL_INSERT_REPAIR_RUN = "INSERT INTO repair_run ("
      + SQL_REPAIR_RUN_ALL_FIELDS_NO_ID
      + ") VALUES "
      + "(:clusterName, :repairUnitId, :cause, :owner, :runState, :creationTime, "
      + ":startTime, :endTime, :pauseTime, :intensity, :lastEvent, :segmentCount, "
      + ":repairParallelism)";
  String SQL_UPDATE_REPAIR_RUN = "UPDATE repair_run SET cause = :cause, owner = :owner, state = :runState, "
      + "start_time = :startTime, end_time = :endTime, pause_time = :pauseTime, "
      + "intensity = :intensity, last_event = :lastEvent, segment_count = :segmentCount, "
      + "repair_parallelism = :repairParallelism WHERE id = :id";
  String SQL_GET_REPAIR_RUN = "SELECT " + SQL_REPAIR_RUN_ALL_FIELDS + " FROM repair_run WHERE id = :id";
  String SQL_GET_REPAIR_RUNS_FOR_CLUSTER = "SELECT " + SQL_REPAIR_RUN_ALL_FIELDS
      + " FROM repair_run WHERE cluster_name = :clusterName";
  String SQL_GET_REPAIR_RUNS_WITH_STATE = "SELECT " + SQL_REPAIR_RUN_ALL_FIELDS
      + " FROM repair_run WHERE state = :state";
  String SQL_GET_REPAIR_RUNS_FOR_UNIT = "SELECT " + SQL_REPAIR_RUN_ALL_FIELDS
      + " FROM repair_run WHERE repair_unit_id = :unitId";
  String SQL_DELETE_REPAIR_RUN = "DELETE FROM repair_run WHERE id = :id";

  // RepairUni
  //
  String SQL_REPAIR_UNIT_ALL_FIELDS_NO_ID =
      "cluster_name, keyspace_name, column_families, "
          + "incremental_repair, nodes, datacenters, blacklisted_tables, repair_thread_count";
  String SQL_REPAIR_UNIT_ALL_FIELDS = "repair_unit.id, " + SQL_REPAIR_UNIT_ALL_FIELDS_NO_ID;
  String SQL_INSERT_REPAIR_UNIT =
      "INSERT INTO repair_unit ("
          + SQL_REPAIR_UNIT_ALL_FIELDS_NO_ID
          + ") VALUES "
          + "(:clusterName, :keyspaceName, :columnFamilies, "
          + ":incrementalRepair, :nodes, :datacenters, :blacklistedTables, :repairThreadCount)";
  String SQL_GET_REPAIR_UNIT = "SELECT " + SQL_REPAIR_UNIT_ALL_FIELDS + " FROM repair_unit WHERE id = :id";

  String SQL_GET_REPAIR_UNIT_BY_CLUSTER_AND_TABLES =
      "SELECT "
          + SQL_REPAIR_UNIT_ALL_FIELDS
          + " FROM repair_unit "
          + "WHERE cluster_name = :clusterName AND keyspace_name = :keyspaceName "
          + "AND column_families = :columnFamilies AND nodes = :nodes AND datacenters = :datacenters "
          + "AND blackListed_tables = :blacklisted_tables AND repair_thread_count = :repairThreadCount";

  String SQL_DELETE_REPAIR_UNIT = "DELETE FROM repair_unit WHERE id = :id";

  // RepairSegmen
  //
  String SQL_REPAIR_SEGMENT_ALL_FIELDS_NO_ID =
      "repair_unit_id, run_id, start_token, end_token, state, coordinator_host, start_time, "
          + "end_time, fail_count, token_ranges";
  String SQL_REPAIR_SEGMENT_ALL_FIELDS = "repair_segment.id, " + SQL_REPAIR_SEGMENT_ALL_FIELDS_NO_ID;
  String SQL_INSERT_REPAIR_SEGMENT =
      "INSERT INTO repair_segment ("
          + SQL_REPAIR_SEGMENT_ALL_FIELDS_NO_ID
          + ") VALUES "
          + "(:repairUnitId, :runId, :startToken, :endToken, :state, :coordinatorHost, :startTime, "
          + ":endTime, :failCount, :tokenRangesTxt)";
  String SQL_UPDATE_REPAIR_SEGMENT =
      "UPDATE repair_segment SET repair_unit_id = :repairUnitId, run_id = :runId, "
          + "start_token = :startToken, end_token = :endToken, state = :state, "
          + "coordinator_host = :coordinatorHost, start_time = :startTime, end_time = :endTime, "
          + "fail_count = :failCount WHERE id = :id";
  String SQL_GET_REPAIR_SEGMENT = "SELECT " + SQL_REPAIR_SEGMENT_ALL_FIELDS + " FROM repair_segment WHERE id = :id";
  String SQL_GET_REPAIR_SEGMENTS_FOR_RUN = "SELECT " + SQL_REPAIR_SEGMENT_ALL_FIELDS
      + " FROM repair_segment WHERE run_id = :runId";
  String SQL_GET_REPAIR_SEGMENTS_FOR_RUN_WITH_STATE = "SELECT " + SQL_REPAIR_SEGMENT_ALL_FIELDS
      + " FROM repair_segment WHERE " + "run_id = :runId AND state = :state";
  String SQL_GET_RUNNING_REPAIRS_FOR_CLUSTER =
      "SELECT start_token, end_token, token_ranges, keyspace_name, column_families, repair_parallelism "
          + "FROM repair_segment "
          + "JOIN repair_run ON run_id = repair_run.id "
          + "JOIN repair_unit ON repair_run.repair_unit_id = repair_unit.id "
          + "WHERE repair_segment.state = 1 AND repair_unit.cluster_name = :clusterName";
  String SQL_GET_NEXT_FREE_REPAIR_SEGMENT =
      "SELECT "
          + SQL_REPAIR_SEGMENT_ALL_FIELDS
          + " FROM repair_segment WHERE run_id = :runId "
          + "AND state = 0 ORDER BY random() LIMIT 1";
  String SQL_GET_NEXT_FREE_REPAIR_SEGMENT_IN_NON_WRAPPING_RANGE =
      "SELECT "
          + SQL_REPAIR_SEGMENT_ALL_FIELDS
          + " FROM repair_segment WHERE "
          + "run_id = :runId AND state = 0 AND start_token < end_token AND "
          + "(start_token >= :startToken AND end_token <= :endToken) "
          + "ORDER BY random() LIMIT 1";
  String SQL_GET_NEXT_FREE_REPAIR_SEGMENT_IN_WRAPPING_RANGE =
      "SELECT "
          + SQL_REPAIR_SEGMENT_ALL_FIELDS
          + " FROM repair_segment WHERE "
          + "run_id = :runId AND state = 0 AND "
          + "((start_token < end_token AND (start_token >= :startToken OR end_token <= :endToken)) OR "
          + "(start_token >= :startToken AND end_token <= :endToken)) "
          + "ORDER BY random() LIMIT 1";
  String SQL_DELETE_REPAIR_SEGMENTS_FOR_RUN = "DELETE FROM repair_segment WHERE run_id = :runId";

  // RepairSchedule
  //
  String SQL_REPAIR_SCHEDULE_ALL_FIELDS_NO_ID =
      "repair_unit_id, state, days_between, next_activation, run_history, segment_count, "
          + "repair_parallelism, intensity, creation_time, owner, pause_time, segment_count_per_node ";
  String SQL_REPAIR_SCHEDULE_ALL_FIELDS = "repair_schedule.id, " + SQL_REPAIR_SCHEDULE_ALL_FIELDS_NO_ID;
  String SQL_INSERT_REPAIR_SCHEDULE =
      "INSERT INTO repair_schedule ("
          + SQL_REPAIR_SCHEDULE_ALL_FIELDS_NO_ID
          + ") VALUES "
          + "(:repairUnitId, :state, :daysBetween, :nextActivation, :runHistorySql, :segmentCount, "
          + ":repairParallelism, :intensity, :creationTime, :owner, :pauseTime, :segmentCountPerNode)";
  String SQL_UPDATE_REPAIR_SCHEDULE =
      "UPDATE repair_schedule SET repair_unit_id = :repairUnitId, state = :state, "
          + "days_between = :daysBetween, next_activation = :nextActivation, "
          + "run_history = :runHistorySql, segment_count = :segmentCount, "
          + "segment_count_per_node = :segmentCountPerNode, "
          + "repair_parallelism = :repairParallelism, creation_time = :creationTime, owner = :owner, "
          + "pause_time = :pauseTime WHERE id = :id";
  String SQL_GET_REPAIR_SCHEDULE = "SELECT " + SQL_REPAIR_SCHEDULE_ALL_FIELDS + " FROM repair_schedule WHERE id = :id";
  String SQL_GET_REPAIR_SCHEDULES_FOR_CLUSTER = "SELECT "
      + SQL_REPAIR_SCHEDULE_ALL_FIELDS
      + " FROM repair_schedule, repair_unit "
      + "WHERE repair_schedule.repair_unit_id = repair_unit.id AND cluster_name = :clusterName";
  String SQL_GET_REPAIR_SCHEDULES_FOR_KEYSPACE = "SELECT "
      + SQL_REPAIR_SCHEDULE_ALL_FIELDS
      + " FROM repair_schedule, repair_unit "
      + "WHERE repair_schedule.repair_unit_id = repair_unit.id AND keyspace_name = :keyspaceName";
  String SQL_GET_REPAIR_SCHEDULES_FOR_CLUSTER_AND_KEYSPACE = "SELECT "
      + SQL_REPAIR_SCHEDULE_ALL_FIELDS
      + " FROM repair_schedule, repair_unit "
      + "WHERE repair_schedule.repair_unit_id = repair_unit.id AND cluster_name = :clusterName "
      + "AND keyspace_name = :keyspaceName";

  String SQL_GET_ALL_REPAIR_SCHEDULES = "SELECT " + SQL_REPAIR_SCHEDULE_ALL_FIELDS + " FROM repair_schedule";
  String SQL_DELETE_REPAIR_SCHEDULE = "DELETE FROM repair_schedule WHERE id = :id";

  // Utility methods
  //
  String SQL_GET_REPAIR_RUN_IDS_FOR_CLUSTER = "SELECT id FROM repair_run WHERE cluster_name = :clusterName";
  String SQL_SEGMENT_AMOUNT_FOR_REPAIR_RUN = "SELECT count(*) FROM repair_segment WHERE run_id = :runId";
  String SQL_SEGMENT_AMOUNT_FOR_REPAIR_RUN_WITH_STATE
      = "SELECT count(*) FROM repair_segment WHERE run_id = :runId AND state = :state";

  // View-specific queries
  //
  String SQL_CLUSTER_RUN_OVERVIEW =
      "SELECT repair_run.id, repair_unit.cluster_name, keyspace_name, column_families, "
          + "nodes, datacenters, blacklisted_tables, "
          + "(SELECT COUNT(case when state = 2 then 1 else null end) "
          + "FROM repair_segment "
          + "WHERE run_id = repair_run.id) AS segments_repaired, "
          + "(SELECT COUNT(*) FROM repair_segment WHERE run_id = repair_run.id) AS segments_total, "
          + "repair_run.state, repair_run.start_time, "
          + "repair_run.end_time, cause, owner, last_event, "
          + "creation_time, pause_time, intensity, repair_parallelism, incremental_repair "
          + "FROM repair_run "
          + "JOIN repair_unit ON repair_unit_id = repair_unit.id "
          + "WHERE repair_unit.cluster_name = :clusterName "
          + "ORDER BY COALESCE(end_time, start_time) DESC, start_time DESC "
          + "LIMIT :limit";

  String SQL_CLUSTER_SCHEDULE_OVERVIEW =
      "SELECT repair_schedule.id, owner, cluster_name, keyspace_name, column_families, state, "
          + "creation_time, next_activation, pause_time, intensity, segment_count, "
          + "repair_parallelism, days_between, incremental_repair, nodes, "
          + "datacenters, blacklisted_tables, segment_count_per_node "
          + "FROM repair_schedule "
          + "JOIN repair_unit ON repair_unit_id = repair_unit.id "
          + "WHERE cluster_name = :clusterName";

  String SQL_SAVE_SNAPSHOT =
      "INSERT INTO snapshot (cluster, snapshot_name, owner, cause, creation_time)"
          + " VALUES "
          + "(:clusterName, :name, :owner, :cause, :creationDate)";

  String SQL_DELETE_SNAPSHOT =
      "DELETE FROM snapshot WHERE cluster = :clusterName AND snapshot_name = :snapshotName";

  String SQL_GET_SNAPSHOT =
      "SELECT cluster, snapshot_name, owner, cause, creation_time "
          + " FROM snapshot WHERE cluster = :clusterName AND snapshot_name = :snapshotName";


  @SqlQuery("SELECT CURRENT_TIMESTAMP")
  String getCurrentDate();

  @SqlQuery(SQL_GET_CLUSTER)
  @Mapper(ClusterMapper.class)
  Cluster getCluster(
      @Bind("name") String clusterName);

  @SqlQuery(SQL_GET_ALL_CLUSTERS)
  @Mapper(ClusterMapper.class)
  Collection<Cluster> getClusters();

  @SqlUpdate(SQL_INSERT_CLUSTER)
  int insertCluster(
      @BindBean Cluster newCluster);

  @SqlUpdate(SQL_UPDATE_CLUSTER)
  int updateCluster(
      @BindBean Cluster newCluster);

  @SqlUpdate(SQL_DELETE_CLUSTER)
  int deleteCluster(
      @Bind("name") String clusterName);

  @SqlQuery(SQL_GET_REPAIR_RUN)
  @Mapper(RepairRunMapper.class)
  RepairRun getRepairRun(
      @Bind("id") long repairRunId);

  @SqlQuery(SQL_GET_REPAIR_RUNS_FOR_CLUSTER)
  @Mapper(RepairRunMapper.class)
  Collection<RepairRun> getRepairRunsForCluster(
      @Bind("clusterName") String clusterName);

  @SqlQuery(SQL_GET_REPAIR_RUNS_WITH_STATE)
  @Mapper(RepairRunMapper.class)
  Collection<RepairRun> getRepairRunsWithState(
      @Bind("state") RepairRun.RunState state);

  @SqlQuery(SQL_GET_REPAIR_RUNS_FOR_UNIT)
  @Mapper(RepairRunMapper.class)
  Collection<RepairRun> getRepairRunsForUnit(
      @Bind("unitId") long unitId);

  @SqlUpdate(SQL_INSERT_REPAIR_RUN)
  @GetGeneratedKeys
  long insertRepairRun(
      @BindBean RepairRun newRepairRun);

  @SqlUpdate(SQL_UPDATE_REPAIR_RUN)
  int updateRepairRun(
      @BindBean RepairRun newRepairRun);

  @SqlUpdate(SQL_DELETE_REPAIR_RUN)
  int deleteRepairRun(
      @Bind("id") long repairRunId);

  @SqlQuery(SQL_GET_REPAIR_UNIT)
  @Mapper(RepairUnitMapper.class)
  RepairUnit getRepairUnit(
      @Bind("id") long repairUnitId);

  @SqlQuery(SQL_GET_REPAIR_UNIT_BY_CLUSTER_AND_TABLES)
  @Mapper(RepairUnitMapper.class)
  RepairUnit getRepairUnitByClusterAndTables(
      @Bind("clusterName") String clusterName,
      @Bind("keyspaceName") String keyspaceName,
      @Bind("columnFamilies") Collection<String> columnFamilies,
      @Bind("nodes") Collection<String> nodes,
      @Bind("datacenters") Collection<String> datacenters,
      @Bind("blacklisted_tables") Collection<String> blacklistedTables,
      @Bind("repairThreadCount") int repairThreadCount);

  @SqlUpdate(SQL_INSERT_REPAIR_UNIT)
  @GetGeneratedKeys
  long insertRepairUnit(
      @BindBean RepairUnit newRepairUnit);

  @SqlUpdate(SQL_DELETE_REPAIR_UNIT)
  int deleteRepairUnit(
      @Bind("id") long repairUnitId);

  @SqlBatch(SQL_INSERT_REPAIR_SEGMENT)
  @BatchChunkSize(500)
  void insertRepairSegments(@BindBean Iterator<PostgresRepairSegment> iterator);

  @SqlUpdate(SQL_UPDATE_REPAIR_SEGMENT)
  int updateRepairSegment(
      @BindBean RepairSegment newRepairSegment);

  @SqlQuery(SQL_GET_REPAIR_SEGMENT)
  @Mapper(RepairSegmentMapper.class)
  RepairSegment getRepairSegment(
      @Bind("id") long repairSegmentId);

  @SqlQuery(SQL_GET_REPAIR_SEGMENTS_FOR_RUN)
  @Mapper(RepairSegmentMapper.class)
  Collection<RepairSegment> getRepairSegmentsForRun(
      @Bind("runId") long runId);

  @SqlQuery(SQL_GET_REPAIR_SEGMENTS_FOR_RUN_WITH_STATE)
  @Mapper(RepairSegmentMapper.class)
  Collection<RepairSegment> getRepairSegmentsForRunWithState(
      @Bind("runId") long runId,
      @Bind("state") RepairSegment.State state);

  @SqlQuery(SQL_GET_RUNNING_REPAIRS_FOR_CLUSTER)
  @Mapper(RepairParametersMapper.class)
  Collection<RepairParameters> getRunningRepairsForCluster(
      @Bind("clusterName") String clusterName);

  @SqlQuery(SQL_GET_NEXT_FREE_REPAIR_SEGMENT)
  @Mapper(RepairSegmentMapper.class)
  RepairSegment getNextFreeRepairSegment(
      @Bind("runId") long runId);

  @SqlQuery(SQL_GET_NEXT_FREE_REPAIR_SEGMENT_IN_NON_WRAPPING_RANGE)
  @Mapper(RepairSegmentMapper.class)
  RepairSegment getNextFreeRepairSegmentInNonWrappingRange(
      @Bind("runId") long runId,
      @Bind("startToken") BigInteger startToken,
      @Bind("endToken") BigInteger endToken);

  @SqlQuery(SQL_GET_NEXT_FREE_REPAIR_SEGMENT_IN_WRAPPING_RANGE)
  @Mapper(RepairSegmentMapper.class)
  RepairSegment getNextFreeRepairSegmentInWrappingRange(
      @Bind("runId") long runId,
      @Bind("startToken") BigInteger startToken,
      @Bind("endToken") BigInteger endToken);

  @SqlUpdate(SQL_DELETE_REPAIR_SEGMENTS_FOR_RUN)
  int deleteRepairSegmentsForRun(
      @Bind("runId") long repairRunId);

  @SqlQuery(SQL_GET_REPAIR_SCHEDULE)
  @Mapper(RepairScheduleMapper.class)
  RepairSchedule getRepairSchedule(
      @Bind("id") long repairScheduleId);

  @SqlUpdate(SQL_INSERT_REPAIR_SCHEDULE)
  @GetGeneratedKeys
  long insertRepairSchedule(
      @BindBean RepairSchedule newRepairSchedule);

  @SqlUpdate(SQL_UPDATE_REPAIR_SCHEDULE)
  int updateRepairSchedule(
      @BindBean RepairSchedule newRepairSchedule);

  @SqlQuery(SQL_GET_REPAIR_SCHEDULES_FOR_CLUSTER)
  @Mapper(RepairScheduleMapper.class)
  Collection<RepairSchedule> getRepairSchedulesForCluster(
      @Bind("clusterName") String clusterName);

  @SqlQuery(SQL_GET_REPAIR_SCHEDULES_FOR_KEYSPACE)
  @Mapper(RepairScheduleMapper.class)
  Collection<RepairSchedule> getRepairSchedulesForKeyspace(
      @Bind("keyspaceName") String keyspaceName);

  @SqlQuery(SQL_GET_REPAIR_SCHEDULES_FOR_CLUSTER_AND_KEYSPACE)
  @Mapper(RepairScheduleMapper.class)
  Collection<RepairSchedule> getRepairSchedulesForClusterAndKeySpace(
      @Bind("clusterName") String clusterName,
      @Bind("keyspaceName") String keyspaceName);

  @SqlQuery(SQL_GET_ALL_REPAIR_SCHEDULES)
  @Mapper(RepairScheduleMapper.class)
  Collection<RepairSchedule> getAllRepairSchedules();

  @SqlQuery(SQL_GET_REPAIR_RUN_IDS_FOR_CLUSTER)
  Collection<Long> getRepairRunIdsForCluster(
      @Bind("clusterName") String clusterName);

  @SqlUpdate(SQL_DELETE_REPAIR_SCHEDULE)
  int deleteRepairSchedule(
      @Bind("id") long repairScheduleId);

  @SqlQuery(SQL_SEGMENT_AMOUNT_FOR_REPAIR_RUN)
  int getSegmentAmountForRepairRun(
      @Bind("runId") long runId);

  @SqlQuery(SQL_SEGMENT_AMOUNT_FOR_REPAIR_RUN_WITH_STATE)
  int getSegmentAmountForRepairRunWithState(
      @Bind("runId") long runId,
      @Bind("state") RepairSegment.State state);

  @SqlQuery(SQL_CLUSTER_RUN_OVERVIEW)
  @Mapper(RepairRunStatusMapper.class)
  List<RepairRunStatus> getClusterRunOverview(
      @Bind("clusterName") String clusterName,
      @Bind("limit") int limit);

  @SqlQuery(SQL_CLUSTER_SCHEDULE_OVERVIEW)
  @Mapper(RepairScheduleStatusMapper.class)
  Collection<RepairScheduleStatus> getClusterScheduleOverview(
      @Bind("clusterName") String clusterName);

  @SqlQuery(SQL_GET_SNAPSHOT)
  @Mapper(SnapshotMapper.class)
  Snapshot getSnapshot(
      @Bind("clusterName") String clusterName, @Bind("snapshotName") String snapshotName);

  @SqlUpdate(SQL_DELETE_SNAPSHOT)
  int deleteSnapshot(
      @Bind("clusterName") String clusterName, @Bind("snapshotName") String snapshotName);

  @SqlUpdate(SQL_SAVE_SNAPSHOT)
  int saveSnapshot(@BindBean Snapshot snapshot);

}
