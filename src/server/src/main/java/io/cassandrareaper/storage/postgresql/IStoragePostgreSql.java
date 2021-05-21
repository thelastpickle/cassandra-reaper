/*
 * Copyright 2014-2017 Spotify AB
 * Copyright 2016-2019 The Last Pickle Ltd
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

package io.cassandrareaper.storage.postgresql;

import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.DiagEventSubscription;
import io.cassandrareaper.core.GenericMetric;
import io.cassandrareaper.core.NodeMetrics;
import io.cassandrareaper.core.PercentRepairedMetric;
import io.cassandrareaper.core.RepairRun;
import io.cassandrareaper.core.RepairSchedule;
import io.cassandrareaper.core.RepairSegment;
import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.core.Snapshot;
import io.cassandrareaper.resources.view.RepairRunStatus;
import io.cassandrareaper.resources.view.RepairScheduleStatus;
import io.cassandrareaper.service.RepairParameters;

import java.math.BigInteger;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

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
  String SQL_CLUSTER_ALL_FIELDS = "name, partitioner, seed_hosts, properties, state, last_contact";
  String SQL_GET_ALL_CLUSTERS = "SELECT " + SQL_CLUSTER_ALL_FIELDS + " FROM cluster";
  String SQL_GET_CLUSTER = "SELECT " + SQL_CLUSTER_ALL_FIELDS + " FROM cluster WHERE name = :name";
  String SQL_INSERT_CLUSTER
      = "INSERT INTO cluster ("
          + SQL_CLUSTER_ALL_FIELDS
          + ") VALUES (:name, :partitioner, :seedHosts, :properties, :state, :lastContact)";
  String SQL_UPDATE_CLUSTER = "UPDATE cluster SET partitioner = :partitioner, "
      + "seed_hosts = :seedHosts, state = :state, last_contact = :lastContact WHERE name = :name";
  String SQL_DELETE_CLUSTER = "DELETE FROM cluster WHERE name = :name";

  // RepairRun
  //
  String SQL_REPAIR_RUN_ALL_FIELDS_NO_ID = "cluster_name, repair_unit_id, cause, owner, state, creation_time, "
      + "start_time, end_time, pause_time, intensity, last_event, "
      + "segment_count, repair_parallelism, tables";
  String SQL_REPAIR_RUN_ALL_FIELDS = "repair_run.id, " + SQL_REPAIR_RUN_ALL_FIELDS_NO_ID;
  String SQL_INSERT_REPAIR_RUN = "INSERT INTO repair_run ("
      + SQL_REPAIR_RUN_ALL_FIELDS_NO_ID
      + ") VALUES "
      + "(:clusterName, :repairUnitId, :cause, :owner, :runState, :creationTime, "
      + ":startTime, :endTime, :pauseTime, :intensity, :lastEvent, :segmentCount, "
      + ":repairParallelism, :tables)";
  String SQL_UPDATE_REPAIR_RUN = "UPDATE repair_run SET cause = :cause, owner = :owner, state = :runState, "
      + "start_time = :startTime, end_time = :endTime, pause_time = :pauseTime, "
      + "intensity = :intensity, last_event = :lastEvent, segment_count = :segmentCount, "
      + "repair_parallelism = :repairParallelism, tables = :tables WHERE id = :id";
  String SQL_GET_REPAIR_RUN = "SELECT " + SQL_REPAIR_RUN_ALL_FIELDS + " FROM repair_run WHERE id = :id";
  String SQL_GET_REPAIR_RUNS_FOR_CLUSTER = "SELECT " + SQL_REPAIR_RUN_ALL_FIELDS
      + " FROM repair_run WHERE cluster_name = :clusterName ORDER BY id desc LIMIT :limit";
  String SQL_GET_REPAIR_RUNS_WITH_STATE = "SELECT " + SQL_REPAIR_RUN_ALL_FIELDS
      + " FROM repair_run WHERE state = :state";
  String SQL_GET_REPAIR_RUNS_FOR_UNIT = "SELECT " + SQL_REPAIR_RUN_ALL_FIELDS
      + " FROM repair_run WHERE repair_unit_id = :unitId";
  String SQL_DELETE_REPAIR_RUN = "DELETE FROM repair_run WHERE id = :id";

  // RepairUni
  //
  String SQL_REPAIR_UNIT_ALL_FIELDS_NO_ID = "cluster_name, keyspace_name, column_families, "
          + "incremental_repair, nodes, datacenters, blacklisted_tables, repair_thread_count";
  String SQL_REPAIR_UNIT_ALL_FIELDS = "repair_unit.id, " + SQL_REPAIR_UNIT_ALL_FIELDS_NO_ID;
  String SQL_INSERT_REPAIR_UNIT = "INSERT INTO repair_unit ("
          + SQL_REPAIR_UNIT_ALL_FIELDS_NO_ID
          + ") VALUES "
          + "(:clusterName, :keyspaceName, :columnFamilies, "
          + ":incrementalRepair, :nodes, :datacenters, :blacklistedTables, :repairThreadCount)";
  String SQL_GET_REPAIR_UNIT = "SELECT " + SQL_REPAIR_UNIT_ALL_FIELDS + " FROM repair_unit WHERE id = :id";

  String SQL_GET_REPAIR_UNIT_BY_CLUSTER_AND_TABLES = "SELECT "
          + SQL_REPAIR_UNIT_ALL_FIELDS
          + " FROM repair_unit "
          + "WHERE cluster_name = :clusterName AND keyspace_name = :keyspaceName "
          + "AND column_families = :columnFamilies AND incremental_repair = :incrementalRepair "
          + "AND nodes = :nodes AND datacenters = :datacenters "
          + "AND blackListed_tables = :blacklisted_tables AND repair_thread_count = :repairThreadCount";

  String SQL_DELETE_REPAIR_UNIT = "DELETE FROM repair_unit WHERE id = :id";
  String SQL_DELETE_REPAIR_UNITS = "DELETE FROM repair_unit WHERE cluster_name = :clusterName";

  // RepairSegmen
  //
  String SQL_REPAIR_SEGMENT_ALL_FIELDS_NO_ID
      = "repair_unit_id, run_id, start_token, end_token, state, coordinator_host, start_time, "
          + "end_time, fail_count, token_ranges, replicas";
  String SQL_REPAIR_SEGMENT_ALL_FIELDS = "repair_segment.id, " + SQL_REPAIR_SEGMENT_ALL_FIELDS_NO_ID;
  String SQL_INSERT_REPAIR_SEGMENT = "INSERT INTO repair_segment ("
          + SQL_REPAIR_SEGMENT_ALL_FIELDS_NO_ID
          + ") VALUES "
          + "(:repairUnitId, :runId, :startToken, :endToken, :state, :coordinatorHost, :startTime, "
          + ":endTime, :failCount, :tokenRangesTxt, :replicasTxt)";
  String SQL_UPDATE_REPAIR_SEGMENT = "UPDATE repair_segment SET repair_unit_id = :repairUnitId, run_id = :runId, "
          + "start_token = :startToken, end_token = :endToken, state = :state, "
          + "coordinator_host = :coordinatorHost, start_time = :startTime, end_time = :endTime, "
          + "fail_count = :failCount WHERE id = :id";
  String SQL_GET_REPAIR_SEGMENT = "SELECT " + SQL_REPAIR_SEGMENT_ALL_FIELDS + " FROM repair_segment WHERE id = :id";
  String SQL_GET_REPAIR_SEGMENTS_FOR_RUN = "SELECT " + SQL_REPAIR_SEGMENT_ALL_FIELDS
      + " FROM repair_segment WHERE run_id = :runId";
  String SQL_GET_REPAIR_SEGMENTS_FOR_RUN_WITH_STATE = "SELECT " + SQL_REPAIR_SEGMENT_ALL_FIELDS
      + " FROM repair_segment WHERE " + "run_id = :runId AND state = :state";
  String SQL_GET_RUNNING_REPAIRS_FOR_CLUSTER
      = "SELECT start_token, end_token, token_ranges, keyspace_name, column_families, "
          + "repair_parallelism, tables, replicas "
          + "FROM repair_segment "
          + "JOIN repair_run ON run_id = repair_run.id "
          + "JOIN repair_unit ON repair_run.repair_unit_id = repair_unit.id "
          + "WHERE repair_segment.state = 1 AND repair_unit.cluster_name = :clusterName";
  String SQL_GET_NEXT_FREE_REPAIR_SEGMENT = "SELECT "
          + SQL_REPAIR_SEGMENT_ALL_FIELDS
          + " FROM repair_segment WHERE run_id = :runId "
          + "AND state = 0 ORDER BY random()";
  String SQL_GET_NEXT_FREE_REPAIR_SEGMENT_IN_NON_WRAPPING_RANGE = "SELECT "
          + SQL_REPAIR_SEGMENT_ALL_FIELDS
          + " FROM repair_segment WHERE "
          + "run_id = :runId AND state = 0 AND start_token < end_token AND "
          + "(start_token >= :startToken AND end_token <= :endToken) "
          + "ORDER BY random()";
  String SQL_GET_NEXT_FREE_REPAIR_SEGMENT_IN_WRAPPING_RANGE = "SELECT "
          + SQL_REPAIR_SEGMENT_ALL_FIELDS
          + " FROM repair_segment WHERE "
          + "run_id = :runId AND state = 0 AND "
          + "((start_token < end_token AND (start_token >= :startToken OR end_token <= :endToken)) OR "
          + "(start_token >= :startToken AND end_token <= :endToken)) "
          + "ORDER BY random()";
  String SQL_DELETE_REPAIR_SEGMENTS_FOR_RUN = "DELETE FROM repair_segment WHERE run_id = :runId";

  // RepairSchedule
  //
  String SQL_REPAIR_SCHEDULE_ALL_FIELDS_NO_ID
      = "repair_unit_id, state, days_between, next_activation, run_history, segment_count, "
          + "repair_parallelism, intensity, creation_time, owner, pause_time, segment_count_per_node ";
  String SQL_REPAIR_SCHEDULE_ALL_FIELDS = "repair_schedule.id, " + SQL_REPAIR_SCHEDULE_ALL_FIELDS_NO_ID;
  String SQL_INSERT_REPAIR_SCHEDULE
      = "INSERT INTO repair_schedule ("
          + SQL_REPAIR_SCHEDULE_ALL_FIELDS_NO_ID
          + ") VALUES "
          + "(:repairUnitId, :state, :daysBetween, :nextActivation, :runHistorySql, :segmentCount, "
          + ":repairParallelism, :intensity, :creationTime, :owner, :pauseTime, :segmentCountPerNode)";
  String SQL_UPDATE_REPAIR_SCHEDULE
      = "UPDATE repair_schedule SET repair_unit_id = :repairUnitId, state = :state, "
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
  String SQL_CLUSTER_RUN_OVERVIEW = "SELECT repair_run.id, repair_unit.cluster_name, keyspace_name, column_families, "
          + "nodes, datacenters, blacklisted_tables, repair_run.repair_unit_id, "
          + "(SELECT COUNT(case when state = 2 then 1 else null end) "
          + "FROM repair_segment "
          + "WHERE run_id = repair_run.id) AS segments_repaired, "
          + "(SELECT COUNT(*) FROM repair_segment WHERE run_id = repair_run.id) AS segments_total, "
          + "repair_run.state, repair_run.start_time, "
          + "repair_run.end_time, cause, owner, last_event, creation_time, "
          + "pause_time, intensity, repair_parallelism, tables, incremental_repair, repair_thread_count "
          + "FROM repair_run "
          + "JOIN repair_unit ON repair_unit_id = repair_unit.id "
          + "WHERE repair_unit.cluster_name = :clusterName "
          + "ORDER BY COALESCE(end_time, start_time) DESC, start_time DESC "
          + "LIMIT :limit";

  String SQL_CLUSTER_SCHEDULE_OVERVIEW
      = "SELECT repair_schedule.id, owner, cluster_name, keyspace_name, column_families, state, "
          + "creation_time, next_activation, pause_time, intensity, segment_count, "
          + "repair_parallelism, days_between, incremental_repair, nodes, "
          + "datacenters, blacklisted_tables, segment_count_per_node, repair_thread_count, repair_unit_id "
          + "FROM repair_schedule "
          + "JOIN repair_unit ON repair_unit_id = repair_unit.id "
          + "WHERE cluster_name = :clusterName";

  String SQL_SAVE_SNAPSHOT = "INSERT INTO snapshot (cluster, snapshot_name, owner, cause, creation_time)"
          + " VALUES "
          + "(:clusterName, :name, :owner, :cause, :creationDate)";

  String SQL_DELETE_SNAPSHOT = "DELETE FROM snapshot WHERE cluster = :clusterName AND snapshot_name = :snapshotName";

  String SQL_GET_SNAPSHOT = "SELECT cluster, snapshot_name, owner, cause, creation_time "
          + " FROM snapshot WHERE cluster = :clusterName AND snapshot_name = :snapshotName";

  // Diagnostic Events
  //
  String SQL_EVENT_SUBSCRIPTION_ALL_FIELDS = " id, cluster, description, include_nodes, events, "
          + "export_sse, export_file_logger, export_http_endpoint ";

  String SQL_EVENT_SUBSCRIPTION_ALL_BUT_ID_FIELDS = "cluster, description, include_nodes, events, "
          + "export_sse, export_file_logger, export_http_endpoint ";


  String SQL_GET_EVENT_SUBSCRIPTIONS_BY_CLUSTER
      = "SELECT " + SQL_EVENT_SUBSCRIPTION_ALL_FIELDS + " FROM diag_event_subscription WHERE cluster = :clusterName";

  String SQL_GET_EVENT_SUBSCRIPTION_BY_ID
      = "SELECT " + SQL_EVENT_SUBSCRIPTION_ALL_FIELDS + " FROM diag_event_subscription WHERE id = :id";

  String SQL_GET_EVENT_SUBSCRIPTIONS = "SELECT * FROM diag_event_subscription";

  String SQL_INSERT_EVENT_SUBSCRIPTION = "INSERT INTO diag_event_subscription ("
          + SQL_EVENT_SUBSCRIPTION_ALL_BUT_ID_FIELDS
          + ") VALUES ("
          + ":cluster, :description, :nodes, :events, :exportSse, :exportFileLogger, :exportHttpEndpoint)";

  String SQL_DELETE_EVENT_SUBSCRIPTION_BY_ID
      = "DELETE FROM diag_event_subscription WHERE id = :id";

  // leader election
  //
  String SQL_INSERT_LEAD = "INSERT INTO leader (leader_id, reaper_instance_id, reaper_instance_host, last_heartbeat)"
          + " VALUES "
          + "(:leaderId, :reaperInstanceId, :reaperInstanceHost, now())";

  String SQL_UPDATE_LEAD = "UPDATE leader "
          + " SET "
          + "reaper_instance_id = :reaperInstanceId, reaper_instance_host = :reaperInstanceHost, last_heartbeat = now()"
          + " WHERE "
          + "leader_id = :leaderId AND last_heartbeat < :expirationTime";

  String SQL_RENEW_LEAD = "UPDATE leader "
      + " SET "
      + "reaper_instance_id = :reaperInstanceId, reaper_instance_host = :reaperInstanceHost, last_heartbeat = now()"
      + " WHERE "
      + "leader_id = :leaderId AND reaper_instance_id = :reaperInstanceId";

  String SQL_SELECT_ACTIVE_LEADERS = "SELECT leader_id from leader"
      + " WHERE "
      + " last_heartbeat >= :expirationTime";

  String SQL_RELEASE_LEAD = "DELETE FROM leader"
      + " WHERE "
      + "leader_id = :leaderId AND reaper_instance_id = :reaperInstanceId";

  String SQL_FORCE_RELEASE_LEAD = "DELETE FROM leader WHERE leader_id = :leaderId";

  String SQL_INSERT_HEARTBEAT = "INSERT INTO running_reapers(reaper_instance_id, reaper_instance_host, last_heartbeat)"
      + " VALUES "
      + "(:reaperInstanceId, :reaperInstanceHost, now())";

  String SQL_UPDATE_HEARTBEAT = "UPDATE running_reapers"
      + " SET "
      + "reaper_instance_id = :reaperInstanceId, reaper_instance_host = :reaperInstanceHost, last_heartbeat = now()"
      + " WHERE "
      + "reaper_instance_id = :reaperInstanceId";

  String SQL_DELETE_OLD_REAPERS = "DELETE FROM running_reapers"
      + " WHERE "
      + "last_heartbeat < :expirationTime";

  String SQL_COUNT_RUNNING_REAPERS = "SELECT COUNT(*) FROM running_reapers"
      + " WHERE "
      + "last_heartbeat >= :expirationTime";

  String SQL_STORE_NODE_METRICS =  "INSERT INTO node_metrics_v1 (run_id,ts,node,datacenter,"
      + "cluster,requested,pending_compactions,has_repair_running,active_anticompactions)"
      + " VALUES "
      + "(:runId, now(), :node, :datacenter, :cluster, :requested, :pendingCompactions, :hasRepairRunning, "
      + ":activeAntiCompactions)";

  String SQL_GET_NODE_METRICS = "SELECT * FROM node_metrics_v1"
      + " WHERE "
      + "run_id = :runId AND ts > :expirationTime";

  String SQL_GET_NODE_METRICS_BY_NODE = "SELECT * FROM node_metrics_v1"
      + " WHERE"
      + " run_id = :runId AND ts > :expirationTime AND node = :node"
      + " ORDER BY ts DESC LIMIT 1";

  String SQL_DELETE_NODE_METRICS_BY_NODE = "DELETE FROM node_metrics_v1"
      + " WHERE "
      + " run_id = :runId AND node = :node";

  String SQL_PURGE_OLD_NODE_METRICS = "DELETE FROM node_metrics_v1"
      + " WHERE"
      + " ts < :expirationTime";

  // sidecar-mode metrics
  //
  String SQL_UPDATE_SOURCE_NODE_TIMESTAMP = "UPDATE node_metrics_v2_source_nodes"
      + " SET last_updated = :timestamp"
      + " WHERE cluster = :cluster AND host = :host";

  String SQL_ADD_SOURCE_NODE = "INSERT INTO node_metrics_v2_source_nodes (cluster, host, last_updated)"
          + " VALUES (:cluster, :host, :timestamp)";

  String SQL_ADD_METRIC_TYPE = "INSERT INTO node_metrics_v2_metric_types"
          + " (metric_domain, metric_type, metric_scope, metric_name, metric_attribute)"
          + " VALUES"
          + " (:metricDomain, :metricType, :metricScope, :metricName, :metricAttribute)";

  String SQL_GET_SOURCE_NODE_ID = "SELECT source_node_id FROM node_metrics_v2_source_nodes"
          + " WHERE cluster = :cluster AND host = :host";

  String SQL_GET_METRIC_TYPE_ID = "SELECT metric_type_id FROM node_metrics_v2_metric_types"
          + " WHERE"
          + " metric_domain = :metricDomain AND metric_type = :metricType AND metric_scope = :metricScope"
          + " AND metric_name = :metricName AND metric_attribute = :metricAttribute";

  String SQL_INSERT_METRIC = "INSERT INTO node_metrics_v2 (metric_type_id, source_node_id, ts, value)"
          + " VALUES ("
          + " (" + SQL_GET_METRIC_TYPE_ID + "),"
          + " (" + SQL_GET_SOURCE_NODE_ID + "),"
          + " :timestamp,"
          + " :value)";

  String SQL_GET_METRICS_FOR_HOST = "SELECT cluster, host, metric_domain, metric_type, metric_scope, metric_name,"
          + " metric_attribute, ts, value"
          + " FROM node_metrics_v2"
          + " NATURAL JOIN node_metrics_v2_metric_types"
          + " NATURAL JOIN node_metrics_v2_source_nodes"
          + " WHERE"
          + " cluster = :cluster AND host = :host AND metric_domain = :metricDomain AND metric_type = :metricType"
          + " AND ts >= :since";

  String SQL_GET_METRICS_FOR_CLUSTER = "SELECT cluster, host, metric_domain, metric_type, metric_scope, metric_name,"
          + " metric_attribute, ts, value"
          + " FROM node_metrics_v2"
          + " NATURAL JOIN node_metrics_v2_metric_types"
          + " NATURAL JOIN node_metrics_v2_source_nodes"
          + " WHERE"
          + " cluster = :cluster AND metric_domain = :metricDomain AND metric_type = :metricType"
          + " AND ts >= :since";

  String SQL_PURGE_OLD_METRICS = "DELETE FROM node_metrics_v2 WHERE ts < :expirationTime";

  String SQL_PURGE_OLD_SOURCE_NODES = "DELETE FROM node_metrics_v2_source_nodes WHERE last_updated < :expirationTime";

  String SQL_INSERT_OPERATIONS = "INSERT INTO node_operations (cluster, type, host, data, ts)"
      + " VALUES (:cluster, :type, :host, :data, now())";

  String SQL_LIST_OPERATIONS = "SELECT data FROM node_operations"
          + " WHERE"
          + " cluster = :cluster AND type = :operationType AND host = :host"
          + " ORDER BY ts DESC LIMIT 1";

  String SQL_PURGE_OLD_NODE_OPERATIONS = "DELETE from node_operations WHERE ts < :expirationTime";

  String SQL_INSERT_NODE_LOCK = "INSERT INTO running_repairs (repair_id, node, last_heartbeat, reaper_instance_host,"
      + "  reaper_instance_id, segment_id) "
      + "values(:repairId, :node, now(), :reaperInstanceHost, :reaperInstanceId, :segmentId)";

  String SQL_UPDATE_NODE_LOCK = "UPDATE running_repairs "
      + " SET "
      + "reaper_instance_id = :reaperInstanceId, reaper_instance_host = :reaperInstanceHost, last_heartbeat = now(),"
      + "segment_id = :segmentId"
      + " WHERE "
      + "repair_id = :repairId AND node = :node AND last_heartbeat < :expirationTime";

  String SQL_RENEW_NODE_LOCK = "UPDATE running_repairs "
      + " SET "
      + "reaper_instance_id = :reaperInstanceId, reaper_instance_host = :reaperInstanceHost, last_heartbeat = now()"
      + " WHERE "
      + "repair_id = :repairId AND node = :node AND reaper_instance_id = :reaperInstanceId"
      + " AND segment_id = :segmentId";

  String SQL_RELEASE_NODE_LOCK = "DELETE FROM running_repairs"
      + " WHERE "
      + "repair_id = :repairId AND node = :node AND reaper_instance_id = :reaperInstanceId"
      + " AND segment_id = :segmentId";

  String SQL_FORCE_RELEASE_NODE_LOCK = "DELETE FROM running_repairs WHERE repair_id = :repairId and node = :node";

  String SQL_SELECT_LOCKED_SEGMENTS = "SELECT segment_id FROM running_repairs "
      + " WHERE "
      + " last_heartbeat >= :expirationTime";

  String SQL_SELECT_LOCKED_NODES = "SELECT node FROM running_repairs "
      + " WHERE "
      + " last_heartbeat >= :expirationTime";

  String SQL_SELECT_PERCENT_REPAIRED = "SELECT cluster, repair_schedule_id, node, keyspace_name,"
      + " table_name, percent_repaired, ts"
      + " FROM percent_repaired_by_schedule"
      + " WHERE cluster = :cluster AND repair_schedule_id = :repairScheduleId"
      + " AND ts >= :since "
      + " ORDER BY ts DESC";

  String SQL_INSERT_PERCENT_REPAIRED = "INSERT INTO percent_repaired_by_schedule"
      + " (cluster, repair_schedule_id, node, keyspace_name, table_name, percent_repaired, ts)"
      + " values(:cluster, :repairScheduleId, :node, :keyspaceName, :tableName, :percentRepaired, current_timestamp)";

  String SQL_UPDATE_PERCENT_REPAIRED = "UPDATE percent_repaired_by_schedule"
      + " set keyspace_name = :keyspaceName, table_name = :tableName,"
      + " percent_repaired = :percentRepaired, ts = current_timestamp"
      + " WHERE cluster = :cluster AND repair_schedule_id = :repairScheduleId and node = :node";

  String SQL_PURGE_PERCENT_REPAIRED = "DELETE FROM percent_repaired_by_schedule"
      + " WHERE"
      + " ts < :expirationTime";

  static String[] parseStringArray(Object obj) {
    String[] values = null;
    if (obj instanceof String[]) {
      values = (String[]) obj;
    } else if (obj instanceof Object[]) {
      Object[] ocf = (Object[]) obj;
      values = Arrays.copyOf(ocf, ocf.length, String[].class);
    }
    return values;
  }

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
      @Bind("name") String name,
      @Bind("partitioner") String partitioner,
      @Bind("seedHosts") Set<String> seedHosts,
      @Bind("properties") String properties,
      @Bind("state") String state,
      @Bind("lastContact") Date lastContact);

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
      @Bind("clusterName") String clusterName,
      @Bind("limit") int limit);

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
      @Bind("incrementalRepair") boolean incrementalRepair,
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

  @SqlUpdate(SQL_DELETE_REPAIR_UNITS)
  int deleteRepairUnits(
      @Bind("clusterName") String clusterName);

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
  Collection<RepairSegment> getNextFreeRepairSegment(
      @Bind("runId") long runId);

  @SqlQuery(SQL_GET_NEXT_FREE_REPAIR_SEGMENT_IN_NON_WRAPPING_RANGE)
  @Mapper(RepairSegmentMapper.class)
  Collection<RepairSegment> getNextFreeRepairSegmentInNonWrappingRange(
      @Bind("runId") long runId,
      @Bind("startToken") BigInteger startToken,
      @Bind("endToken") BigInteger endToken);

  @SqlQuery(SQL_GET_NEXT_FREE_REPAIR_SEGMENT_IN_WRAPPING_RANGE)
  @Mapper(RepairSegmentMapper.class)
  Collection<RepairSegment> getNextFreeRepairSegmentInWrappingRange(
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

  @SqlQuery(SQL_GET_EVENT_SUBSCRIPTIONS_BY_CLUSTER)
  @Mapper(DiagEventSubscriptionMapper.class)
  Collection<DiagEventSubscription> getEventSubscriptions(
          @Bind("clusterName") String clusterName);

  @SqlQuery(SQL_GET_EVENT_SUBSCRIPTIONS)
  @Mapper(DiagEventSubscriptionMapper.class)
  Collection<DiagEventSubscription> getEventSubscriptions();

  @SqlQuery(SQL_GET_EVENT_SUBSCRIPTION_BY_ID)
  @Mapper(DiagEventSubscriptionMapper.class)
  DiagEventSubscription getEventSubscription(
          @Bind("id") long subscriptionId);

  @SqlUpdate(SQL_INSERT_EVENT_SUBSCRIPTION)
  @GetGeneratedKeys
  long insertDiagEventSubscription(
          @BindBean DiagEventSubscription subscription);

  @SqlUpdate(SQL_DELETE_EVENT_SUBSCRIPTION_BY_ID)
  int deleteEventSubscription(
          @Bind("id") long subscriptionId);

  @SqlUpdate(SQL_INSERT_LEAD)
  int insertLeaderEntry(
      @Bind("leaderId") UUID leaderId,
      @Bind("reaperInstanceId") UUID reaperInstanceId,
      @Bind("reaperInstanceHost") String reaperInstanceHost
  );

  @SqlUpdate(SQL_UPDATE_LEAD)
  int updateLeaderEntry(
      @Bind("leaderId") UUID leaderId,
      @Bind("reaperInstanceId") UUID reaperInstanceId,
      @Bind("reaperInstanceHost") String reaperInstanceHost,
      @Bind("expirationTime") Instant expirationTime
  );

  @SqlUpdate(SQL_RENEW_LEAD)
  int renewLead(
      @Bind("leaderId") UUID leaderId,
      @Bind("reaperInstanceId") UUID reaperInstanceId,
      @Bind("reaperInstanceHost") String reaperInstanceHost
  );

  @SqlQuery(SQL_SELECT_ACTIVE_LEADERS)
  List<Long> getLeaders(
      @Bind("expirationTime") Instant expirationTime
  );

  @SqlUpdate(SQL_RELEASE_LEAD)
  int releaseLead(
      @Bind("leaderId") UUID leaderId,
      @Bind("reaperInstanceId") UUID reaperInstanceId
  );

  @SqlUpdate(SQL_FORCE_RELEASE_LEAD)
  int forceReleaseLead(
      @Bind("leaderId") UUID leaderId
  );

  @SqlUpdate(SQL_INSERT_HEARTBEAT)
  int insertHeartbeat(
      @Bind("reaperInstanceId") UUID reaperInstanceId,
      @Bind("reaperInstanceHost") String reaperInstanceHost
  );

  @SqlUpdate(SQL_UPDATE_HEARTBEAT)
  int updateHeartbeat(
      @Bind("reaperInstanceId") UUID reaperInstanceId,
      @Bind("reaperInstanceHost") String reaperInstanceHost
  );

  @SqlUpdate(SQL_DELETE_OLD_REAPERS)
  int deleteOldReapers(
      @Bind("expirationTime") Instant expirationTime
  );

  @SqlQuery(SQL_COUNT_RUNNING_REAPERS)
  int countRunningReapers(
      @Bind("expirationTime") Instant expirationTime
  );

  @SqlUpdate(SQL_STORE_NODE_METRICS)
  int storeNodeMetrics(
      @Bind("runId") long runId,
      @Bind("node") String node,
      @Bind("cluster") String cluster,
      @Bind("datacenter") String datacenter,
      @Bind("requested") Boolean requested,
      @Bind("pendingCompactions") int pendingCompactions,
      @Bind("hasRepairRunning") Boolean hasRepairRunning,
      @Bind("activeAntiCompactions") int activeAntiCompactions
  );

  @SqlQuery(SQL_GET_NODE_METRICS)
  @Mapper(NodeMetricsMapper.class)
  Collection<NodeMetrics> getNodeMetrics(
      @Bind("runId") long runId,
      @Bind("expirationTime") Instant expirationTime
  );

  @SqlQuery(SQL_GET_NODE_METRICS_BY_NODE)
  @Mapper(NodeMetricsMapper.class)
  NodeMetrics getNodeMetricsByNode(
      @Bind("runId") long runId,
      @Bind("expirationTime") Instant expirationTime,
      @Bind("node") String node
  );

  @SqlUpdate(SQL_DELETE_NODE_METRICS_BY_NODE)
  int deleteNodeMetricsByNode(
      @Bind("runId") long runId,
      @Bind("node") String node
  );

  @SqlUpdate(SQL_PURGE_OLD_NODE_METRICS)
  int purgeOldNodeMetrics(
      @Bind("expirationTime") Instant expirationTime
  );

  @SqlUpdate(SQL_ADD_SOURCE_NODE)
  int insertMetricSourceNode(
      @Bind("cluster") String cluster,
      @Bind("host") String host,
      @Bind("timestamp") Instant timestamp
  );

  @SqlUpdate(SQL_UPDATE_SOURCE_NODE_TIMESTAMP)
  int updateMetricSourceNodeTimestamp(
      @Bind("cluster") String cluster,
      @Bind("host") String host,
      @Bind("timestamp") Instant timestamp
  );

  @SqlUpdate(SQL_ADD_METRIC_TYPE)
  int insertMetricType(
      @Bind("metricDomain") String metricDomain,
      @Bind("metricType") String metricType,
      @Bind("metricScope") String metricScope,
      @Bind("metricName") String metricName,
      @Bind("metricAttribute") String metricAttribute
  );

  @SqlUpdate(SQL_INSERT_METRIC)
  int insertMetric(
      @Bind("cluster") String cluster,
      @Bind("host") String host,
      @Bind("timestamp") Instant timestamp,
      @Bind("metricDomain") String metricDomain,
      @Bind("metricType") String metricType,
      @Bind("metricScope") String metricScope,
      @Bind("metricName") String metricName,
      @Bind("metricAttribute") String metricAttribute,
      @Bind("value") double value
  );

  @SqlQuery(SQL_GET_METRICS_FOR_HOST)
  @Mapper(GenericMetricMapper.class)
  Collection<GenericMetric> getMetricsForHost(
      @Bind("cluster") String cluster,
      @Bind("host") String host,
      @Bind("metricDomain") String metricDomain,
      @Bind("metricType") String metricType,
      @Bind("since") Instant since
  );

  @SqlQuery(SQL_GET_METRICS_FOR_CLUSTER)
  @Mapper(GenericMetricMapper.class)
  Collection<GenericMetric> getMetricsForCluster(
      @Bind("cluster") String cluster,
      @Bind("metricDomain") String metricDomain,
      @Bind("metricType") String metricType,
      @Bind("since") Instant since
  );

  @SqlUpdate(SQL_PURGE_OLD_METRICS)
  int purgeOldMetrics(
      @Bind("expirationTime") Instant expirationTime
  );

  @SqlUpdate(SQL_PURGE_OLD_SOURCE_NODES)
  int purgeOldSourceNodes(
      @Bind("expirationTime") Instant expirationTime
  );

  @SqlUpdate(SQL_INSERT_OPERATIONS)
  int insertOperations(
      @Bind("cluster") String cluster,
      @Bind("type") String operationType,
      @Bind("host") String host,
      @Bind("data") String data
  );

  @SqlQuery(SQL_LIST_OPERATIONS)
  String listOperations(
      @Bind("cluster") String cluster,
      @Bind("operationType") String operationType,
      @Bind("host") String host
  );

  @SqlUpdate(SQL_PURGE_OLD_NODE_OPERATIONS)
  int purgeOldNodeOperations(
      @Bind("expirationTime") Instant expirationTime
  );

  @SqlUpdate(SQL_INSERT_NODE_LOCK)
  int insertNodeLock(
      @Bind("repairId") UUID repairId,
      @Bind("node") String node,
      @Bind("reaperInstanceId") UUID reaperInstanceId,
      @Bind("reaperInstanceHost") String reaperInstanceHost,
      @Bind("segmentId") UUID segmentId
  );

  @SqlUpdate(SQL_UPDATE_NODE_LOCK)
  int updateNodeLock(
      @Bind("repairId") UUID repairId,
      @Bind("node") String node,
      @Bind("reaperInstanceId") UUID reaperInstanceId,
      @Bind("reaperInstanceHost") String reaperInstanceHost,
      @Bind("segmentId") UUID segmentId,
      @Bind("expirationTime") Instant expirationTime
  );

  @SqlUpdate(SQL_RENEW_NODE_LOCK)
  int renewNodeLock(
      @Bind("repairId") UUID repairId,
      @Bind("node") String node,
      @Bind("reaperInstanceId") UUID reaperInstanceId,
      @Bind("reaperInstanceHost") String reaperInstanceHost,
      @Bind("segmentId") UUID segmentId
  );

  @SqlUpdate(SQL_RELEASE_NODE_LOCK)
  int releaseNodeLock(
      @Bind("repairId") UUID repairId,
      @Bind("node") String node,
      @Bind("segmentId") UUID segmentId,
      @Bind("reaperInstanceId") UUID reaperInstanceId
  );

  @SqlUpdate(SQL_FORCE_RELEASE_NODE_LOCK)
  int forceReleaseNodeLock(
      @Bind("repairId") UUID repairId,
      @Bind("node") String node
  );

  @SqlQuery(SQL_SELECT_LOCKED_NODES)
  List<String> getLockedNodes(
      @Bind("expirationTime") Instant expirationTime
  );

  @SqlQuery(SQL_SELECT_LOCKED_SEGMENTS)
  List<Long> getLockedSegments(
      @Bind("expirationTime") Instant expirationTime
  );

  @SqlUpdate(SQL_INSERT_PERCENT_REPAIRED)
  int insertPercentRepairedMetric(
      @BindBean PercentRepairedMetric percentRepaired
  );

  @SqlUpdate(SQL_UPDATE_PERCENT_REPAIRED)
  int updatePercentRepairedMetric(
      @BindBean PercentRepairedMetric percentRepaired
  );

  @SqlQuery(SQL_SELECT_PERCENT_REPAIRED)
  @Mapper(PercentRepairedMetricMapper.class)
  Collection<PercentRepairedMetric> getPercentRepairedMetrics(
      @Bind("cluster") String cluster,
      @Bind("repairScheduleId") long repairScheduleId,
      @Bind("since") Instant since
  );

  @SqlUpdate(SQL_PURGE_PERCENT_REPAIRED)
  int purgeOldPercentRepairMetrics(
      @Bind("expirationTime") Instant expirationTime
  );
}
