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
package com.spotify.reaper.storage.postgresql;

import com.spotify.reaper.core.Cluster;
import com.spotify.reaper.core.RepairUnit;
import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.core.RepairSegment;

import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.BindBean;
import org.skife.jdbi.v2.sqlobject.GetGeneratedKeys;
import org.skife.jdbi.v2.sqlobject.SqlBatch;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.customizers.BatchChunkSize;
import org.skife.jdbi.v2.sqlobject.customizers.Mapper;

import java.math.BigInteger;
import java.util.Collection;
import java.util.Iterator;

/**
 * JDBI based PostgreSQL interface.
 * 
 * See following specification for more info: http://jdbi.org/sql_object_api_dml/
 */
public interface IStoragePostgreSQL {

  @SqlQuery("SELECT version()")
  public String getVersion();

  // Cluster
  //
  static final String SQL_CLUSTER_ALL_FIELDS = "name, partitioner, seed_hosts";

  static final String SQL_GET_ALL_CLUSTERS = "SELECT " + SQL_CLUSTER_ALL_FIELDS + " FROM cluster";

  static final String SQL_GET_CLUSTER =
      "SELECT " + SQL_CLUSTER_ALL_FIELDS + " FROM cluster WHERE name = :name";

  static final String SQL_INSERT_CLUSTER = "INSERT INTO cluster (" + SQL_CLUSTER_ALL_FIELDS +
      ") VALUES (:name, :partitioner, :seedHosts)";

  static final String SQL_UPDATE_CLUSTER =
      "UPDATE cluster SET partitioner = :partitioner, seed_hosts = :seedHosts WHERE name = :name";

  @SqlQuery(SQL_GET_CLUSTER)
  @Mapper(ClusterMapper.class)
  public Cluster getCluster(@Bind("name") String clusterName);

  @SqlQuery(SQL_GET_ALL_CLUSTERS)
  @Mapper(ClusterMapper.class)
  public Collection<Cluster> getClusters();

  @SqlUpdate(SQL_INSERT_CLUSTER)
  public int insertCluster(@BindBean Cluster newCluster);

  @SqlUpdate(SQL_UPDATE_CLUSTER)
  public int updateCluster(@BindBean Cluster newCluster);

  // RepairRun
  //
  static final String SQL_REPAIR_RUN_ALL_FIELDS_NO_ID =
      "cluster_name, column_family_id, cause, owner, state, creation_time, "
          + "start_time, end_time, pause_time, intensity";

  static final String SQL_REPAIR_RUN_ALL_FIELDS = "id, " + SQL_REPAIR_RUN_ALL_FIELDS_NO_ID;

  static final String SQL_INSERT_REPAIR_RUN =
      "INSERT INTO repair_run (" + SQL_REPAIR_RUN_ALL_FIELDS_NO_ID + ") VALUES "
          + "(:clusterName, :columnFamilyId, :cause, :owner, :runState, :creationTime, "
          + ":startTime, :endTime, :pauseTime, :intensity)";

  static final String SQL_UPDATE_REPAIR_RUN =
      "UPDATE repair_run SET cause = :cause, owner = :owner, state = :runState, "
          + "start_time = :startTime, end_time = :endTime, pause_time = :pauseTime, "
          + "intensity = :intensity WHERE id = :id";

  static final String SQL_GET_REPAIR_RUN =
      "SELECT " + SQL_REPAIR_RUN_ALL_FIELDS + " FROM repair_run WHERE id = :id";

  static final String SQL_GET_REPAIR_RUNS_FOR_CLUSTER =
      "SELECT " + SQL_REPAIR_RUN_ALL_FIELDS + " FROM repair_run WHERE cluster_name = :clusterName";

  static final String SQL_GET_REPAIR_RUNS_WITH_STATE =
      "SELECT " + SQL_REPAIR_RUN_ALL_FIELDS + " FROM repair_run WHERE state = :state";

  @SqlQuery(SQL_GET_REPAIR_RUN)
  @Mapper(RepairRunMapper.class)
  public RepairRun getRepairRun(@Bind("id") long repairRunId);

  @SqlQuery(SQL_GET_REPAIR_RUNS_FOR_CLUSTER)
  @Mapper(RepairRunMapper.class)
  public Collection<RepairRun> getRepairRunsForCluster(@Bind("clusterName") String clusterName);

  @SqlQuery(SQL_GET_REPAIR_RUNS_WITH_STATE)
  @Mapper(RepairRunMapper.class)
  public Collection<RepairRun> getRepairRunsWithState(@Bind("state") RepairRun.RunState state);

  @SqlUpdate(SQL_INSERT_REPAIR_RUN)
  @GetGeneratedKeys
  public long insertRepairRun(@BindBean RepairRun newRepairRun);

  @SqlUpdate(SQL_UPDATE_REPAIR_RUN)
  public int updateRepairRun(@BindBean RepairRun newRepairRun);

  // RepairUnit
  //
  static final String SQL_REPAIR_UNIT_ALL_FIELDS_NO_ID =
      "cluster_name, keyspace_name, column_families, segment_count, snapshot_repair";

  static final String SQL_REPAIR_UNIT_ALL_FIELDS = "id, " + SQL_REPAIR_UNIT_ALL_FIELDS_NO_ID;

  static final String SQL_INSERT_REPAIR_UNIT =
      "INSERT INTO column_family (" + SQL_REPAIR_UNIT_ALL_FIELDS_NO_ID + ") VALUES "
          + "(:clusterName, :keyspaceName, :columnFamilies, :segmentCount, :snapshotRepair)";

  static final String SQL_GET_REPAIR_UNIT =
      "SELECT " + SQL_REPAIR_UNIT_ALL_FIELDS + " FROM column_family WHERE id = :id";

  static final String SQL_GET_REPAIR_UNIT_BY_CLUSTER_AND_TABLES =
      "SELECT " + SQL_REPAIR_UNIT_ALL_FIELDS + " FROM column_family "
          + "WHERE cluster_name = :clusterName AND keyspace_name = :keyspaceName "
          + "AND column_families @> :columnFamilies AND column_families <@ :columnFamilies";

  @SqlQuery(SQL_GET_REPAIR_UNIT)
  @Mapper(RepairUnitMapper.class)
  public RepairUnit getRepairUnit(@Bind("id") long columnFamilyId);

  @SqlQuery(SQL_GET_REPAIR_UNIT_BY_CLUSTER_AND_TABLES)
  @Mapper(RepairUnitMapper.class)
  public RepairUnit getRepairUnitByClusterAndTables(@Bind("clusterName") String clusterName,
      @Bind("keyspaceName") String keyspaceName,
      @Bind("columnFamilies") Collection<String> columnFamilies);

  @SqlUpdate(SQL_INSERT_REPAIR_UNIT)
  @GetGeneratedKeys
  public long insertRepairUnit(@BindBean RepairUnit newRepairUnit);

  // RepairSegment
  //
  static final String SQL_REPAIR_SEGMENT_ALL_FIELDS_NO_ID =
      "column_family_id, run_id, start_token, end_token, state, start_time, end_time, fail_count";

  static final String SQL_REPAIR_SEGMENT_ALL_FIELDS = "id, " + SQL_REPAIR_SEGMENT_ALL_FIELDS_NO_ID;

  static final String SQL_INSERT_REPAIR_SEGMENT =
      "INSERT INTO repair_segment (" + SQL_REPAIR_SEGMENT_ALL_FIELDS_NO_ID + ") VALUES "
          + "(:columnFamilyId, :runId, :startToken, :endToken, :state, :startTime, :endTime, "
          + ":failCount)";

  static final String SQL_UPDATE_REPAIR_SEGMENT =
      "UPDATE repair_segment SET column_family_id = :columnFamilyId, run_id = :runId, "
          + "start_token = :startToken, end_token = :endToken, state = :state, "
          + "start_time = :startTime, end_time = :endTime, fail_count = :failCount WHERE id = :id";

  static final String SQL_GET_REPAIR_SEGMENT =
      "SELECT " + SQL_REPAIR_SEGMENT_ALL_FIELDS + " FROM repair_segment WHERE id = :id";

  static final String SQL_GET_REPAIR_SEGMENT_FOR_RUN_WITH_STATE =
      "SELECT " + SQL_REPAIR_SEGMENT_ALL_FIELDS + " FROM repair_segment WHERE "
          + "run_id = :runId AND state = :state";

  static final String SQL_GET_NEXT_FREE_REPAIR_SEGMENT =
      "SELECT " + SQL_REPAIR_SEGMENT_ALL_FIELDS + " FROM repair_segment WHERE run_id = :runId "
          + "AND state = 0 ORDER BY fail_count ASC, start_token ASC LIMIT 1";

  static final String SQL_GET_NEXT_FREE_REPAIR_SEGMENT_ON_RANGE =
      "SELECT " + SQL_REPAIR_SEGMENT_ALL_FIELDS + " FROM repair_segment WHERE "
          + "run_id = :runId AND state = 0 AND start_token >= :startToken "
          + "AND end_token < :endToken ORDER BY fail_count ASC, start_token ASC LIMIT 1";

  @SqlBatch(SQL_INSERT_REPAIR_SEGMENT)
  @BatchChunkSize(500)
  public void insertRepairSegments(@BindBean Iterator<RepairSegment> newRepairSegments);

  @SqlUpdate(SQL_UPDATE_REPAIR_SEGMENT)
  public int updateRepairSegment(@BindBean RepairSegment newRepairSegment);

  @SqlQuery(SQL_GET_REPAIR_SEGMENT)
  @Mapper(RepairSegmentMapper.class)
  public RepairSegment getRepairSegment(@Bind("id") long repairSegmentId);

  @SqlQuery(SQL_GET_REPAIR_SEGMENT_FOR_RUN_WITH_STATE)
  @Mapper(RepairSegmentMapper.class)
  public Collection<RepairSegment> getRepairSegmentForRunWithState(@Bind("runId") long runId,
      @Bind("state")
      RepairSegment.State state);

  @SqlQuery(SQL_GET_NEXT_FREE_REPAIR_SEGMENT)
  @Mapper(RepairSegmentMapper.class)
  public RepairSegment getNextFreeRepairSegment(@Bind("runId") long runId);

  @SqlQuery(SQL_GET_NEXT_FREE_REPAIR_SEGMENT_ON_RANGE)
  @Mapper(RepairSegmentMapper.class)
  public RepairSegment getNextFreeRepairSegmentOnRange(@Bind("runId") long runId,
      @Bind("startToken") BigInteger startToken,
      @Bind("endToken") BigInteger endToken);

  // Utility methods
  //
  static final String SQL_GET_REPAIR_RUN_IDS_FOR_CLUSTER =
      "SELECT id FROM repair_run WHERE cluster_name = :clusterName";

  static final String SQL_SEGMENTS_AMOUNT_FOR_REPAIR_RUN =
      "SELECT count(*) FROM repair_segment WHERE run_id = :runId AND state = :state";

  @SqlQuery(SQL_GET_REPAIR_RUN_IDS_FOR_CLUSTER) Collection<Long> getRepairRunIdsForCluster(
      @Bind("clusterName") String clusterName);

  @SqlQuery(SQL_SEGMENTS_AMOUNT_FOR_REPAIR_RUN) int getSegmentAmountForRepairRun(
      @Bind("runId") long runId,
      @Bind("state") RepairSegment.State state);

}
