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
import com.spotify.reaper.core.ColumnFamily;
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

  static final String SQL_GET_ALL_CLUSTERS =
      "SELECT name, partitioner, seed_hosts FROM cluster";

  static final String SQL_GET_CLUSTER =
      "SELECT name, partitioner, seed_hosts FROM cluster WHERE name = :name";

  static final String SQL_INSERT_CLUSTER =
      "INSERT INTO cluster (name, partitioner, seed_hosts) "
      + "VALUES (:name, :partitioner, :seedHosts)";

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
  static final String SQL_INSERT_REPAIR_RUN =
      "INSERT INTO repair_run (cluster_name, column_family_id, cause, owner, state, "
      + "creation_time, start_time, end_time, intensity) "
      + "VALUES (:clusterName, :columnFamilyId, :cause, :owner, :runState, :creationTime, "
      + ":startTime, :endTime, :intensity)";

  static final String SQL_UPDATE_REPAIR_RUN =
      "UPDATE repair_run SET cause = :cause, owner = :owner, state = :runState, "
      + "start_time = :startTime, end_time = :endTime, intensity = :intensity WHERE id = :id";

  static final String SQL_GET_REPAIR_RUN =
      "SELECT id, cluster_name, column_family_id, cause, owner, state, creation_time, "
      + "start_time, end_time, intensity FROM repair_run WHERE id = :id";

  static final String SQL_GET_REPAIR_RUNS_FOR_CLUSTER =
      "SELECT id, cluster_name, column_family_id, cause, owner, state, creation_time, "
      + "start_time, end_time, intensity FROM repair_run WHERE cluster_name = :clusterName";

  @SqlQuery(SQL_GET_REPAIR_RUN)
  @Mapper(RepairRunMapper.class)
  public RepairRun getRepairRun(@Bind("id") long repairRunId);

  @SqlQuery(SQL_GET_REPAIR_RUNS_FOR_CLUSTER)
  @Mapper(RepairRunMapper.class)
  public Collection<RepairRun> getRepairRunsForCluster(@Bind("clusterName") String clusterName);

  @SqlUpdate(SQL_INSERT_REPAIR_RUN)
  @GetGeneratedKeys
  public long insertRepairRun(@BindBean RepairRun newRepairRun);

  @SqlUpdate(SQL_UPDATE_REPAIR_RUN)
  public int updateRepairRun(@BindBean RepairRun newRepairRun);

  // ColumnFamily
  //
  static final String SQL_INSERT_COLUMN_FAMILY =
      "INSERT INTO column_family (cluster_name, keyspace_name, name, segment_count, "
      + "snapshot_repair) VALUES (:clusterName, :keyspaceName, :name, :segmentCount, "
      + ":snapshotRepair)";

  static final String SQL_GET_COLUMN_FAMILY =
      "SELECT id, cluster_name, keyspace_name, name, segment_count, snapshot_repair "
      + "FROM column_family WHERE id = :id";

  static final String SQL_GET_COLUMN_FAMILY_BY_CLUSTER_AND_NAME =
      "SELECT id, cluster_name, keyspace_name, name, segment_count, snapshot_repair "
      + "FROM column_family WHERE cluster_name = :clusterName AND keyspace_name = :keyspaceName "
      + "AND name = :name";

  @SqlQuery(SQL_GET_COLUMN_FAMILY)
  @Mapper(ColumnFamilyMapper.class)
  public ColumnFamily getColumnFamily(@Bind("id") long columnFamilyId);

  @SqlQuery(SQL_GET_COLUMN_FAMILY_BY_CLUSTER_AND_NAME)
  @Mapper(ColumnFamilyMapper.class)
  public ColumnFamily getColumnFamilyByClusterAndName(@Bind("clusterName") String clusterName,
                                                      @Bind("keyspaceName") String keyspaceName,
                                                      @Bind("name") String tableName);

  @SqlUpdate(SQL_INSERT_COLUMN_FAMILY)
  @GetGeneratedKeys
  public long insertColumnFamily(@BindBean ColumnFamily newColumnFamily);

  // RepairSegment
  //
  static final String SQL_INSERT_REPAIR_SEGMENT =
      "INSERT INTO repair_segment (column_family_id, run_id, start_token, end_token, state, "
      + "start_time, end_time) VALUES (:columnFamilyId, :runId, :startToken, :endToken, "
      + ":state, :startTime, :endTime)";

  static final String SQL_UPDATE_REPAIR_SEGMENT =
      "UPDATE repair_segment SET column_family_id = :columnFamilyId, run_id = :runId, "
      + "start_token = :startToken, end_token = :endToken, state = :state, "
      + "start_time = :startTime, end_time = :endTime WHERE id = :id";

  static final String SQL_GET_REPAIR_SEGMENT =
      "SELECT id, column_family_id, run_id, start_token, end_token, state, start_time, end_time "
      + "FROM repair_segment WHERE id = :id";

  static final String SQL_GET_NEXT_FREE_REPAIR_SEGMENT =
      "SELECT id, column_family_id, run_id, start_token, end_token, state, start_time, end_time "
      + "FROM repair_segment WHERE run_id = :runId AND state = 0 ORDER BY start_token ASC LIMIT 1";

  static final String SQL_GET_NEXT_FREE_REPAIR_SEGMENT_ON_RANGE =
      "SELECT id, column_family_id, run_id, start_token, end_token, state, start_time, end_time "
      + "FROM repair_segment WHERE run_id = :runId AND state = 0 AND "
      + "start_token >= :startToken AND end_token < :endToken ORDER BY start_token ASC LIMIT 1";

  @SqlBatch(SQL_INSERT_REPAIR_SEGMENT)
  @BatchChunkSize(500)
  public int insertRepairSegments(@BindBean Iterator<RepairSegment> newRepairSegments);

  @SqlUpdate(SQL_UPDATE_REPAIR_SEGMENT)
  public int updateRepairSegment(@BindBean RepairSegment newRepairSegment);

  @SqlQuery(SQL_GET_REPAIR_SEGMENT)
  @Mapper(RepairSegmentMapper.class)
  public RepairSegment getRepairSegment(@Bind("id") long repairSegmentId);

  @SqlQuery(SQL_GET_NEXT_FREE_REPAIR_SEGMENT)
  @Mapper(RepairSegmentMapper.class)
  public RepairSegment getNextFreeRepairSegment(@Bind("runId") long runId);

  @SqlQuery(SQL_GET_NEXT_FREE_REPAIR_SEGMENT_ON_RANGE)
  @Mapper(RepairSegmentMapper.class)
  public RepairSegment getNextFreeRepairSegmentOnRange(@Bind("runId") long runId,
                                                       @Bind("startToken") BigInteger startToken,
                                                       @Bind("endToken") BigInteger endToken);

}
