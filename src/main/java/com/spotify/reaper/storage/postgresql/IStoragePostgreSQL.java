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

import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.BindBean;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.customizers.Mapper;

/**
 * JDBI based PostgreSQL interface.
 *
 * See following specification for more info: http://jdbi.org/sql_object_api_dml/
 */
public interface IStoragePostgreSQL {

  static final String SQL_GET_CLUSTER = "SELECT name, partitioner, seed_hosts FROM cluster "
                                        + "WHERE name = :name";

  static final String SQL_INSERT_CLUSTER = "INSERT INTO cluster (name, partitioner, seed_hosts) "
                                           + "VALUES (:name, :partitioner, :seedHosts)";

  static final String SQL_UPDATE_CLUSTER = "UPDATE cluster SET partitioner = :partitioner, "
                                           + "seed_hosts = :seedHosts WHERE name = :name";

  @SqlQuery(SQL_GET_CLUSTER)
  @Mapper(ClusterMapper.class)
  public Cluster getCluster(@Bind("name") String clusterName);

  @SqlUpdate(SQL_INSERT_CLUSTER)
  public int insertCluster(@BindBean Cluster newCluster);

  @SqlUpdate(SQL_UPDATE_CLUSTER)
  public int updateCluster(@BindBean Cluster newCluster);

}
