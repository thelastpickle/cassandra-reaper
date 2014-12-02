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

  static final String SQL_GET_CLUSTER = "SELECT id, partitioner, name, seed_hosts FROM cluster ";

  static final String SQL_INSERT_CLUSTER = "INSERT INTO cluster (partitioner, name, seed_hosts) "
                                           + "VALUES (:partitioner, :name, :seedHosts)";

  static final String SQL_UPDATE_CLUSTER = "UPDATE cluster SET partitioner = :partitioner, "
                                           + "name = :name, seed_hosts = :seedHosts WHERE id = :id";

  @SqlQuery(SQL_GET_CLUSTER + "WHERE name = :name")
  @Mapper(ClusterMapper.class)
  public Cluster getCluster(@Bind("name") String clusterName);

  @SqlQuery(SQL_GET_CLUSTER + "WHERE id = :id")
  @Mapper(ClusterMapper.class)
  public Cluster getCluster(@Bind("id") long id);

  @SqlUpdate(SQL_INSERT_CLUSTER)
  public int insertCluster(@BindBean Cluster newCluster);

  @SqlUpdate(SQL_UPDATE_CLUSTER)
  public int updateCluster(@BindBean Cluster newCluster);

}
