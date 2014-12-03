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
