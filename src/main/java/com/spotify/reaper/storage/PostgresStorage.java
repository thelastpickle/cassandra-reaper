package com.spotify.reaper.storage;

import com.spotify.reaper.ReaperApplicationConfiguration;
import com.spotify.reaper.ReaperException;
import com.spotify.reaper.core.Cluster;
import com.spotify.reaper.core.ColumnFamily;
import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.core.RepairSegment;
import com.spotify.reaper.storage.postgresql.IStoragePostgreSQL;

import org.joda.time.DateTime;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

import io.dropwizard.jdbi.DBIFactory;
import io.dropwizard.setup.Environment;

/**
 * Implements the StorageAPI using PostgreSQL database.
 */
public class PostgresStorage implements IStorage {

  private static final Logger LOG = LoggerFactory.getLogger(PostgresStorage.class);

  private final DBI jdbi;

  public PostgresStorage(ReaperApplicationConfiguration config, Environment environment)
      throws ReaperException {
    try {
      final DBIFactory factory = new DBIFactory();
      jdbi = factory.build(environment, config.getDataSourceFactory(), "postgresql");
    } catch (ClassNotFoundException ex) {
      LOG.error("failed creating database connection: {}", ex);
      throw new ReaperException(ex);
    }
  }

  @Override
  public Cluster getCluster(String clusterName) {
    Handle h = jdbi.open();
    IStoragePostgreSQL postgres = h.attach(IStoragePostgreSQL.class);
    Cluster result = postgres.getCluster(clusterName);
    h.close();
    return result;
  }

  @Override
  public RepairRun addRepairRun(RepairRun.Builder newRepairRun) {
    // TODO: implementation
    return null;
  }

  @Override
  public Cluster addCluster(Cluster newCluster) {
    Handle h = jdbi.open();
    IStoragePostgreSQL postgres = h.attach(IStoragePostgreSQL.class);
    int rowsAdded = postgres.insertCluster(newCluster);
    h.close();
    if (rowsAdded < 1) {
      LOG.warn("failed inserting cluster with name: {}", newCluster.getName());
      return null;
    }
    return newCluster;
  }

  @Override
  public Cluster updateCluster(Cluster cluster) {
    Handle h = jdbi.open();
    IStoragePostgreSQL postgres = h.attach(IStoragePostgreSQL.class);
    int rowsAdded = postgres.updateCluster(cluster);
    h.close();
    if (rowsAdded < 1) {
      LOG.warn("failed updating cluster with name: {}", cluster.getName());
      return null;
    }
    return cluster;
  }

  @Override
  public RepairRun getRepairRun(long id) {
    // TODO: implementation
    return null;
  }

  @Override
  public ColumnFamily addColumnFamily(ColumnFamily.Builder newTable) {
    // TODO: implementation
    return null;
  }

  @Override
  public ColumnFamily getColumnFamily(long id) {
    // TODO: implementation
    return null;
  }

  @Override
  public ColumnFamily getColumnFamily(String cluster, String keyspace, String table) {
    return null;
  }

  @Override
  public boolean addRepairSegments(Collection<RepairSegment> newSegments) {
    // TODO: implementation
    return false;
  }

  @Override
  public boolean updateRepairSegment(RepairSegment newRepairSegment) {
    // TODO: implementation
    return false;
  }

  @Override
  public RepairSegment getNextFreeSegment(long runId) {
    // TODO: implementation
    return null;
  }

  @Override
  public RepairSegment getNextFreeSegmentInRange(long runId, long start, long end) {
    // TODO: implementation
    return null;
  }
}
