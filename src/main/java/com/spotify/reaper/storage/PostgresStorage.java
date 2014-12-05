package com.spotify.reaper.storage;

import com.google.common.collect.Range;

import com.spotify.reaper.ReaperApplicationConfiguration;
import com.spotify.reaper.ReaperException;
import com.spotify.reaper.core.Cluster;
import com.spotify.reaper.core.ColumnFamily;
import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.core.RepairSegment;
import com.spotify.reaper.storage.postgresql.IStoragePostgreSQL;

import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
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
  public boolean updateRepairRun(RepairRun repairRun) {
    return false;
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
  public boolean updateCluster(Cluster cluster) {
    Handle h = jdbi.open();
    IStoragePostgreSQL postgres = h.attach(IStoragePostgreSQL.class);
    int rowsAdded = postgres.updateCluster(cluster);
    h.close();
    if (rowsAdded < 1) {
      LOG.warn("failed updating cluster with name: {}", cluster.getName());
      return false;
    }
    return true;
  }

  @Override
  public RepairRun getRepairRun(long id, Object repairRunLock) {
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
  public Collection<RepairSegment> addRepairSegments(
      Collection<RepairSegment.Builder> newSegments) {
    // TODO: implementation
    return null;
  }

  @Override
  public boolean updateRepairSegment(RepairSegment newRepairSegment) {
    // TODO: implementation
    return false;
  }

  @Override
  public RepairSegment getRepairSegment(long id) {
    // TODO: implementation
    return null;
  }

  @Override
  public RepairSegment getNextFreeSegment(long runId) {
    // TODO: implementation
    return null;
  }

  @Override
  public RepairSegment getNextFreeSegmentInRange(long runId, BigInteger start, BigInteger end) {
    // TODO: implementation
    return null;
  }
}
