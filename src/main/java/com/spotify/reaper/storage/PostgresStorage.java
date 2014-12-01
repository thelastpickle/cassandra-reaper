package com.spotify.reaper.storage;

import com.spotify.reaper.ReaperApplicationConfiguration;
import com.spotify.reaper.ReaperException;
import com.spotify.reaper.core.Cluster;
import com.spotify.reaper.core.RepairRun;

import org.skife.jdbi.v2.DBI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    }
    catch (ClassNotFoundException ex) {
      LOG.error("failed creating database connection: {}", ex);
      throw new ReaperException(ex);
    }
  }

  @Override
  public Cluster getCluster(String clusterName) {
    return null;
  }

  @Override
  public RepairRun addRepairRun(RepairRun repairRun) {
    return null;
  }

  @Override
  public RepairRun getRepairRun(long id) {
    return null;
  }
}
