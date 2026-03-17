/*
 * Copyright 2014-2017 Spotify AB Copyright 2016-2019 The Last Pickle Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cassandrareaper.storage;

import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.PercentRepairedMetric;
import io.cassandrareaper.storage.cluster.IClusterDao;
import io.cassandrareaper.storage.cluster.MemoryClusterDao;
import io.cassandrareaper.storage.events.IEventsDao;
import io.cassandrareaper.storage.events.MemoryEventsDao;
import io.cassandrareaper.storage.memory.ReplicaLockManagerWithTtl;
import io.cassandrareaper.storage.metrics.MemoryMetricsDao;
import io.cassandrareaper.storage.repairrun.IRepairRunDao;
import io.cassandrareaper.storage.repairrun.MemoryRepairRunDao;
import io.cassandrareaper.storage.repairschedule.IRepairScheduleDao;
import io.cassandrareaper.storage.repairschedule.MemoryRepairScheduleDao;
import io.cassandrareaper.storage.repairsegment.IRepairSegmentDao;
import io.cassandrareaper.storage.repairsegment.MemoryRepairSegmentDao;
import io.cassandrareaper.storage.repairunit.IRepairUnitDao;
import io.cassandrareaper.storage.repairunit.MemoryRepairUnitDao;
import io.cassandrareaper.storage.snapshot.ISnapshotDao;
import io.cassandrareaper.storage.snapshot.MemorySnapshotDao;
import io.cassandrareaper.storage.sqlite.EclipseStoreToSqliteMigration;
import io.cassandrareaper.storage.sqlite.SqliteMigrationManager;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implements the StorageAPI using transient Java classes. */
public final class MemoryStorageFacade implements IStorageDao {

  // Default time to live of leads taken on a segment
  private static final long DEFAULT_LEAD_TTL = 90_000;
  private static final Logger LOG = LoggerFactory.getLogger(MemoryStorageFacade.class);

  private final Connection sqliteConnection;
  private final boolean isPersistent;
  private final MemoryRepairSegmentDao memRepairSegment;
  private final MemoryRepairUnitDao memoryRepairUnitDao;
  private final MemoryRepairRunDao memoryRepairRunDao;
  private final MemoryRepairScheduleDao memRepairScheduleDao;
  private final MemoryEventsDao memEventsDao;
  private final MemoryClusterDao memClusterDao;
  private final MemorySnapshotDao memSnapshotDao = new MemorySnapshotDao();
  private final MemoryMetricsDao memMetricsDao = new MemoryMetricsDao();
  private final ReplicaLockManagerWithTtl replicaLockManagerWithTtl;
  private final String persistenceStoragePath;

  public MemoryStorageFacade(String persistenceStoragePath, long leadTime) {
    LOG.info(
        "Using memory storage backend with SQLite. Persistence storage path: {}",
        persistenceStoragePath);
    this.persistenceStoragePath = persistenceStoragePath;

    // Initialize SQLite connection
    try {
      if (persistenceStoragePath == null || persistenceStoragePath.isEmpty()) {
        // In-memory mode (volatile) with shared cache disabled for test isolation
        // Using file::memory:?cache=private creates a unique in-memory database per connection
        LOG.info("Using in-memory SQLite mode (volatile - data lost on restart)");
        this.sqliteConnection =
            DriverManager.getConnection("jdbc:sqlite:file::memory:?cache=private");
        this.isPersistent = false;
      } else {
        // Persistent mode: persistenceStoragePath is a DIRECTORY
        File storageDir = new File(persistenceStoragePath);
        if (!storageDir.exists()) {
          storageDir.mkdirs();
          LOG.info("Created storage directory: {}", storageDir.getAbsolutePath());
        }

        String dbPath = new File(storageDir, "reaper.db").getAbsolutePath();
        LOG.info("Using persistent SQLite mode: {}", dbPath);
        this.sqliteConnection = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
        this.isPersistent = true;
      }

      // Enable autocommit mode for immediate visibility of writes
      sqliteConnection.setAutoCommit(true);
      LOG.info("SQLite autocommit enabled: {}", sqliteConnection.getAutoCommit());

      // Initialize schema FIRST (before migration)
      SqliteMigrationManager.initializeSchema(sqliteConnection);

      // Now check for EclipseStore data and migrate if needed
      if (isPersistent) {
        File storageDir = new File(persistenceStoragePath);
        boolean migrated =
            EclipseStoreToSqliteMigration.migrateIfNeeded(storageDir, sqliteConnection);
        if (migrated) {
          LOG.info("Successfully migrated from EclipseStore to SQLite");
        }
      }

    } catch (SQLException e) {
      LOG.error("Failed to initialize SQLite connection", e);
      throw new RuntimeException("Failed to initialize SQLite storage", e);
    }

    // Initialize DAOs (must be done after SQLite connection is established)
    this.memRepairSegment = new MemoryRepairSegmentDao(this);
    this.memoryRepairUnitDao = new MemoryRepairUnitDao(this);
    this.memoryRepairRunDao = new MemoryRepairRunDao(this, memRepairSegment, memoryRepairUnitDao);
    this.memRepairScheduleDao = new MemoryRepairScheduleDao(this, memoryRepairUnitDao);
    this.memEventsDao = new MemoryEventsDao(this);
    this.memClusterDao =
        new MemoryClusterDao(
            this, memoryRepairUnitDao, memoryRepairRunDao, memRepairScheduleDao, memEventsDao);

    this.replicaLockManagerWithTtl = new ReplicaLockManagerWithTtl(leadTime);
  }

  public MemoryStorageFacade() {
    this("", DEFAULT_LEAD_TTL);
  }

  public MemoryStorageFacade(String persistenceStoragePath) {
    this(persistenceStoragePath, DEFAULT_LEAD_TTL);
  }

  public MemoryStorageFacade(long leadTime) {
    this("", leadTime);
  }

  @Override
  public boolean isStorageConnected() {
    try {
      return this.sqliteConnection != null && !this.sqliteConnection.isClosed();
    } catch (SQLException e) {
      LOG.error("Failed to check SQLite connection status", e);
      return false;
    }
  }

  private boolean addClusterAssertions(Cluster cluster) {
    return memClusterDao.addClusterAssertions(cluster);
  }

  @Override
  public List<PercentRepairedMetric> getPercentRepairedMetrics(
      String clusterName, UUID repairScheduleId, Long since) {
    return memMetricsDao.getPercentRepairedMetrics(clusterName, repairScheduleId, since);
  }

  @Override
  public void storePercentRepairedMetric(PercentRepairedMetric metric) {
    memMetricsDao.storePercentRepairedMetric(metric);
  }

  @Override
  public void start() {
    // no-op
  }

  @Override
  public void stop() {
    if (this.sqliteConnection != null) {
      try {
        this.sqliteConnection.close();
        LOG.info("SQLite connection closed");
      } catch (SQLException e) {
        LOG.error("Error closing SQLite connection", e);
      }
    }
  }

  /**
   * Get the SQLite database connection for use by DAOs.
   *
   * @return The SQLite connection
   */
  public Connection getSqliteConnection() {
    return this.sqliteConnection;
  }

  /**
   * Check if storage is in persistent mode (backed by disk file).
   *
   * @return true if persistent, false if in-memory
   */
  public boolean isPersistent() {
    return this.isPersistent;
  }

  /**
   * Clear all data from the database (for testing purposes). This deletes all rows from all tables
   * while preserving schema.
   *
   * <p>NOTE: This method only clears database state. For integration tests, you should also clear
   * in-memory state such as running RepairRunner threads and ClusterFacade caches separately.
   */
  public void clearDatabase() {
    synchronized (sqliteConnection) {
      try {
        // Temporarily disable foreign keys for cleanup
        try (Statement stmt = sqliteConnection.createStatement()) {
          stmt.execute("PRAGMA foreign_keys = OFF");
        }

        // Delete all data (order matters due to foreign keys)
        try (Statement stmt = sqliteConnection.createStatement()) {
          stmt.execute("DELETE FROM repair_segment");
          stmt.execute("DELETE FROM repair_run");
          stmt.execute("DELETE FROM repair_schedule");
          stmt.execute("DELETE FROM repair_unit");
          stmt.execute("DELETE FROM diag_event_subscription");
          stmt.execute("DELETE FROM cluster");
        }

        // Re-enable foreign keys
        try (Statement stmt = sqliteConnection.createStatement()) {
          stmt.execute("PRAGMA foreign_keys = ON");
        }

        // Verify the clear worked
        try (Statement stmt = sqliteConnection.createStatement();
            ResultSet rs =
                stmt.executeQuery(
                    "SELECT "
                        + "(SELECT COUNT(*) FROM cluster) + "
                        + "(SELECT COUNT(*) FROM repair_unit) + "
                        + "(SELECT COUNT(*) FROM repair_run) + "
                        + "(SELECT COUNT(*) FROM repair_schedule) + "
                        + "(SELECT COUNT(*) FROM repair_segment) + "
                        + "(SELECT COUNT(*) FROM diag_event_subscription) as total")) {
          if (rs.next()) {
            int totalRows = rs.getInt(1);
            if (totalRows > 0) {
              LOG.error("Database clear FAILED - {} rows still remain after DELETE!", totalRows);
            } else {
              LOG.info("Database cleared and verified - 0 rows remaining");
            }
          }
        }

      } catch (SQLException e) {
        LOG.error("Failed to clear database", e);
        throw new RuntimeException("Failed to clear database", e);
      }
    }
  }

  @Override
  public IEventsDao getEventsDao() {
    return this.memEventsDao;
  }

  @Override
  public ISnapshotDao getSnapshotDao() {
    return this.memSnapshotDao;
  }

  @Override
  public IRepairRunDao getRepairRunDao() {
    return this.memoryRepairRunDao;
  }

  @Override
  public IRepairSegmentDao getRepairSegmentDao() {
    return this.memRepairSegment;
  }

  @Override
  public IRepairUnitDao getRepairUnitDao() {
    return this.memoryRepairUnitDao;
  }

  @Override
  public IRepairScheduleDao getRepairScheduleDao() {
    return this.memRepairScheduleDao;
  }

  @Override
  public IClusterDao getClusterDao() {
    return this.memClusterDao;
  }

  @Override
  public boolean lockRunningRepairsForNodes(UUID runId, UUID segmentId, Set<String> replicas) {
    return replicaLockManagerWithTtl.lockRunningRepairsForNodes(runId, segmentId, replicas);
  }

  @Override
  public boolean renewRunningRepairsForNodes(UUID runId, UUID segmentId, Set<String> replicas) {
    return replicaLockManagerWithTtl.renewRunningRepairsForNodes(runId, segmentId, replicas);
  }

  @Override
  public boolean releaseRunningRepairsForNodes(UUID runId, UUID segmentId, Set<String> replicas) {
    LOG.info(
        "Releasing locks for runId: {}, segmentId: {}, replicas: {}", runId, segmentId, replicas);
    return replicaLockManagerWithTtl.releaseRunningRepairsForNodes(runId, segmentId, replicas);
  }

  @Override
  public Set<UUID> getLockedSegmentsForRun(UUID runId) {
    return replicaLockManagerWithTtl.getLockedSegmentsForRun(runId);
  }

  public Set<String> getLockedNodesForRun(UUID runId) {
    // List the nodes which are locked for the runId
    return replicaLockManagerWithTtl.getLockedNodesForRun(runId);
  }
}
