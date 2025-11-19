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
import io.cassandrareaper.core.DiagEventSubscription;
import io.cassandrareaper.core.PercentRepairedMetric;
import io.cassandrareaper.core.RepairRun;
import io.cassandrareaper.core.RepairSchedule;
import io.cassandrareaper.core.RepairSegment;
import io.cassandrareaper.core.RepairUnit;
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
import io.cassandrareaper.storage.sqlite.UuidUtil;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

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

  @Deprecated // Use DAOs directly
  private void persist(Object... objects) {
    // No-op: persistence now handled by SQLite in DAOs
  }

  // Deprecated: Cluster operations - use getClusterDao() instead
  @Deprecated
  public Map<String, Cluster> getClusters() {
    return memClusterDao.getClusters().stream()
        .collect(Collectors.toMap(Cluster::getName, cluster -> cluster));
  }

  @Deprecated
  public Cluster addCluster(Cluster cluster) {
    memClusterDao.addCluster(cluster);
    return cluster;
  }

  @Deprecated
  public Cluster removeCluster(String clusterName) {
    return memClusterDao.deleteCluster(clusterName);
  }

  // Deprecated: RepairSchedule operations - use getRepairScheduleDao() instead
  @Deprecated
  public RepairSchedule addRepairSchedule(RepairSchedule schedule) {
    return memRepairScheduleDao.addRepairSchedule(schedule.with());
  }

  @Deprecated
  public RepairSchedule removeRepairSchedule(UUID id) {
    Optional<RepairSchedule> deletedOpt = memRepairScheduleDao.deleteRepairSchedule(id);
    return deletedOpt.orElse(null);
  }

  @Deprecated
  public Optional<RepairSchedule> getRepairScheduleById(UUID id) {
    return memRepairScheduleDao.getRepairSchedule(id);
  }

  @Deprecated
  public Collection<RepairSchedule> getRepairSchedules() {
    return memRepairScheduleDao.getAllRepairSchedules();
  }

  // Deprecated: RepairRun operations - use getRepairRunDao() instead
  @Deprecated
  public Collection<RepairRun> getRepairRuns() {
    // Get all repair runs by querying without a specific cluster filter
    // This is inefficient but maintains backward compatibility
    try (PreparedStatement stmt = sqliteConnection.prepareStatement("SELECT * FROM repair_run")) {
      try (ResultSet rs = stmt.executeQuery()) {
        List<RepairRun> runs = new ArrayList<>();
        while (rs.next()) {
          UUID id = UuidUtil.fromBytes(rs.getBytes("id"));
          Optional<RepairRun> runOpt = memoryRepairRunDao.getRepairRun(id);
          runOpt.ifPresent(runs::add);
        }
        return runs;
      }
    } catch (SQLException e) {
      LOG.error("Failed to get all repair runs", e);
      return Collections.emptyList();
    }
  }

  @Deprecated
  public RepairRun addRepairRun(RepairRun run) {
    // Use the DAO's addRepairRun which expects Builder + segments
    // Since we don't have segments here, pass empty collection
    return memoryRepairRunDao.addRepairRun(run.with(), Collections.emptyList());
  }

  @Deprecated
  public RepairRun removeRepairRun(UUID id) {
    Optional<RepairRun> deletedOpt = memoryRepairRunDao.deleteRepairRun(id);
    return deletedOpt.orElse(null);
  }

  @Deprecated
  public Optional<RepairRun> getRepairRunById(UUID id) {
    return memoryRepairRunDao.getRepairRun(id);
  }

  // Deprecated: RepairUnit operations - use getRepairUnitDao() instead
  @Deprecated
  public Collection<RepairUnit> getRepairUnits() {
    // Get all repair units by querying without a specific cluster filter
    // This is inefficient but maintains backward compatibility
    try (PreparedStatement stmt = sqliteConnection.prepareStatement("SELECT * FROM repair_unit")) {
      try (ResultSet rs = stmt.executeQuery()) {
        List<RepairUnit> units = new ArrayList<>();
        while (rs.next()) {
          UUID id = UuidUtil.fromBytes(rs.getBytes("id"));
          RepairUnit unit = memoryRepairUnitDao.getRepairUnit(id);
          if (unit != null) {
            units.add(unit);
          }
        }
        return units;
      }
    } catch (SQLException e) {
      LOG.error("Failed to get all repair units", e);
      return Collections.emptyList();
    }
  }

  @Deprecated
  public RepairUnit addRepairUnit(Optional<RepairUnit.Builder> key, RepairUnit unit) {
    return memoryRepairUnitDao.addRepairUnit(key.orElse(unit.with()));
  }

  @Deprecated
  public RepairUnit removeRepairUnit(Optional<RepairUnit.Builder> key, UUID id) {
    RepairUnit unit = memoryRepairUnitDao.getRepairUnit(id);
    if (unit != null) {
      memoryRepairUnitDao.deleteRepairUnit(id);
    }
    return unit;
  }

  @Deprecated
  public RepairUnit getRepairUnitById(UUID id) {
    return memoryRepairUnitDao.getRepairUnit(id);
  }

  @Deprecated
  public RepairUnit getRepairUnitByKey(RepairUnit.Builder key) {
    Optional<RepairUnit> unitOpt = memoryRepairUnitDao.getRepairUnit(key);
    return unitOpt.orElse(null);
  }

  // Deprecated: RepairSegment operations - use getRepairSegmentDao() instead
  @Deprecated
  public RepairSegment addRepairSegment(RepairSegment segment) {
    memRepairSegment.addRepairSegmentWithId(segment);
    return segment;
  }

  @Deprecated
  public RepairSegment removeRepairSegment(UUID id) {
    // Need to get all segments to find the one with matching ID
    try (PreparedStatement stmt =
        sqliteConnection.prepareStatement("SELECT run_id FROM repair_segment WHERE id = ?")) {
      stmt.setBytes(1, UuidUtil.toBytes(id));
      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          UUID runId = UuidUtil.fromBytes(rs.getBytes("run_id"));
          Optional<RepairSegment> segmentOpt = memRepairSegment.getRepairSegment(runId, id);
          if (segmentOpt.isPresent()) {
            // Note: This is a deprecated method so we keep the imperfect behavior
            memRepairSegment.deleteRepairSegmentsForRun(runId);
            return segmentOpt.get();
          }
        }
      }
    } catch (SQLException e) {
      LOG.error("Failed to remove repair segment {}", id, e);
    }
    return null;
  }

  @Deprecated
  public RepairSegment getRepairSegmentById(UUID id) {
    // Need to query to get the run ID first
    try (PreparedStatement stmt =
        sqliteConnection.prepareStatement("SELECT run_id FROM repair_segment WHERE id = ?")) {
      stmt.setBytes(1, UuidUtil.toBytes(id));
      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          UUID runId = UuidUtil.fromBytes(rs.getBytes("run_id"));
          Optional<RepairSegment> segmentOpt = memRepairSegment.getRepairSegment(runId, id);
          return segmentOpt.orElse(null);
        }
      }
    } catch (SQLException e) {
      LOG.error("Failed to get repair segment {}", id, e);
    }
    return null;
  }

  @Deprecated
  public Collection<RepairSegment> getRepairSegmentsByRunId(UUID runId) {
    return memRepairSegment.getRepairSegmentsForRun(runId);
  }

  // Deprecated: RepairSubscription operations - use getEventsDao() instead
  @Deprecated
  public Map<UUID, DiagEventSubscription> getSubscriptionsById() {
    return memEventsDao.getEventSubscriptions().stream()
        .collect(Collectors.toMap(sub -> sub.getId().get(), sub -> sub));
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
