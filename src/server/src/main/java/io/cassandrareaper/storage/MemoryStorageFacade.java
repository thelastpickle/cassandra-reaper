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
import io.cassandrareaper.storage.memory.MemoryStorageRoot;
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
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.eclipse.serializer.persistence.types.PersistenceFieldEvaluator;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implements the StorageAPI using transient Java classes. */
public final class MemoryStorageFacade implements IStorageDao {

  // Default time to live of leads taken on a segment
  private static final long DEFAULT_LEAD_TTL = 90_000;
  private static final Logger LOG = LoggerFactory.getLogger(MemoryStorageFacade.class);

  /**
   * Field evaluator to find transient attributes. This is needed to deal with persisting Guava
   * collections objects that sometimes use the transient keyword for some of their implementation's
   * backing stores
   */
  private static final PersistenceFieldEvaluator TRANSIENT_FIELD_EVALUATOR =
      (clazz, field) -> !field.getName().startsWith("_");

  @Deprecated // Will be removed in favor of direct SQLite access
  private final EmbeddedStorageManager embeddedStorage;
  @Deprecated // Will be removed in favor of direct SQLite access
  private final MemoryStorageRoot memoryStorageRoot;
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
        // In-memory mode (volatile)
        LOG.info("Using in-memory SQLite mode (volatile - data lost on restart)");
        this.sqliteConnection = DriverManager.getConnection("jdbc:sqlite::memory:");
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

        // Check for EclipseStore data and migrate if needed
        boolean migrated =
            EclipseStoreToSqliteMigration.migrateIfNeeded(storageDir, sqliteConnection);
        if (migrated) {
          LOG.info("Successfully migrated from EclipseStore to SQLite");
        }
      }

      // Initialize schema
      SqliteMigrationManager.initializeSchema(sqliteConnection);

    } catch (SQLException e) {
      LOG.error("Failed to initialize SQLite connection", e);
      throw new RuntimeException("Failed to initialize SQLite storage", e);
    }

    // TODO: Remove these deprecated fields in follow-up work
    // For now, keep them null to avoid breaking existing DAO code that may reference them
    this.embeddedStorage = null;
    this.memoryStorageRoot = new MemoryStorageRoot(); // Keep temporarily for compatibility

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
    // Deprecated: EclipseStore shutdown
    if (this.embeddedStorage != null) {
      this.embeddedStorage.shutdown();
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

  private void persist(Object... objects) {
    synchronized (memoryStorageRoot) {
      try {
        if (this.embeddedStorage != null) {
          this.embeddedStorage.storeAll(objects);
          this.embeddedStorage.storeRoot();
        }
      } catch (RuntimeException ex) {
        LOG.error("Failed persisting Reaper state to disk", ex);
        throw ex;
      }
    }
  }

  // Cluster operations
  public Map<String, Cluster> getClusters() {
    return this.memoryStorageRoot.getClusters();
  }

  public Cluster addCluster(Cluster cluster) {
    Cluster newCluster = this.memoryStorageRoot.addCluster(cluster);
    this.persist(memoryStorageRoot.getClusters());
    return newCluster;
  }

  public Cluster removeCluster(String clusterName) {
    Cluster cluster = this.memoryStorageRoot.removeCluster(clusterName);
    this.persist(memoryStorageRoot.getClusters());
    return cluster;
  }

  // RepairSchedule operations
  public RepairSchedule addRepairSchedule(RepairSchedule schedule) {
    RepairSchedule newSchedule = this.memoryStorageRoot.addRepairSchedule(schedule);
    this.persist(this.memoryStorageRoot.getRepairSchedules());
    return newSchedule;
  }

  public RepairSchedule removeRepairSchedule(UUID id) {
    RepairSchedule schedule = this.memoryStorageRoot.removeRepairSchedule(id);
    this.persist(this.memoryStorageRoot.getRepairSchedules());
    return schedule;
  }

  public Optional<RepairSchedule> getRepairScheduleById(UUID id) {
    return Optional.ofNullable(this.memoryStorageRoot.getRepairScheduleById(id));
  }

  public Collection<RepairSchedule> getRepairSchedules() {
    return this.memoryStorageRoot.getRepairSchedules().values();
  }

  // RepairRun operations
  public Collection<RepairRun> getRepairRuns() {
    return this.memoryStorageRoot.getRepairRuns().values();
  }

  public RepairRun addRepairRun(RepairRun run) {
    RepairRun newRun = this.memoryStorageRoot.addRepairRun(run);
    this.persist(this.memoryStorageRoot.getRepairRuns());
    return newRun;
  }

  public RepairRun removeRepairRun(UUID id) {
    RepairRun run = this.memoryStorageRoot.removeRepairRun(id);
    this.persist(this.memoryStorageRoot.getRepairRuns());
    return run;
  }

  public Optional<RepairRun> getRepairRunById(UUID id) {
    return Optional.ofNullable(this.memoryStorageRoot.getRepairRunById(id));
  }

  // RepairUnit operations
  public Collection<RepairUnit> getRepairUnits() {
    return this.memoryStorageRoot.getRepairUnits().values();
  }

  public RepairUnit addRepairUnit(Optional<RepairUnit.Builder> key, RepairUnit unit) {
    RepairUnit newUnit = this.memoryStorageRoot.addRepairUnit(key.get(), unit);
    this.persist(
        this.memoryStorageRoot.getRepairUnits(), this.memoryStorageRoot.getRepairUnitsByKey());
    return newUnit;
  }

  public RepairUnit removeRepairUnit(Optional<RepairUnit.Builder> key, UUID id) {
    RepairUnit unit = this.memoryStorageRoot.removeRepairUnit(key.get(), id);
    this.persist(
        this.memoryStorageRoot.getRepairUnits(), this.memoryStorageRoot.getRepairUnitsByKey());
    return unit;
  }

  public RepairUnit getRepairUnitById(UUID id) {
    return this.memoryStorageRoot.getrRepairUnitById(id);
  }

  public RepairUnit getRepairUnitByKey(RepairUnit.Builder key) {
    return this.memoryStorageRoot.getRepairUnitByKey(key);
  }

  // RepairSegment operations
  public RepairSegment addRepairSegment(RepairSegment segment) {
    final RepairSegment newSegment = this.memoryStorageRoot.addRepairSegment(segment);
    this.persist(this.memoryStorageRoot.getRepairSegments());
    return newSegment;
  }

  public RepairSegment removeRepairSegment(UUID id) {
    RepairSegment segment = this.memoryStorageRoot.removeRepairSegment(id);
    this.persist(this.memoryStorageRoot.getRepairSegments());
    return segment;
  }

  public RepairSegment getRepairSegmentById(UUID id) {
    return this.memoryStorageRoot.getRepairSegmentById(id);
  }

  public Collection<RepairSegment> getRepairSegmentsByRunId(UUID runId) {
    return this.memoryStorageRoot.getRepairSegments().values().stream()
        .filter(segment -> segment.getRunId().equals(runId))
        .collect(Collectors.toSet());
  }

  // RepairSubscription operations
  public Map<UUID, DiagEventSubscription> getSubscriptionsById() {
    return this.memoryStorageRoot.getSubscriptionsById();
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
