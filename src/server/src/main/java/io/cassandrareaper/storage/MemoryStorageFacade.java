/*
 * Copyright 2014-2017 Spotify AB
 * Copyright 2016-2019 The Last Pickle Ltd
 *
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

import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.eclipse.serializer.persistence.types.PersistenceFieldEvaluator;
import org.eclipse.store.storage.embedded.types.EmbeddedStorage;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implements the StorageAPI using transient Java classes.
 */
public final class MemoryStorageFacade implements IStorageDao {

  private static final long DEFAULT_LEAD_TIME = 90;
  private static final Logger LOG = LoggerFactory.getLogger(MemoryStorageFacade.class);
  /** Field evaluator to find transient attributes. This is needed to deal with persisting Guava collections objects
   * that sometimes use the transient keyword for some of their implementation's backing stores**/
  private static final PersistenceFieldEvaluator TRANSIENT_FIELD_EVALUATOR =
      (clazz, field) -> !field.getName().startsWith("_");
  private static final UUID REAPER_INSTANCE_ID = UUID.randomUUID();

  private final EmbeddedStorageManager embeddedStorage;
  private final MemoryStorageRoot memoryStorageRoot;
  private final MemoryRepairSegmentDao memRepairSegment = new MemoryRepairSegmentDao(this);
  private final MemoryRepairUnitDao memoryRepairUnitDao = new MemoryRepairUnitDao(this);
  private final MemoryRepairRunDao memoryRepairRunDao =
      new MemoryRepairRunDao(this, memRepairSegment, memoryRepairUnitDao);
  private final MemoryRepairScheduleDao memRepairScheduleDao = new MemoryRepairScheduleDao(this, memoryRepairUnitDao);
  private final MemoryEventsDao memEventsDao = new MemoryEventsDao(this);
  private final MemoryClusterDao memClusterDao = new MemoryClusterDao(
      this,
      memoryRepairUnitDao,
      memoryRepairRunDao,
      memRepairScheduleDao,
      memEventsDao
  );
  private final MemorySnapshotDao memSnapshotDao = new MemorySnapshotDao();
  private final MemoryMetricsDao memMetricsDao = new MemoryMetricsDao();
  private final ReplicaLockManagerWithTtl repairRunLockManager;

  public MemoryStorageFacade(String persistenceStoragePath, long leadTime) {
    LOG.info("Using memory storage backend. Persistence storage path: {}", persistenceStoragePath);
    this.embeddedStorage = EmbeddedStorage.Foundation(Paths.get(persistenceStoragePath))
        .onConnectionFoundation(
            c -> {
              c.setFieldEvaluatorPersistable(TRANSIENT_FIELD_EVALUATOR);
            }
        ).createEmbeddedStorageManager();
    this.embeddedStorage.start();
    if (this.embeddedStorage.root() == null) {
      LOG.info("Creating new data storage");
      this.memoryStorageRoot = new MemoryStorageRoot();
      this.embeddedStorage.setRoot(this.memoryStorageRoot);
    } else {
      LOG.info("Loading existing data from persistence storage");
      this.memoryStorageRoot = (MemoryStorageRoot) this.embeddedStorage.root();
    }
    this.repairRunLockManager = new ReplicaLockManagerWithTtl(leadTime);
  }

  public MemoryStorageFacade() {
    this("/tmp/" + UUID.randomUUID().toString(), DEFAULT_LEAD_TIME);
  }

  public MemoryStorageFacade(String persistenceStoragePath) {
    this(persistenceStoragePath, DEFAULT_LEAD_TIME);
  }

  public MemoryStorageFacade(long leadTime) {
    this("/tmp/" + UUID.randomUUID().toString(), leadTime);
  }

  @Override
  public boolean isStorageConnected() {
    // Just assuming the MemoryStorage is always functional when instantiated.
    return true;
  }

  private boolean addClusterAssertions(Cluster cluster) {
    return memClusterDao.addClusterAssertions(cluster);
  }

  @Override
  public List<PercentRepairedMetric> getPercentRepairedMetrics(String clusterName, UUID repairScheduleId, Long since) {
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
    this.embeddedStorage.shutdown();
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
        this.embeddedStorage.storeAll(objects);
        this.embeddedStorage.storeRoot();
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
    Cluster cluster =  this.memoryStorageRoot.removeCluster(clusterName);
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
    this.persist(this.memoryStorageRoot.getRepairUnits(), this.memoryStorageRoot.getRepairUnitsByKey());
    return newUnit;
  }

  public RepairUnit removeRepairUnit(Optional<RepairUnit.Builder> key, UUID id) {
    RepairUnit unit = this.memoryStorageRoot.removeRepairUnit(key.get(), id);
    this.persist(this.memoryStorageRoot.getRepairUnits(), this.memoryStorageRoot.getRepairUnitsByKey());
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
        .filter(segment -> segment.getRunId().equals(runId)).collect(Collectors.toSet());
  }

  // RepairSubscription operations
  public Map<UUID, DiagEventSubscription> getSubscriptionsById() {
    return this.memoryStorageRoot.getSubscriptionsById();
  }

  @Override
  public boolean lockRunningRepairsForNodes(UUID runId, UUID segmentId, Set<String> replicas) {
    return repairRunLockManager.lockRunningRepairsForNodes(runId, segmentId, replicas);
  }

  @Override
  public boolean renewRunningRepairsForNodes(UUID runId, UUID segmentId, Set<String> replicas) {
    return repairRunLockManager.renewRunningRepairsForNodes(runId, segmentId, replicas);
  }

  @Override
  public boolean releaseRunningRepairsForNodes(UUID runId, UUID segmentId, Set<String> replicas) {
    return repairRunLockManager.releaseRunningRepairsForNodes(runId, segmentId, replicas);
  }

  @Override
  public Set<UUID> getLockedSegmentsForRun(UUID runId) {
    return repairRunLockManager.getLockedSegmentsForRun(runId);
  }
}
