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
import io.cassandrareaper.core.PercentRepairedMetric;
import io.cassandrareaper.storage.cluster.IClusterDao;
import io.cassandrareaper.storage.cluster.MemoryClusterDao;
import io.cassandrareaper.storage.events.IEventsDao;
import io.cassandrareaper.storage.events.MemoryEventsDao;
import io.cassandrareaper.storage.memory.MemoryStorageRoot;
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
import java.util.List;
import java.util.UUID;

import org.eclipse.store.storage.embedded.types.EmbeddedStorage;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implements the StorageAPI using transient Java classes.
 */
public final class MemoryStorageFacade implements IStorageDao {

  private static final Logger LOG = LoggerFactory.getLogger(MemoryStorageFacade.class);
  public final MemoryStorageRoot memoryStorageRoot;
  public final EmbeddedStorageManager embeddedStorage;
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

  public MemoryStorageFacade(String persistenceStoragePath) {
    LOG.info("Using memory storage backend. Persistence storage path: {}", persistenceStoragePath);
    this.embeddedStorage = EmbeddedStorage.start(Paths.get(persistenceStoragePath));
    if (embeddedStorage.root() == null) {
      LOG.info("Creating new data storage");
      this.memoryStorageRoot = new MemoryStorageRoot();
      embeddedStorage.setRoot(this.memoryStorageRoot);
    } else {
      LOG.info("Loading existing data from persistence storage");
      this.memoryStorageRoot = (MemoryStorageRoot) embeddedStorage.root();
      LOG.info("Loaded {} clusters: {}",
          memoryStorageRoot.getClusters().size(), memoryStorageRoot.getClusters().keySet());
      memoryStorageRoot.getClusters().entrySet().stream().forEach(entry -> {
        Cluster cluster = entry.getValue();
        LOG.info("Loaded cluster: {} / seeds: {}", cluster.getName(), cluster.getSeedHosts());
      });
    }
  }

  public MemoryStorageFacade() {
    this("/tmp/" + UUID.randomUUID().toString());
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
    embeddedStorage.shutdown();
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

  public synchronized void persistChanges() {
    LOG.info("Persisting changes to storage");
    MemoryStorageRoot root = (MemoryStorageRoot) embeddedStorage.root();
    this.embeddedStorage.setRoot(root);
    embeddedStorage.storeRoot();
  }
}

