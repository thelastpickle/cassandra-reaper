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

import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implements the StorageAPI using transient Java classes.
 */
public final class MemoryStorageFacade implements IStorageDao {

  private static final Logger LOG = LoggerFactory.getLogger(MemoryStorageFacade.class);
  private final MemoryRepairSegmentDao memRepairSegment = new MemoryRepairSegmentDao(this);
  private final MemoryRepairUnitDao memoryRepairUnitDao = new MemoryRepairUnitDao();
  private final MemoryRepairRunDao memoryRepairRunDao = new MemoryRepairRunDao(memRepairSegment, memoryRepairUnitDao);
  private final MemoryRepairScheduleDao memRepairScheduleDao = new MemoryRepairScheduleDao(memoryRepairUnitDao);
  private final MemoryEventsDao memEventsDao = new MemoryEventsDao();
  private final MemoryClusterDao memClusterDao = new MemoryClusterDao(
      memoryRepairUnitDao,
      memoryRepairRunDao,
      memRepairScheduleDao,
      memEventsDao
  );
  private final MemorySnapshotDao memSnapshotDao = new MemorySnapshotDao();
  private final MemoryMetricsDao memMetricsDao = new MemoryMetricsDao();

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
    // no-op
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

}