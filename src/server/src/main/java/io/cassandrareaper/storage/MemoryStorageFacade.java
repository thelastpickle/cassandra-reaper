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
import io.cassandrareaper.storage.cluster.ICluster;
import io.cassandrareaper.storage.cluster.MemClusterDao;
import io.cassandrareaper.storage.events.IEvents;
import io.cassandrareaper.storage.events.MemEventsDao;
import io.cassandrareaper.storage.metrics.MemMetricsDao;
import io.cassandrareaper.storage.repairrun.IRepairRun;
import io.cassandrareaper.storage.repairrun.MemRepairRunDao;
import io.cassandrareaper.storage.repairschedule.IRepairSchedule;
import io.cassandrareaper.storage.repairschedule.MemRepairScheduleDao;
import io.cassandrareaper.storage.repairsegment.IRepairSegment;
import io.cassandrareaper.storage.repairsegment.MemRepairSegment;
import io.cassandrareaper.storage.repairunit.IRepairUnit;
import io.cassandrareaper.storage.repairunit.MemRepairUnitDao;
import io.cassandrareaper.storage.snapshot.ISnapshot;
import io.cassandrareaper.storage.snapshot.MemSnapshotDao;

import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implements the StorageAPI using transient Java classes.
 */
public final class MemoryStorageFacade implements IStorage {

  private static final Logger LOG = LoggerFactory.getLogger(MemoryStorageFacade.class);
  private final MemRepairSegment memRepairSegment = new MemRepairSegment(this);
  private final MemRepairUnitDao memRepairUnitDao = new MemRepairUnitDao();
  private final MemRepairRunDao memRepairRunDao = new MemRepairRunDao(memRepairSegment, memRepairUnitDao);
  private final MemRepairScheduleDao memRepairScheduleDao = new MemRepairScheduleDao(memRepairUnitDao);
  private final MemEventsDao memEventsDao = new MemEventsDao();
  private final MemClusterDao memClusterDao = new MemClusterDao(
      memRepairUnitDao,
      memRepairRunDao,
      memRepairScheduleDao,
      memEventsDao
  );
  private final MemSnapshotDao memSnapshotDao = new MemSnapshotDao();
  private final MemMetricsDao memMetricsDao = new MemMetricsDao();

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
  public IEvents getEventsDao() {
    return this.memEventsDao;
  }

  @Override
  public ISnapshot getSnapshotDao() {
    return this.memSnapshotDao;
  }

  @Override
  public IRepairRun getRepairRunDao() {
    return this.memRepairRunDao;
  }

  @Override
  public IRepairSegment getRepairSegmentDao() {
    return this.memRepairSegment;
  }

  @Override
  public IRepairUnit getRepairUnitDao() {
    return this.memRepairUnitDao;
  }

  @Override
  public IRepairSchedule getRepairScheduleDao() {
    return this.memRepairScheduleDao;
  }

  @Override
  public ICluster getClusterDao() {
    return this.memClusterDao;
  }

}