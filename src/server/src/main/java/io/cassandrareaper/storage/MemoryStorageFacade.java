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
import io.cassandrareaper.core.Snapshot;
import io.cassandrareaper.resources.view.RepairRunStatus;
import io.cassandrareaper.resources.view.RepairScheduleStatus;
import io.cassandrareaper.storage.cluster.MemClusterDao;
import io.cassandrareaper.storage.events.MemEventsDao;
import io.cassandrareaper.storage.metrics.MemMetricsDao;
import io.cassandrareaper.storage.repairrun.MemRepairRunDao;
import io.cassandrareaper.storage.repairschedule.MemRepairScheduleDao;
import io.cassandrareaper.storage.repairsegment.MemRepairSegment;
import io.cassandrareaper.storage.repairunit.MemRepairUnitDao;
import io.cassandrareaper.storage.snapshot.MemSnapshotDao;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.SortedSet;
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

  @Override
  public Collection<Cluster> getClusters() {
    return memClusterDao.getClusters();
  }

  @Override
  public boolean addCluster(Cluster cluster) {
    return memClusterDao.addCluster(cluster);
  }

  @Override
  public boolean updateCluster(Cluster newCluster) {
    return memClusterDao.updateCluster(newCluster);
  }


  private boolean addClusterAssertions(Cluster cluster) {

    // TODO – unit tests need to also always set the paritioner
    //Preconditions.checkState(cluster.getPartitioner().isPresent(), "Cannot store cluster with no partitioner.");

    // assert we're not overwriting a cluster with the same name but different node list

    return memClusterDao.addClusterAssertions(cluster);
  }

  @Override
  public Cluster getCluster(String clusterName) {
    return memClusterDao.getCluster(clusterName);
  }

  @Override
  public Cluster deleteCluster(String clusterName) {

    return memClusterDao.deleteCluster(clusterName);
  }

  @Override
  public RepairRun addRepairRun(RepairRun.Builder repairRun, Collection<RepairSegment.Builder> newSegments) {
    return memRepairRunDao.addRepairRun(repairRun, newSegments);
  }

  @Override
  public boolean updateRepairRun(RepairRun repairRun) {
    return memRepairRunDao.updateRepairRun(repairRun);
  }

  @Override
  public boolean updateRepairRun(RepairRun repairRun, Optional<Boolean> updateRepairState) {
    return memRepairRunDao.updateRepairRun(repairRun, updateRepairState);
  }

  @Override
  public Optional<RepairRun> getRepairRun(UUID id) {
    return memRepairRunDao.getRepairRun(id);
  }

  @Override
  public List<RepairRun> getRepairRunsForCluster(String clusterName, Optional<Integer> limit) {
    return memRepairRunDao.getRepairRunsForCluster(clusterName, limit);
  }

  @Override
  public List<RepairRun> getRepairRunsForClusterPrioritiseRunning(String clusterName, Optional<Integer> limit) {
    return memRepairRunDao.getRepairRunsForClusterPrioritiseRunning(clusterName, limit);
  }

  @Override
  public Collection<RepairRun> getRepairRunsForUnit(UUID repairUnitId) {
    return memRepairRunDao.getRepairRunsForUnit(repairUnitId);
  }

  @Override
  public Collection<RepairRun> getRepairRunsWithState(RepairRun.RunState runState) {
    return memRepairRunDao.getRepairRunsWithState(runState);
  }

  @Override
  public Optional<RepairRun> deleteRepairRun(UUID id) {
    return memRepairRunDao.deleteRepairRun(id);
  }

  @Override
  public RepairUnit addRepairUnit(RepairUnit.Builder repairUnit) {
    return memRepairUnitDao.addRepairUnit(repairUnit);
  }

  @Override
  public void updateRepairUnit(RepairUnit updatedRepairUnit) {
    memRepairUnitDao.updateRepairUnit(updatedRepairUnit);
  }

  @Override
  public RepairUnit getRepairUnit(UUID id) {
    return memRepairUnitDao.getRepairUnit(id);
  }

  @Override
  public Optional<RepairUnit> getRepairUnit(RepairUnit.Builder params) {
    return memRepairUnitDao.getRepairUnit(params);
  }

  @Override
  public boolean updateRepairSegment(RepairSegment newRepairSegment) {
    return memRepairSegment.updateRepairSegment(newRepairSegment);
  }

  @Override
  public Optional<RepairSegment> getRepairSegment(UUID runId, UUID segmentId) {
    return memRepairSegment.getRepairSegment(runId, segmentId);
  }

  @Override
  public Collection<RepairSegment> getRepairSegmentsForRun(UUID runId) {
    return memRepairSegment.getRepairSegmentsForRun(runId);
  }

  @Override
  public List<RepairSegment> getNextFreeSegments(UUID runId) {
    return memRepairSegment.getNextFreeSegments(runId);
  }

  @Override
  public Collection<RepairSegment> getSegmentsWithState(UUID runId, RepairSegment.State segmentState) {
    return memRepairSegment.getSegmentsWithState(runId, segmentState);
  }

  @Override
  public SortedSet<UUID> getRepairRunIdsForCluster(String clusterName, Optional<Integer> limit) {
    return memRepairRunDao.getRepairRunIdsForCluster(clusterName, limit);
  }

  @Override
  public int getSegmentAmountForRepairRun(UUID runId) {
    return memRepairSegment.getSegmentAmountForRepairRun(runId);
  }

  @Override
  public int getSegmentAmountForRepairRunWithState(UUID runId, RepairSegment.State state) {
    return memRepairSegment.getSegmentAmountForRepairRunWithState(runId, state);
  }

  @Override
  public RepairSchedule addRepairSchedule(RepairSchedule.Builder repairSchedule) {
    return memRepairScheduleDao.addRepairSchedule(repairSchedule);
  }

  @Override
  public Optional<RepairSchedule> getRepairSchedule(UUID id) {
    return memRepairScheduleDao.getRepairSchedule(id);
  }

  @Override
  public Collection<RepairSchedule> getRepairSchedulesForCluster(String clusterName) {
    return memRepairScheduleDao.getRepairSchedulesForCluster(clusterName);
  }

  @Override
  public Collection<RepairSchedule> getRepairSchedulesForCluster(String clusterName, boolean incremental) {
    return memRepairScheduleDao.getRepairSchedulesForCluster(clusterName, incremental);
  }

  @Override
  public Collection<RepairSchedule> getRepairSchedulesForKeyspace(String keyspaceName) {
    return memRepairScheduleDao.getRepairSchedulesForKeyspace(keyspaceName);
  }

  @Override
  public Collection<RepairSchedule> getRepairSchedulesForClusterAndKeyspace(String clusterName, String keyspaceName) {
    return memRepairScheduleDao.getRepairSchedulesForClusterAndKeyspace(clusterName, keyspaceName);
  }

  @Override
  public Collection<RepairSchedule> getAllRepairSchedules() {
    return memRepairScheduleDao.getAllRepairSchedules();
  }

  @Override
  public boolean updateRepairSchedule(RepairSchedule newRepairSchedule) {
    return memRepairScheduleDao.updateRepairSchedule(newRepairSchedule);
  }

  @Override
  public Optional<RepairSchedule> deleteRepairSchedule(UUID id) {
    return memRepairScheduleDao.deleteRepairSchedule(id);
  }

  @Override
  public Collection<RepairRunStatus> getClusterRunStatuses(String clusterName, int limit) {
    return memRepairRunDao.getClusterRunStatuses(clusterName, limit);
  }

  @Override
  public Collection<RepairScheduleStatus> getClusterScheduleStatuses(String clusterName) {
    return memRepairScheduleDao.getClusterScheduleStatuses(clusterName);
  }

  @Override
  public boolean saveSnapshot(Snapshot snapshot) {
    return memSnapshotDao.saveSnapshot(snapshot);
  }

  @Override
  public boolean deleteSnapshot(Snapshot snapshot) {
    return memSnapshotDao.deleteSnapshot(snapshot);
  }

  @Override
  public Snapshot getSnapshot(String clusterName, String snapshotName) {
    return memSnapshotDao.getSnapshot(clusterName, snapshotName);
  }

  @Override
  public Collection<DiagEventSubscription> getEventSubscriptions() {
    return memEventsDao.getEventSubscriptions();
  }

  @Override
  public Collection<DiagEventSubscription> getEventSubscriptions(String clusterName) {
    return memEventsDao.getEventSubscriptions(clusterName);
  }

  @Override
  public DiagEventSubscription getEventSubscription(UUID id) {
    return memEventsDao.getEventSubscription(id);
  }

  @Override
  public DiagEventSubscription addEventSubscription(DiagEventSubscription subscription) {
    return memEventsDao.addEventSubscription(subscription);
  }

  @Override
  public boolean deleteEventSubscription(UUID id) {
    return memEventsDao.deleteEventSubscription(id);
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
}
