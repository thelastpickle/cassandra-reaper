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
import io.cassandrareaper.storage.repairrun.MemRepairRunDao;
import io.cassandrareaper.storage.repairschedule.MemRepairScheduleDao;
import io.cassandrareaper.storage.repairsegment.MemRepairSegment;
import io.cassandrareaper.storage.repairunit.MemRepairUnitDao;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implements the StorageAPI using transient Java classes.
 */
public final class MemoryStorageFacade implements IStorage {

  private static final Logger LOG = LoggerFactory.getLogger(MemoryStorageFacade.class);
  private final ConcurrentMap<String, Cluster> clusters = Maps.newConcurrentMap();
  private final ConcurrentMap<String, Snapshot> snapshots = Maps.newConcurrentMap();
  private final ConcurrentMap<UUID, DiagEventSubscription> subscriptionsById = Maps.newConcurrentMap();
  private final ConcurrentMap<String, Map<String, PercentRepairedMetric>> percentRepairedMetrics
      = Maps.newConcurrentMap();
  private final MemRepairSegment memRepairSegment = new MemRepairSegment(this);
  private final MemRepairUnitDao memRepairUnitDao = new MemRepairUnitDao();
  private final MemRepairRunDao memRepairRunDao = new MemRepairRunDao(memRepairSegment, memRepairUnitDao);
  private final MemRepairScheduleDao memRepairScheduleDao = new MemRepairScheduleDao(this);

  @Override
  public boolean isStorageConnected() {
    // Just assuming the MemoryStorage is always functional when instantiated.
    return true;
  }

  @Override
  public Collection<Cluster> getClusters() {
    return clusters.values();
  }

  @Override
  public boolean addCluster(Cluster cluster) {
    assert addClusterAssertions(cluster);
    Cluster existing = clusters.put(cluster.getName(), cluster);
    return existing == null;
  }

  @Override
  public boolean updateCluster(Cluster newCluster) {
    addCluster(newCluster);
    return true;
  }


  private boolean addClusterAssertions(Cluster cluster) {
    Preconditions.checkState(
        Cluster.State.UNKNOWN != cluster.getState(),
        "Cluster should not be persisted with UNKNOWN state");

    // TODO – unit tests need to also always set the paritioner
    //Preconditions.checkState(cluster.getPartitioner().isPresent(), "Cannot store cluster with no partitioner.");

    // assert we're not overwriting a cluster with the same name but different node list
    Set<String> previousNodes;
    try {
      previousNodes = getCluster(cluster.getName()).getSeedHosts();
    } catch (IllegalArgumentException ignore) {
      // there is no previous cluster with same name
      previousNodes = cluster.getSeedHosts();
    }
    Set<String> addedNodes = cluster.getSeedHosts();

    Preconditions.checkArgument(
        !Collections.disjoint(previousNodes, addedNodes),
        "Trying to add/update cluster using an existing name: %s. No nodes overlap between %s and %s",
        cluster.getName(), StringUtils.join(previousNodes, ','), StringUtils.join(addedNodes, ','));

    return true;
  }

  @Override
  public Cluster getCluster(String clusterName) {
    Preconditions.checkArgument(clusters.containsKey(clusterName), "no such cluster: %s", clusterName);
    return clusters.get(clusterName);
  }

  @Override
  public Cluster deleteCluster(String clusterName) {
    memRepairScheduleDao.getRepairSchedulesForCluster(clusterName).forEach(
        schedule -> memRepairScheduleDao.deleteRepairSchedule(schedule.getId())
    );
    memRepairRunDao.getRepairRunIdsForCluster(clusterName, Optional.empty())
        .forEach(runId -> memRepairRunDao.deleteRepairRun(runId));

    getEventSubscriptions(clusterName)
        .stream()
        .filter(subscription -> subscription.getId().isPresent())
        .forEach(subscription -> deleteEventSubscription(subscription.getId().get()));

    memRepairUnitDao.repairUnits.values().stream()
        .filter((unit) -> unit.getClusterName().equals(clusterName))
        .forEach((unit) -> {
          assert memRepairRunDao.getRepairRunsForUnit(
              unit.getId()).isEmpty() : StringUtils.join(memRepairRunDao.getRepairRunsForUnit(unit.getId())
          );
          memRepairUnitDao.repairUnits.remove(unit.getId());
          memRepairUnitDao.repairUnitsByKey.remove(unit.with());
        });

    return clusters.remove(clusterName);
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
    List<RepairRunStatus> runStatuses = Lists.newArrayList();
    for (RepairRun run : memRepairRunDao.getRepairRunsForCluster(clusterName, Optional.of(limit))) {
      RepairUnit unit = memRepairUnitDao.getRepairUnit(run.getRepairUnitId());
      int segmentsRepaired = memRepairSegment
          .getSegmentAmountForRepairRunWithState(run.getId(), RepairSegment.State.DONE);
      int totalSegments = memRepairSegment.getSegmentAmountForRepairRun(run.getId());
      runStatuses.add(
          new RepairRunStatus(
              run.getId(),
              clusterName,
              unit.getKeyspaceName(),
              run.getTables(),
              segmentsRepaired,
              totalSegments,
              run.getRunState(),
              run.getStartTime(),
              run.getEndTime(),
              run.getCause(),
              run.getOwner(),
              run.getLastEvent(),
              run.getCreationTime(),
              run.getPauseTime(),
              run.getIntensity(),
              unit.getIncrementalRepair(),
              run.getRepairParallelism(),
              unit.getNodes(),
              unit.getDatacenters(),
              unit.getBlacklistedTables(),
              unit.getRepairThreadCount(),
              unit.getId(),
              unit.getTimeout(),
              run.getAdaptiveSchedule()));
    }
    return runStatuses;
  }

  @Override
  public Collection<RepairScheduleStatus> getClusterScheduleStatuses(String clusterName) {
    List<RepairScheduleStatus> scheduleStatuses = Lists.newArrayList();
    Collection<RepairSchedule> schedules = memRepairScheduleDao.getRepairSchedulesForCluster(clusterName);
    for (RepairSchedule schedule : schedules) {
      RepairUnit unit = memRepairUnitDao.getRepairUnit(schedule.getRepairUnitId());
      scheduleStatuses.add(new RepairScheduleStatus(schedule, unit));
    }
    return scheduleStatuses;
  }

  @Override
  public boolean saveSnapshot(Snapshot snapshot) {
    snapshots.put(snapshot.getClusterName() + "-" + snapshot.getName(), snapshot);
    return true;
  }

  @Override
  public boolean deleteSnapshot(Snapshot snapshot) {
    snapshots.remove(snapshot.getClusterName() + "-" + snapshot.getName());
    return true;
  }

  @Override
  public Snapshot getSnapshot(String clusterName, String snapshotName) {
    Snapshot snapshot = snapshots.get(clusterName + "-" + snapshotName);
    return snapshot;
  }

  @Override
  public Collection<DiagEventSubscription> getEventSubscriptions() {
    return subscriptionsById.values();
  }

  @Override
  public Collection<DiagEventSubscription> getEventSubscriptions(String clusterName) {
    Preconditions.checkNotNull(clusterName);
    Collection<DiagEventSubscription> ret = new ArrayList<>();
    for (DiagEventSubscription sub : subscriptionsById.values()) {
      if (sub.getCluster().equals(clusterName)) {
        ret.add(sub);
      }
    }
    return ret;
  }

  @Override
  public DiagEventSubscription getEventSubscription(UUID id) {
    if (subscriptionsById.containsKey(id)) {
      return subscriptionsById.get(id);
    }
    throw new IllegalArgumentException("No event subscription with id " + id);
  }

  @Override
  public DiagEventSubscription addEventSubscription(DiagEventSubscription subscription) {
    Preconditions.checkArgument(subscription.getId().isPresent());
    subscriptionsById.put(subscription.getId().get(), subscription);
    return subscription;
  }

  @Override
  public boolean deleteEventSubscription(UUID id) {
    return subscriptionsById.remove(id) != null;
  }

  @Override
  public List<PercentRepairedMetric> getPercentRepairedMetrics(String clusterName, UUID repairScheduleId, Long since) {
    return percentRepairedMetrics.entrySet().stream()
        .filter(entry -> entry.getKey().equals(clusterName + "-" + repairScheduleId))
        .map(entry -> entry.getValue().entrySet())
        .flatMap(Collection::stream)
        .map(Entry::getValue)
        .collect(Collectors.toList());
  }

  @Override
  public void storePercentRepairedMetric(PercentRepairedMetric metric) {
    synchronized (this) {
      String metricKey = metric.getCluster() + "-" + metric.getRepairScheduleId();
      Map<String, PercentRepairedMetric> newValue = Maps.newHashMap();
      if (percentRepairedMetrics.containsKey(metricKey)) {
        newValue.putAll(percentRepairedMetrics.get(metricKey));
      }
      newValue.put(metric.getNode(), metric);
      percentRepairedMetrics.put(metricKey, newValue);
    }
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
