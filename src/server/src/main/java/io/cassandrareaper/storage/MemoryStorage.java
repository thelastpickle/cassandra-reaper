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
import io.cassandrareaper.service.RepairParameters;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import com.datastax.driver.core.utils.UUIDs;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements the StorageAPI using transient Java classes.
 */
public final class MemoryStorage implements IStorage {

  private static final Logger LOG = LoggerFactory.getLogger(MemoryStorage.class);
  private final ConcurrentMap<String, Cluster> clusters = Maps.newConcurrentMap();
  private final ConcurrentMap<UUID, RepairRun> repairRuns = Maps.newConcurrentMap();
  private final ConcurrentMap<UUID, RepairUnit> repairUnits = Maps.newConcurrentMap();
  private final ConcurrentMap<RepairUnit.Builder, RepairUnit> repairUnitsByKey = Maps.newConcurrentMap();
  private final ConcurrentMap<UUID, RepairSegment> repairSegments = Maps.newConcurrentMap();
  private final ConcurrentMap<UUID, LinkedHashMap<UUID, RepairSegment>> repairSegmentsByRunId = Maps.newConcurrentMap();
  private final ConcurrentMap<UUID, RepairSchedule> repairSchedules = Maps.newConcurrentMap();
  private final ConcurrentMap<String, Snapshot> snapshots = Maps.newConcurrentMap();
  private final ConcurrentMap<UUID, DiagEventSubscription> subscriptionsById = Maps.newConcurrentMap();
  private final ConcurrentMap<String, Map<String, PercentRepairedMetric>> percentRepairedMetrics
      = Maps.newConcurrentMap();

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
    getRepairSchedulesForCluster(clusterName).forEach(schedule -> deleteRepairSchedule(schedule.getId()));
    getRepairRunIdsForCluster(clusterName, Optional.empty()).forEach(runId -> deleteRepairRun(runId));

    getEventSubscriptions(clusterName)
        .stream()
        .filter(subscription -> subscription.getId().isPresent())
        .forEach(subscription -> deleteEventSubscription(subscription.getId().get()));

    repairUnits.values().stream()
        .filter((unit) -> unit.getClusterName().equals(clusterName))
        .forEach((unit) -> {
          assert getRepairRunsForUnit(unit.getId()).isEmpty() : StringUtils.join(getRepairRunsForUnit(unit.getId()));
          repairUnits.remove(unit.getId());
          repairUnitsByKey.remove(unit.with());
        });

    return clusters.remove(clusterName);
  }

  @Override
  public RepairRun addRepairRun(RepairRun.Builder repairRun, Collection<RepairSegment.Builder> newSegments) {
    RepairRun newRepairRun = repairRun.build(UUIDs.timeBased());
    repairRuns.put(newRepairRun.getId(), newRepairRun);
    addRepairSegments(newSegments, newRepairRun.getId());
    return newRepairRun;
  }

  @Override
  public boolean updateRepairRun(RepairRun repairRun) {
    return updateRepairRun(repairRun, Optional.of(true));
  }

  @Override
  public boolean updateRepairRun(RepairRun repairRun, Optional<Boolean> updateRepairState) {
    if (!getRepairRun(repairRun.getId()).isPresent()) {
      return false;
    } else {
      repairRuns.put(repairRun.getId(), repairRun);
      return true;
    }
  }

  @Override
  public Optional<RepairRun> getRepairRun(UUID id) {
    return Optional.ofNullable(repairRuns.get(id));
  }

  @Override
  public List<RepairRun> getRepairRunsForCluster(String clusterName, Optional<Integer> limit) {
    List<RepairRun> foundRepairRuns = new ArrayList<>();
    TreeMap<UUID,RepairRun> reverseOrder = new TreeMap<>(Collections.reverseOrder());
    reverseOrder.putAll(repairRuns);
    for (RepairRun repairRun : reverseOrder.values()) {
      if (repairRun.getClusterName().equalsIgnoreCase(clusterName)) {
        foundRepairRuns.add(repairRun);
        if (foundRepairRuns.size() == limit.orElse(1000)) {
          break;
        }
      }
    }
    return foundRepairRuns;
  }

  @Override
  public Collection<RepairRun> getRepairRunsForUnit(UUID repairUnitId) {
    List<RepairRun> foundRepairRuns = new ArrayList<>();
    for (RepairRun repairRun : repairRuns.values()) {
      if (repairRun.getRepairUnitId().equals(repairUnitId)) {
        foundRepairRuns.add(repairRun);
      }
    }
    return foundRepairRuns;
  }

  @Override
  public Collection<RepairRun> getRepairRunsWithState(RepairRun.RunState runState) {
    List<RepairRun> foundRepairRuns = new ArrayList<>();
    for (RepairRun repairRun : repairRuns.values()) {
      if (repairRun.getRunState() == runState) {
        foundRepairRuns.add(repairRun);
      }
    }
    return foundRepairRuns;
  }

  /**
   * Delete a RepairUnit instance from Storage, but only if no run or schedule is referencing it.
   *
   * @param repairUnitId The RepairUnit instance id to delete.
   * @return The deleted RepairUnit instance, if delete succeeded.
   */
  private Optional<RepairUnit> deleteRepairUnit(UUID repairUnitId) {
    RepairUnit deletedUnit = null;
    boolean canDelete = true;
    for (RepairRun repairRun : repairRuns.values()) {
      if (repairRun.getRepairUnitId().equals(repairUnitId)) {
        canDelete = false;
        break;
      }
    }
    if (canDelete) {
      for (RepairSchedule schedule : repairSchedules.values()) {
        if (schedule.getRepairUnitId().equals(repairUnitId)) {
          canDelete = false;
          break;
        }
      }
    }
    if (canDelete) {
      deletedUnit = repairUnits.remove(repairUnitId);
      repairUnitsByKey.remove(deletedUnit.with());
    }
    return Optional.ofNullable(deletedUnit);
  }

  private int deleteRepairSegmentsForRun(UUID runId) {
    Map<UUID, RepairSegment> segmentsMap = repairSegmentsByRunId.remove(runId);
    if (null != segmentsMap) {
      for (RepairSegment segment : segmentsMap.values()) {
        repairSegments.remove(segment.getId());
      }
    }
    return segmentsMap != null ? segmentsMap.size() : 0;
  }

  @Override
  public Optional<RepairRun> deleteRepairRun(UUID id) {
    RepairRun deletedRun = repairRuns.remove(id);
    if (deletedRun != null) {
      if (getSegmentAmountForRepairRunWithState(id, RepairSegment.State.RUNNING) == 0) {
        deleteRepairUnit(deletedRun.getRepairUnitId());
        deleteRepairSegmentsForRun(id);

        deletedRun = deletedRun.with()
            .runState(RepairRun.RunState.DELETED)
            .endTime(DateTime.now())
            .build(id);
      }
    }
    return Optional.ofNullable(deletedRun);
  }

  @Override
  public RepairUnit addRepairUnit(RepairUnit.Builder repairUnit) {
    Optional<RepairUnit> existing = getRepairUnit(repairUnit);
    if (existing.isPresent() && repairUnit.incrementalRepair == existing.get().getIncrementalRepair()) {
      return existing.get();
    } else {
      RepairUnit newRepairUnit = repairUnit.build(UUIDs.timeBased());
      repairUnits.put(newRepairUnit.getId(), newRepairUnit);
      repairUnitsByKey.put(repairUnit, newRepairUnit);
      return newRepairUnit;
    }
  }

  @Override
  public void updateRepairUnit(RepairUnit updatedRepairUnit) {
    repairUnits.put(updatedRepairUnit.getId(), updatedRepairUnit);
    repairUnitsByKey.put(updatedRepairUnit.with(), updatedRepairUnit);
  }

  @Override
  public RepairUnit getRepairUnit(UUID id) {
    RepairUnit unit = repairUnits.get(id);
    Preconditions.checkArgument(null != unit);
    return unit;
  }

  @Override
  public Optional<RepairUnit> getRepairUnit(RepairUnit.Builder params) {
    return Optional.ofNullable(repairUnitsByKey.get(params));
  }

  private void addRepairSegments(Collection<RepairSegment.Builder> segments, UUID runId) {
    LinkedHashMap<UUID, RepairSegment> newSegments = Maps.newLinkedHashMap();
    for (RepairSegment.Builder segment : segments) {
      RepairSegment newRepairSegment = segment.withRunId(runId).withId(UUIDs.timeBased()).build();
      repairSegments.put(newRepairSegment.getId(), newRepairSegment);
      newSegments.put(newRepairSegment.getId(), newRepairSegment);
    }
    repairSegmentsByRunId.put(runId, newSegments);
  }

  @Override
  public boolean updateRepairSegment(RepairSegment newRepairSegment) {
    if (getRepairSegment(newRepairSegment.getRunId(), newRepairSegment.getId()) == null) {
      return false;
    } else {
      repairSegments.put(newRepairSegment.getId(), newRepairSegment);
      LinkedHashMap<UUID, RepairSegment> updatedSegment = repairSegmentsByRunId.get(newRepairSegment.getRunId());
      updatedSegment.put(newRepairSegment.getId(), newRepairSegment);
      return true;
    }
  }

  @Override
  public Optional<RepairSegment> getRepairSegment(UUID runId, UUID segmentId) {
    return Optional.ofNullable(repairSegments.get(segmentId));
  }

  @Override
  public Collection<RepairSegment> getRepairSegmentsForRun(UUID runId) {
    return repairSegmentsByRunId.get(runId).values();
  }

  @Override
  public List<RepairSegment> getNextFreeSegments(UUID runId) {
    return repairSegmentsByRunId.get(runId).values().stream()
                                                    .filter(seg -> seg.getState() == RepairSegment.State.NOT_STARTED)
                                                    .collect(Collectors.toList());
  }

  @Override
  public Collection<RepairSegment> getSegmentsWithState(UUID runId, RepairSegment.State segmentState) {
    List<RepairSegment> segments = Lists.newArrayList();
    for (RepairSegment segment : repairSegmentsByRunId.get(runId).values()) {
      if (segment.getState() == segmentState) {
        segments.add(segment);
      }
    }
    return segments;
  }

  @Override
  public Collection<RepairParameters> getOngoingRepairsInCluster(String clusterName) {
    List<RepairParameters> ongoingRepairs = Lists.newArrayList();
    for (RepairRun run : getRepairRunsWithState(RepairRun.RunState.RUNNING)) {
      for (RepairSegment segment : getSegmentsWithState(run.getId(), RepairSegment.State.RUNNING)) {
        RepairUnit unit = getRepairUnit(segment.getRepairUnitId());
        ongoingRepairs.add(
            new RepairParameters(
                segment.getTokenRange(), unit.getKeyspaceName(), unit.getColumnFamilies(), run.getRepairParallelism()));
      }
    }
    return ongoingRepairs;
  }

  @Override
  public SortedSet<UUID> getRepairRunIdsForCluster(String clusterName, Optional<Integer> limit) {
    SortedSet<UUID> repairRunIds = Sets.newTreeSet((u0, u1) -> (int)(u0.timestamp() - u1.timestamp()));
    for (RepairRun repairRun : repairRuns.values()) {
      if (repairRun.getClusterName().equalsIgnoreCase(clusterName)) {
        repairRunIds.add(repairRun.getId());
      }
    }
    return repairRunIds;
  }

  @Override
  public int getSegmentAmountForRepairRun(UUID runId) {
    Map<UUID, RepairSegment> segmentsMap = repairSegmentsByRunId.get(runId);
    return segmentsMap == null ? 0 : segmentsMap.size();
  }

  @Override
  public int getSegmentAmountForRepairRunWithState(UUID runId, RepairSegment.State state) {
    Map<UUID, RepairSegment> segmentsMap = repairSegmentsByRunId.get(runId);
    int amount = 0;
    if (null != segmentsMap) {
      for (RepairSegment segment : segmentsMap.values()) {
        if (segment.getState() == state) {
          amount += 1;
        }
      }
    }
    return amount;
  }

  @Override
  public RepairSchedule addRepairSchedule(RepairSchedule.Builder repairSchedule) {
    RepairSchedule newRepairSchedule = repairSchedule.build(UUIDs.timeBased());
    repairSchedules.put(newRepairSchedule.getId(), newRepairSchedule);
    return newRepairSchedule;
  }

  @Override
  public Optional<RepairSchedule> getRepairSchedule(UUID id) {
    return Optional.ofNullable(repairSchedules.get(id));
  }

  @Override
  public Collection<RepairSchedule> getRepairSchedulesForCluster(String clusterName) {
    Collection<RepairSchedule> foundRepairSchedules = new ArrayList<>();
    for (RepairSchedule repairSchedule : repairSchedules.values()) {
      RepairUnit repairUnit = getRepairUnit(repairSchedule.getRepairUnitId());
      if (repairUnit.getClusterName().equals(clusterName)) {
        foundRepairSchedules.add(repairSchedule);
      }
    }
    return foundRepairSchedules;
  }

  @Override
  public Collection<RepairSchedule> getRepairSchedulesForCluster(String clusterName, boolean incremental) {
    return getRepairSchedulesForCluster(clusterName).stream()
        .filter(schedule -> getRepairUnit(schedule.getRepairUnitId()).getIncrementalRepair() == incremental)
        .collect(Collectors.toList());
  }

  @Override
  public Collection<RepairSchedule> getRepairSchedulesForKeyspace(String keyspaceName) {
    Collection<RepairSchedule> foundRepairSchedules = new ArrayList<>();
    for (RepairSchedule repairSchedule : repairSchedules.values()) {
      RepairUnit repairUnit = getRepairUnit(repairSchedule.getRepairUnitId());
      if (repairUnit.getKeyspaceName().equals(keyspaceName)) {
        foundRepairSchedules.add(repairSchedule);
      }
    }
    return foundRepairSchedules;
  }

  @Override
  public Collection<RepairSchedule> getRepairSchedulesForClusterAndKeyspace(String clusterName, String keyspaceName) {
    Collection<RepairSchedule> foundRepairSchedules = new ArrayList<>();
    for (RepairSchedule repairSchedule : repairSchedules.values()) {
      RepairUnit repairUnit = getRepairUnit(repairSchedule.getRepairUnitId());
      if (repairUnit.getClusterName().equals(clusterName) && repairUnit.getKeyspaceName().equals(keyspaceName)) {
        foundRepairSchedules.add(repairSchedule);
      }
    }
    return foundRepairSchedules;
  }

  @Override
  public Collection<RepairSchedule> getAllRepairSchedules() {
    return repairSchedules.values();
  }

  @Override
  public boolean updateRepairSchedule(RepairSchedule newRepairSchedule) {
    if (repairSchedules.get(newRepairSchedule.getId()) == null) {
      return false;
    } else {
      repairSchedules.put(newRepairSchedule.getId(), newRepairSchedule);
      return true;
    }
  }

  @Override
  public Optional<RepairSchedule> deleteRepairSchedule(UUID id) {
    RepairSchedule deletedSchedule = repairSchedules.remove(id);
    if (deletedSchedule != null) {
      deletedSchedule = deletedSchedule.with().state(RepairSchedule.State.DELETED).build(id);
    }
    return Optional.ofNullable(deletedSchedule);
  }

  @Override
  public Collection<RepairRunStatus> getClusterRunStatuses(String clusterName, int limit) {
    List<RepairRunStatus> runStatuses = Lists.newArrayList();
    for (RepairRun run : getRepairRunsForCluster(clusterName, Optional.of(limit))) {
      RepairUnit unit = getRepairUnit(run.getRepairUnitId());
      int segmentsRepaired = getSegmentAmountForRepairRunWithState(run.getId(), RepairSegment.State.DONE);
      int totalSegments = getSegmentAmountForRepairRun(run.getId());
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
    Collection<RepairSchedule> schedules = getRepairSchedulesForCluster(clusterName);
    for (RepairSchedule schedule : schedules) {
      RepairUnit unit = getRepairUnit(schedule.getRepairUnitId());
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
  public List<PercentRepairedMetric> getPercentRepairedMetrics(String clusterName, UUID repairScheduleId, long since) {
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
}
