/*
 * Copyright 2014-2017 Spotify AB
 * Copyright 2016-2018 The Last Pickle Ltd
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
import io.cassandrareaper.core.RepairRun;
import io.cassandrareaper.core.RepairSchedule;
import io.cassandrareaper.core.RepairSegment;
import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.core.Snapshot;
import io.cassandrareaper.resources.view.RepairRunStatus;
import io.cassandrareaper.resources.view.RepairScheduleStatus;
import io.cassandrareaper.service.RepairParameters;
import io.cassandrareaper.service.RingRange;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

import com.datastax.driver.core.utils.UUIDs;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;

/**
 * Implements the StorageAPI using transient Java classes.
 */
public final class MemoryStorage implements IStorage {

  private final ConcurrentMap<String, Cluster> clusters = Maps.newConcurrentMap();
  private final ConcurrentMap<UUID, RepairRun> repairRuns = Maps.newConcurrentMap();
  private final ConcurrentMap<UUID, RepairUnit> repairUnits = Maps.newConcurrentMap();
  private final ConcurrentMap<RepairUnit.Builder, RepairUnit> repairUnitsByKey = Maps.newConcurrentMap();
  private final ConcurrentMap<UUID, RepairSegment> repairSegments = Maps.newConcurrentMap();
  private final ConcurrentMap<UUID, LinkedHashMap<UUID, RepairSegment>> repairSegmentsByRunId = Maps.newConcurrentMap();
  private final ConcurrentMap<UUID, RepairSchedule> repairSchedules = Maps.newConcurrentMap();
  private final ConcurrentMap<String, Snapshot> snapshots = Maps.newConcurrentMap();

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
    Cluster existing = clusters.putIfAbsent(cluster.getName(), cluster);
    return existing == null;
  }

  @Override
  public boolean updateCluster(Cluster newCluster) {
    if (!getCluster(newCluster.getName()).isPresent()) {
      return false;
    } else {
      clusters.put(newCluster.getName(), newCluster);
      return true;
    }
  }

  @Override
  public Optional<Cluster> getCluster(String clusterName) {
    return Optional.ofNullable(clusters.get(clusterName));
  }

  @Override
  public Optional<Cluster> deleteCluster(String clusterName) {
    assert getRepairSchedulesForCluster(clusterName).isEmpty()
        : StringUtils.join(getRepairSchedulesForCluster(clusterName));

    assert getRepairRunsForCluster(clusterName, Optional.of(Integer.MAX_VALUE)).isEmpty()
        : StringUtils.join(getRepairRunsForCluster(clusterName, Optional.of(Integer.MAX_VALUE)));

    if (getRepairSchedulesForCluster(clusterName).isEmpty()
        && getRepairRunsForCluster(clusterName, Optional.of(Integer.MAX_VALUE)).isEmpty()) {

      repairUnits.values().stream()
          .filter((unit) -> unit.getClusterName().equals(clusterName))
          .forEach((unit) -> {
            assert getRepairRunsForUnit(unit.getId()).isEmpty() : StringUtils.join(getRepairRunsForUnit(unit.getId()));
            repairUnits.remove(unit.getId());
            repairUnitsByKey.remove(unit.with());
          });

      return Optional.ofNullable(clusters.remove(clusterName));
    }
    return Optional.empty();
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
        deletedRun = deletedRun.with().runState(RepairRun.RunState.DELETED).build(id);
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

  private Optional<RepairSegment> getNextFreeSegment(UUID runId) {
    for (RepairSegment segment : repairSegmentsByRunId.get(runId).values()) {
      if (segment.getState() == RepairSegment.State.NOT_STARTED) {
        return Optional.of(segment);
      }
    }
    return Optional.empty();
  }

  @Override
  public Optional<RepairSegment> getNextFreeSegmentInRange(UUID runId, Optional<RingRange> range) {
    if (range.isPresent()) {
      for (RepairSegment segment : repairSegmentsByRunId.get(runId).values()) {
        if (segment.getState() == RepairSegment.State.NOT_STARTED
            && range.get().encloses(segment.getTokenRange().getBaseRange())) {
          return Optional.of(segment);
        }
      }
    } else {
      return getNextFreeSegment(runId);
    }
    return Optional.empty();
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
  public SortedSet<UUID> getRepairRunIdsForCluster(String clusterName) {
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
    Optional<Cluster> cluster = getCluster(clusterName);
    if (!cluster.isPresent()) {
      return Collections.emptyList();
    } else {
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
                unit.getColumnFamilies(),
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
                run.getActiveTime(),
                run.getInactiveTime())
        );
      }
      return runStatuses;
    }
  }

  @Override
  public Collection<RepairScheduleStatus> getClusterScheduleStatuses(String clusterName) {
    Optional<Cluster> cluster = getCluster(clusterName);
    if (!cluster.isPresent()) {
      return Collections.emptyList();
    } else {
      List<RepairScheduleStatus> scheduleStatuses = Lists.newArrayList();
      Collection<RepairSchedule> schedules = getRepairSchedulesForCluster(clusterName);
      for (RepairSchedule schedule : schedules) {
        RepairUnit unit = getRepairUnit(schedule.getRepairUnitId());
        scheduleStatuses.add(new RepairScheduleStatus(schedule, unit));
      }
      return scheduleStatuses;
    }
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
}
