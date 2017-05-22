/*
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
package com.spotify.reaper.storage;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Collections2;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.spotify.reaper.core.Cluster;
import com.spotify.reaper.core.HostMetrics;
import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.core.RepairSchedule;
import com.spotify.reaper.core.RepairSegment;
import com.spotify.reaper.core.RepairUnit;
import com.spotify.reaper.resources.view.RepairRunStatus;
import com.spotify.reaper.resources.view.RepairScheduleStatus;
import com.spotify.reaper.service.RepairParameters;
import com.spotify.reaper.service.RingRange;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implements the StorageAPI using transient Java classes.
 */
public class MemoryStorage implements IStorage {

  private final AtomicInteger REPAIR_RUN_ID = new AtomicInteger(0);
  private final AtomicInteger REPAIR_UNIT_ID = new AtomicInteger(0);
  private final AtomicInteger SEGMENT_ID = new AtomicInteger(0);
  private final AtomicInteger REPAIR_SCHEDULE_ID = new AtomicInteger(0);

  private final ConcurrentMap<String, Cluster> clusters = Maps.newConcurrentMap();
  private final ConcurrentMap<Long, RepairRun> repairRuns = Maps.newConcurrentMap();
  private final ConcurrentMap<Long, RepairUnit> repairUnits = Maps.newConcurrentMap();
  private final ConcurrentMap<RepairUnitKey, RepairUnit> repairUnitsByKey = Maps.newConcurrentMap();
  private final ConcurrentMap<Long, RepairSegment> repairSegments = Maps.newConcurrentMap();
  private final ConcurrentMap<Long, LinkedHashMap<Long, RepairSegment>> repairSegmentsByRunId =
      Maps.newConcurrentMap();
  private final ConcurrentMap<Long, RepairSchedule> repairSchedules = Maps.newConcurrentMap();

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
    return Optional.fromNullable(clusters.get(clusterName));
  }

  @Override
  public Optional<Cluster> deleteCluster(String clusterName) {
    if (getRepairSchedulesForCluster(clusterName).isEmpty()
        && getRepairRunsForCluster(clusterName).isEmpty()) {
      return Optional.fromNullable(clusters.remove(clusterName));
    }
    return Optional.absent();
  }

  @Override
  public RepairRun addRepairRun(RepairRun.Builder repairRun) {
    RepairRun newRepairRun = repairRun.build(REPAIR_RUN_ID.incrementAndGet());
    repairRuns.put(newRepairRun.getId(), newRepairRun);
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
  public Optional<RepairRun> getRepairRun(long id) {
    return Optional.fromNullable(repairRuns.get(id));
  }

  @Override
  public List<RepairRun> getRepairRunsForCluster(String clusterName) {
    List<RepairRun> foundRepairRuns = new ArrayList<>();
    for (RepairRun repairRun : repairRuns.values()) {
      if (repairRun.getClusterName().equalsIgnoreCase(clusterName)) {
        foundRepairRuns.add(repairRun);
      }
    }
    return foundRepairRuns;
  }

  @Override
  public Collection<RepairRun> getRepairRunsForUnit(long repairUnitId) {
    List<RepairRun> foundRepairRuns = new ArrayList<>();
    for (RepairRun repairRun : repairRuns.values()) {
      if (repairRun.getRepairUnitId() == repairUnitId) {
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
  private Optional<RepairUnit> deleteRepairUnit(long repairUnitId) {
    RepairUnit deletedUnit = null;
    boolean canDelete = true;
    for (RepairRun repairRun : repairRuns.values()) {
      if (repairRun.getRepairUnitId() == repairUnitId) {
        canDelete = false;
        break;
      }
    }
    if (canDelete) {
      for (RepairSchedule schedule : repairSchedules.values()) {
        if (schedule.getRepairUnitId() == repairUnitId) {
          canDelete = false;
          break;
        }
      }
    }
    if (canDelete) {
      deletedUnit = repairUnits.remove(repairUnitId);
      repairUnitsByKey.remove(new RepairUnitKey(deletedUnit));
    }
    return Optional.fromNullable(deletedUnit);
  }

  private int deleteRepairSegmentsForRun(long runId) {
    Map<Long, RepairSegment> segmentsMap = repairSegmentsByRunId.remove(runId);
    if (null != segmentsMap) {
      for (RepairSegment segment : segmentsMap.values()) {
        repairSegments.remove(segment.getId());
      }
    }
    return segmentsMap != null ? segmentsMap.size() : 0;
  }

  @Override
  public Optional<RepairRun> deleteRepairRun(long id) {
    RepairRun deletedRun = repairRuns.remove(id);
    if (deletedRun != null) {
      if (getSegmentAmountForRepairRunWithState(id, RepairSegment.State.RUNNING) == 0) {
        deleteRepairUnit(deletedRun.getRepairUnitId());
        deleteRepairSegmentsForRun(id);
        deletedRun = deletedRun.with().runState(RepairRun.RunState.DELETED).build(id);
      }
    }
    return Optional.fromNullable(deletedRun);
  }

  @Override
  public RepairUnit addRepairUnit(RepairUnit.Builder repairUnit) {
    Optional<RepairUnit> existing =
        getRepairUnit(repairUnit.clusterName, repairUnit.keyspaceName, repairUnit.columnFamilies);
    if (existing.isPresent() && repairUnit.incrementalRepair == existing.get().getIncrementalRepair().booleanValue()) {
      return existing.get();
    } else {
      RepairUnit newRepairUnit = repairUnit.build(REPAIR_UNIT_ID.incrementAndGet());
      repairUnits.put(newRepairUnit.getId(), newRepairUnit);
      RepairUnitKey unitKey = new RepairUnitKey(newRepairUnit);
      repairUnitsByKey.put(unitKey, newRepairUnit);
      return newRepairUnit;
    }
  }

  @Override
  public Optional<RepairUnit> getRepairUnit(long id) {
    return Optional.fromNullable(repairUnits.get(id));
  }

  @Override
  public Optional<RepairUnit> getRepairUnit(String cluster, String keyspace, Set<String> tables) {
    return Optional.fromNullable(
        repairUnitsByKey.get(new RepairUnitKey(cluster, keyspace, tables)));
  }

  @Override
  public void addRepairSegments(Collection<RepairSegment.Builder> segments, long runId) {
    LinkedHashMap<Long, RepairSegment> newSegments = Maps.newLinkedHashMap();
    for (RepairSegment.Builder segment : segments) {
      RepairSegment newRepairSegment = segment.build(SEGMENT_ID.incrementAndGet());
      repairSegments.put(newRepairSegment.getId(), newRepairSegment);
      newSegments.put(newRepairSegment.getId(), newRepairSegment);
    }
    repairSegmentsByRunId.put(runId, newSegments);
  }

  @Override
  public boolean updateRepairSegment(RepairSegment newRepairSegment) {
    if (getRepairSegment(newRepairSegment.getId()) == null) {
      return false;
    } else {
      repairSegments.put(newRepairSegment.getId(), newRepairSegment);
      LinkedHashMap<Long, RepairSegment> updatedSegment =
          repairSegmentsByRunId.get(newRepairSegment.getRunId());
      updatedSegment.put(newRepairSegment.getId(), newRepairSegment);
      return true;
    }
  }

  @Override
  public Optional<RepairSegment> getRepairSegment(long id) {
    return Optional.fromNullable(repairSegments.get(id));
  }

  @Override
  public Collection<RepairSegment> getRepairSegmentsForRun(long runId) {
    return repairSegmentsByRunId.get(runId).values();
  }

  @Override
  public Optional<RepairSegment> getNextFreeSegment(long runId) {
    for (RepairSegment segment : repairSegmentsByRunId.get(runId).values()) {
      if (segment.getState() == RepairSegment.State.NOT_STARTED) {
        return Optional.of(segment);
      }
    }
    return Optional.absent();
  }

  @Override
  public Optional<RepairSegment> getNextFreeSegmentInRange(long runId, RingRange range) {
    for (RepairSegment segment : repairSegmentsByRunId.get(runId).values()) {
      if (segment.getState() == RepairSegment.State.NOT_STARTED &&
          range.encloses(segment.getTokenRange())) {
        return Optional.of(segment);
      }
    }
    return Optional.absent();
  }

  @Override
  public Collection<RepairSegment> getSegmentsWithState(long runId,
      RepairSegment.State segmentState) {
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
        RepairUnit unit = getRepairUnit(segment.getRepairUnitId()).get();
        ongoingRepairs.add(new RepairParameters(
            segment.getTokenRange(),
            unit.getKeyspaceName(),
            unit.getColumnFamilies(),
            run.getRepairParallelism()));
      }
    }
    return ongoingRepairs;
  }

  @Override
  public Collection<Long> getRepairRunIdsForCluster(String clusterName) {
    Collection<Long> repairRunIds = new HashSet<>();
    for (RepairRun repairRun : repairRuns.values()) {
      if (repairRun.getClusterName().equalsIgnoreCase(clusterName)) {
        repairRunIds.add(repairRun.getId());
      }
    }
    return repairRunIds;
  }

  @Override
  public int getSegmentAmountForRepairRun(long runId) {
    Map<Long, RepairSegment> segmentsMap = repairSegmentsByRunId.get(runId);
    return segmentsMap == null ? 0 : segmentsMap.size();
  }

  @Override
  public int getSegmentAmountForRepairRunWithState(long runId, RepairSegment.State state) {
    Map<Long, RepairSegment> segmentsMap = repairSegmentsByRunId.get(runId);
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
    RepairSchedule newRepairSchedule = repairSchedule.build(REPAIR_SCHEDULE_ID.incrementAndGet());
    repairSchedules.put(newRepairSchedule.getId(), newRepairSchedule);
    return newRepairSchedule;
  }

  @Override
  public Optional<RepairSchedule> getRepairSchedule(long id) {
    return Optional.fromNullable(repairSchedules.get(id));
  }

  @Override
  public Collection<RepairSchedule> getRepairSchedulesForCluster(String clusterName) {
    Collection<RepairSchedule> foundRepairSchedules = new ArrayList<>();
    for (RepairSchedule repairSchedule : repairSchedules.values()) {
      RepairUnit repairUnit = getRepairUnit(repairSchedule.getRepairUnitId()).get();
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
      RepairUnit repairUnit = getRepairUnit(repairSchedule.getRepairUnitId()).get();
      if (repairUnit.getKeyspaceName().equals(keyspaceName)) {
        foundRepairSchedules.add(repairSchedule);
      }
    }
    return foundRepairSchedules;
  }

  @Override
  public Collection<RepairSchedule> getRepairSchedulesForClusterAndKeyspace(String clusterName,
      String keyspaceName) {
    Collection<RepairSchedule> foundRepairSchedules = new ArrayList<>();
    for (RepairSchedule repairSchedule : repairSchedules.values()) {
      RepairUnit repairUnit = getRepairUnit(repairSchedule.getRepairUnitId()).get();
      if (repairUnit.getClusterName().equals(clusterName) && repairUnit.getKeyspaceName()
          .equals(keyspaceName)) {
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
  public Optional<RepairSchedule> deleteRepairSchedule(long id) {
    RepairSchedule deletedSchedule = repairSchedules.remove(id);
    if (deletedSchedule != null) {
      deletedSchedule = deletedSchedule.with().state(RepairSchedule.State.DELETED).build(id);
    }
    return Optional.fromNullable(deletedSchedule);
  }

  @Override
  public Collection<RepairRunStatus> getClusterRunStatuses(String clusterName, int limit) {
    Optional<Cluster> cluster = getCluster(clusterName);
    if (!cluster.isPresent()) {
      return Collections.emptyList();
    } else {
      List<RepairRunStatus> runStatuses = Lists.newArrayList();
      List<RepairRun> runs = getRepairRunsForCluster(clusterName);
      Collections.sort(runs);
      for (RepairRun run : Iterables.limit(runs, limit)) {
        RepairUnit unit = getRepairUnit(run.getRepairUnitId()).get();
        int segmentsRepaired =
            getSegmentAmountForRepairRunWithState(run.getId(), RepairSegment.State.DONE);
        int totalSegments = getSegmentAmountForRepairRun(run.getId());
        runStatuses.add(new RepairRunStatus(
            run.getId(), clusterName, unit.getKeyspaceName(), unit.getColumnFamilies(),
            segmentsRepaired, totalSegments, run.getRunState(), run.getStartTime(),
            run.getEndTime(), run.getCause(), run.getOwner(), run.getLastEvent(),
            run.getCreationTime(), run.getPauseTime(), run.getIntensity(), unit.getIncrementalRepair(),
            run.getRepairParallelism()));
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
        RepairUnit unit = getRepairUnit(schedule.getRepairUnitId()).get();
        scheduleStatuses.add(new RepairScheduleStatus(schedule, unit));
      }
      return scheduleStatuses;
    }
  }

  public static class RepairUnitKey {

    public final String cluster;
    public final String keyspace;
    public final Set<String> tables;

    public RepairUnitKey(RepairUnit unit) {
      this(unit.getClusterName(), unit.getKeyspaceName(), unit.getColumnFamilies());
    }

    public RepairUnitKey(String cluster, String keyspace, Set<String> tables) {
      this.cluster = cluster;
      this.keyspace = keyspace;
      this.tables = tables;
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof RepairUnitKey &&
             cluster.equals(((RepairUnitKey) other).cluster) &&
             keyspace.equals(((RepairUnitKey) other).keyspace) &&
             tables.equals(((RepairUnitKey) other).tables);
    }

    @Override
    public int hashCode() {
      return cluster.hashCode() ^ keyspace.hashCode() ^ tables.hashCode();
    }
  }

  @Override
  public boolean takeLeadOnSegment(long segmentId) {
    return true;
  }

  @Override
  public boolean renewLeadOnSegment(long segmentId) {
    return true;
  }

  @Override
  public void releaseLeadOnSegment(long segmentId) {
    // Fault tolerance is not supported with this storage backend
  }

  @Override
  public void storeHostMetrics(HostMetrics hostMetrics) {
    // Fault tolerance is not supported with this storage backend
  }

  @Override
  public Optional<HostMetrics> getHostMetrics(String hostName) {
    return Optional.absent();
  }

  @Override
  public StorageType getStorageType() {
    return StorageType.MEMORY;
  }

  @Override
  public int countRunningReapers() {
    return 1;
  }

  @Override
  public void saveHeartbeat() {
    // Fault tolerance is not supported with this storage backend
  }

  @Override
  public Collection<RepairSegment> getRepairSegmentsForRunInLocalMode(long runId, List<RingRange> localRanges) {
    throw new UnsupportedOperationException("Cannot run local mode with memory storage");
  }
}
