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

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.spotify.reaper.core.Cluster;
import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.core.RepairSegment;
import com.spotify.reaper.core.RepairUnit;
import com.spotify.reaper.service.RingRange;

import java.util.ArrayList;
import java.util.Collection;
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

  private ConcurrentMap<String, Cluster> clusters = Maps.newConcurrentMap();
  private ConcurrentMap<Long, RepairRun> repairRuns = Maps.newConcurrentMap();
  private ConcurrentMap<Long, RepairUnit> repairUnits = Maps.newConcurrentMap();
  private ConcurrentMap<RepairUnitKey, RepairUnit> repairUnitsByKey = Maps.newConcurrentMap();
  private ConcurrentMap<Long, RepairSegment> repairSegments = Maps.newConcurrentMap();
  private ConcurrentMap<Long, LinkedHashMap<Long, RepairSegment>> repairSegmentsByRunId =
      Maps.newConcurrentMap();

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
  public RepairRun addRepairRun(RepairRun.Builder repairRun) {
    RepairRun newRepairRun = repairRun.build(REPAIR_RUN_ID.incrementAndGet());
    repairRuns.put(newRepairRun.getId(), newRepairRun);
    return newRepairRun;
  }

  @Override
  public boolean updateRepairRun(RepairRun repairRun) {
    if (getRepairRun(repairRun.getId()) == null) {
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
  public Collection<RepairRun> getRepairRunsWithState(RepairRun.RunState runState) {
    List<RepairRun> foundRepairRuns = new ArrayList<>();
    for (RepairRun repairRun : repairRuns.values()) {
      if (repairRun.getRunState() == runState) {
        foundRepairRuns.add(repairRun);
      }
    }
    return foundRepairRuns;
  }

  @Override
  public RepairUnit addRepairUnit(RepairUnit.Builder repairUnit) {
    Optional<RepairUnit> existing =
        getRepairUnit(repairUnit.clusterName, repairUnit.keyspaceName, repairUnit.columnFamilies);
    if (existing.isPresent()) {
      return existing.get();
    } else {
      RepairUnit newRepairUnit = repairUnit.build(REPAIR_UNIT_ID.incrementAndGet());
      repairUnits.put(newRepairUnit.getId(), newRepairUnit);
      RepairUnitKey unitTables = new RepairUnitKey(newRepairUnit.getClusterName(),
                                                   newRepairUnit.getKeyspaceName(),
                                                   newRepairUnit.getColumnFamilies());
      repairUnitsByKey.put(unitTables, newRepairUnit);
      return newRepairUnit;
    }
  }

  @Override
  public Optional<RepairUnit> getRepairUnit(long id) {
    return Optional.fromNullable(repairUnits.get(id));
  }

  @Override
  public Optional<RepairUnit> getRepairUnit(String cluster, String keyspace, Set<String> tables) {
    return Optional
        .fromNullable(repairUnitsByKey.get(new RepairUnitKey(cluster, keyspace, tables)));
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
  public int getSegmentAmountForRepairRun(long runId, RepairSegment.State state) {
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

  public static class RepairUnitKey {

    public final String cluster;
    public final String keyspace;
    public final Set<String> tables;

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

}
