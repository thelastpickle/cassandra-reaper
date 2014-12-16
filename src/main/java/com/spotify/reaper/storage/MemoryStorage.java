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

import com.google.common.collect.Maps;

import com.spotify.reaper.core.Cluster;
import com.spotify.reaper.core.ColumnFamily;
import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.core.RepairSegment;
import com.spotify.reaper.service.RingRange;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implements the StorageAPI using transient Java classes.
 */
public class MemoryStorage implements IStorage {

  private static final AtomicInteger REPAIR_RUN_ID = new AtomicInteger(0);
  private static final AtomicInteger COLUMN_FAMILY_ID = new AtomicInteger(0);
  private static final AtomicInteger SEGMENT_ID = new AtomicInteger(0);

  private ConcurrentMap<String, Cluster> clusters = Maps.newConcurrentMap();
  private ConcurrentMap<Long, RepairRun> repairRuns = Maps.newConcurrentMap();
  private ConcurrentMap<Long, ColumnFamily> columnFamilies = Maps.newConcurrentMap();
  private ConcurrentMap<TableName, ColumnFamily> columnFamiliesByName = Maps.newConcurrentMap();
  private ConcurrentMap<Long, RepairSegment> repairSegments = Maps.newConcurrentMap();
  private ConcurrentMap<Long, LinkedHashMap<Long, RepairSegment>> repairSegmentsByRunId =
      Maps.newConcurrentMap();

  public static class TableName {

    public final String cluster;
    public final String keyspace;
    public final String table;

    public TableName(String cluster, String keyspace, String table) {
      this.cluster = cluster;
      this.keyspace = keyspace;
      this.table = table;
    }

    @Override
    public boolean equals(Object other) {
      if (other instanceof TableName) {
        return
            cluster.equals(((TableName) other).cluster) &&
            keyspace.equals(((TableName) other).keyspace) &&
            table.equals(((TableName) other).table);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return (cluster + keyspace + table).hashCode();
    }
  }

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
  public Cluster addCluster(Cluster cluster) {
    Cluster existing = clusters.put(cluster.getName(), cluster);
    return existing == null ? cluster : null;
  }

  @Override
  public boolean updateCluster(Cluster newCluster) {
    if (getCluster(newCluster.getName()) == null) {
      return false;
    } else {
      clusters.put(newCluster.getName(), newCluster);
      return true;
    }
  }

  @Override
  public Cluster getCluster(String clusterName) {
    return clusters.get(clusterName);
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
  public RepairRun getRepairRun(long id) {
    return repairRuns.get(id);
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
  public ColumnFamily addColumnFamily(ColumnFamily.Builder columnFamily) {
    ColumnFamily existing =
        getColumnFamily(columnFamily.clusterName, columnFamily.keyspaceName, columnFamily.name);
    if (existing == null) {
      ColumnFamily newColumnFamily = columnFamily.build(COLUMN_FAMILY_ID.incrementAndGet());
      columnFamilies.put(newColumnFamily.getId(), newColumnFamily);
      columnFamiliesByName
          .put(new TableName(newColumnFamily.getClusterName(),
                             newColumnFamily.getKeyspaceName(),
                             newColumnFamily.getName()), newColumnFamily);
      return newColumnFamily;
    } else {
      return null;
    }
  }

  @Override
  public ColumnFamily getColumnFamily(long id) {
    return columnFamilies.get(id);
  }

  @Override
  public ColumnFamily getColumnFamily(String cluster, String keyspace, String table) {
    return columnFamiliesByName.get(new TableName(cluster, keyspace, table));
  }

  @Override
  public int addRepairSegments(Collection<RepairSegment.Builder> segments) {
    LinkedHashMap<Long, RepairSegment> newSegments = Maps.newLinkedHashMap();
    for (RepairSegment.Builder segment : segments) {
      RepairSegment newRepairSegment = segment.build(SEGMENT_ID.incrementAndGet());
      repairSegments.put(newRepairSegment.getId(), newRepairSegment);
      newSegments.put(newRepairSegment.getId(), newRepairSegment);
    }
    repairSegmentsByRunId.put(newSegments.values().iterator().next().getRunId(), newSegments);
    return newSegments.size();
  }

  @Override
  public boolean updateRepairSegment(RepairSegment newRepairSegment) {
    if (getRepairSegment(newRepairSegment.getId()) == null) {
      return false;
    } else {
      repairSegments.put(newRepairSegment.getId(), newRepairSegment);
      repairSegmentsByRunId.get(newRepairSegment.getRunId())
          .put(newRepairSegment.getId(), newRepairSegment);
      return true;
    }
  }

  @Override
  public RepairSegment getRepairSegment(long id) {
    return repairSegments.get(id);
  }

  @Override
  public RepairSegment getNextFreeSegment(long runId) {
    for (RepairSegment segment : repairSegmentsByRunId.get(runId).values()) {
      if (segment.getState() == RepairSegment.State.NOT_STARTED) {
        return segment;
      }
    }
    return null;
  }

  @Override
  public RepairSegment getNextFreeSegmentInRange(long runId, RingRange range) {
    for (RepairSegment segment : repairSegmentsByRunId.get(runId).values()) {
      if (segment.getState() == RepairSegment.State.NOT_STARTED &&
          range.encloses(segment.getTokenRange())) {
        return segment;
      }
    }
    return null;
  }

}
