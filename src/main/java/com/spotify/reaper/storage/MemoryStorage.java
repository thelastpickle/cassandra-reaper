package com.spotify.reaper.storage;

import com.google.common.collect.Maps;

import com.spotify.reaper.core.Cluster;
import com.spotify.reaper.core.ColumnFamily;
import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.core.RepairSegment;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implements the StorageAPI using transient Java classes.
 */
public class MemoryStorage implements IStorage {

  private static final AtomicInteger REPAIR_RUN_ID = new AtomicInteger(0);
  private static final AtomicInteger COLUMN_FAMILY_ID = new AtomicInteger(0);

  private Map<String, Cluster> clusters = Maps.newHashMap();
  private Map<Long, RepairRun> repairRuns = Maps.newHashMap();
  private Map<Long, ColumnFamily> columnFamilies = Maps.newHashMap();
  private Map<TableName, ColumnFamily> columnFamiliesByName = Maps.newHashMap();

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
  public Cluster addCluster(Cluster cluster) {
    Cluster existing = clusters.putIfAbsent(cluster.getName(), cluster);
    return existing == null ? cluster : null;
  }

  @Override
  public Cluster updateCluster(Cluster newCluster) {
    clusters.put(newCluster.getName(), newCluster);
    return newCluster;
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
  public RepairRun getRepairRun(long id) {
    return repairRuns.get(id);
  }

  @Override
  public ColumnFamily addColumnFamily(ColumnFamily.Builder columnFamily) {
    ColumnFamily
        existing =
        getColumnFamily(columnFamily.cluster.getName(), columnFamily.keyspaceName,
                        columnFamily.name);
    if (existing == null) {
      ColumnFamily newColumnFamily = columnFamily.build(COLUMN_FAMILY_ID.incrementAndGet());
      columnFamilies.put(newColumnFamily.getId(), newColumnFamily);
      columnFamiliesByName
          .put(new TableName(newColumnFamily.getCluster().getName(),
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
  public boolean addRepairSegments(Collection<RepairSegment.Builder> newSegments) {
    // TODO:
    return false;
  }

  @Override
  public boolean updateRepairSegment(RepairSegment newRepairSegment) {
    // TODO:
    return false;
  }

  @Override
  public RepairSegment getNextFreeSegment(long runId) {
    // TODO:
    return null;
  }

  @Override
  public RepairSegment getNextFreeSegmentInRange(long runId, long start, long end) {
    // TODO:
    return null;
  }
}
