package com.spotify.reaper.storage;

import com.google.common.collect.Maps;

import com.spotify.reaper.core.Cluster;
import com.spotify.reaper.core.ColumnFamily;
import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.core.RepairSegment;

import java.util.Map;

/**
 * Implements the StorageAPI using transient Java classes.
 */
public class MemoryStorage implements IStorage {

  private Map<String, Cluster> clusters = Maps.newHashMap();
  private Map<Long, RepairRun> repairRuns = Maps.newHashMap();
  private Map<Long, ColumnFamily> columnFamilies = Maps.newHashMap();

  @Override
  public Cluster addCluster(Cluster cluster) {
    return clusters.put(cluster.getName(), cluster);
  }

  @Override
  public Cluster updateCluster(Cluster cluster) {
    return addCluster(cluster);
  }

  @Override
  public Cluster getCluster(String clusterName) {
    return clusters.get(clusterName);
  }

  @Override
  public RepairRun addRepairRun(RepairRun repairRun) {
    return repairRuns.put(repairRun.getId(), repairRun);
  }

  @Override
  public RepairRun getRepairRun(long id) {
    return repairRuns.get(id);
  }

  @Override
  public boolean addColumnFamily(ColumnFamily columnFamily) {
    columnFamilies.put(columnFamily.getId(), columnFamily);
    return true;
  }

  @Override
  public ColumnFamily getColumnFamily(long id) {
    return columnFamilies.get(id);
  }

  @Override
  public RepairSegment getNextFreeSegment(long runId) {
    // TODO:
    return null;
  }
}
