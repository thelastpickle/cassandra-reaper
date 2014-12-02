package com.spotify.reaper.storage;

import com.google.common.collect.Maps;

import com.spotify.reaper.core.Cluster;
import com.spotify.reaper.core.RepairRun;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Implements the StorageAPI using transient Java classes.
 */
public class MemoryStorage implements IStorage {

  private Map<String, Cluster> clusters = Maps.newHashMap();
  private Map<Long, RepairRun> repairRuns = Maps.newHashMap();


  @Override
  public Cluster addCluster(Cluster cluster) {
    return clusters.put(cluster.getName(), cluster);
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
}
