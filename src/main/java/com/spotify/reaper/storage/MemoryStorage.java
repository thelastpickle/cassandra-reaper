package com.spotify.reaper.storage;

import com.spotify.reaper.core.Cluster;
import com.spotify.reaper.core.RepairRun;

/**
 * Implements the StorageAPI using transient Java classes.
 */
public class MemoryStorage implements IStorage {

  @Override
  public Cluster getCluster(String clusterName) {
    return null;
  }

  @Override
  public Cluster insertCluster(Cluster newCluster) {
    return null;
  }

  @Override
  public RepairRun addRepairRun(RepairRun repairRun) {
    return null;
  }

  @Override
  public RepairRun getRepairRun(long id) {
    return null;
  }
}
