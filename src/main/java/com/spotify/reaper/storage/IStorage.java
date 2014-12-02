package com.spotify.reaper.storage;

import com.spotify.reaper.core.Cluster;
import com.spotify.reaper.core.RepairRun;

/**
 * API definition for cassandra-reaper.
 */
public interface IStorage {

  public Cluster getCluster(String clusterName);

  public Cluster insertCluster(Cluster newCluster);

  public RepairRun addRepairRun(RepairRun repairRun);

  public RepairRun getRepairRun(long id);

}
