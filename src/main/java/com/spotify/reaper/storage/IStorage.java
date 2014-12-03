package com.spotify.reaper.storage;

import com.spotify.reaper.core.Cluster;
import com.spotify.reaper.core.ColumnFamily;
import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.core.RepairSegment;

/**
 * API definition for cassandra-reaper.
 */
public interface IStorage {

  public Cluster addCluster(Cluster cluster);

  public Cluster updateCluster(Cluster cluster);

  public Cluster getCluster(String clusterName);

  public RepairRun addRepairRun(RepairRun repairRun);

  public RepairRun getRepairRun(long id);

  public boolean addColumnFamily(ColumnFamily newTable);

  public ColumnFamily getColumnFamily(long id);

  public RepairSegment getNextFreeSegment(long runId);

  // IDEA: should we have something like this for parallel runners on same ring?
  //public RepairSegment getNextFreeSegmentInRange(long runId, long start, long end);
}
