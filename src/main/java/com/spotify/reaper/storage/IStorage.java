package com.spotify.reaper.storage;

import com.spotify.reaper.core.Cluster;
import com.spotify.reaper.core.ColumnFamily;
import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.core.RepairSegment;

import java.util.Collection;

/**
 * API definition for cassandra-reaper.
 */
public interface IStorage {

  public boolean addCluster(Cluster newCluster);

  public boolean updateCluster(Cluster newCluster);

  public Cluster getCluster(String clusterName);

  public boolean addRepairRun(RepairRun newRepairRun);

  public RepairRun getRepairRun(long id);

  public boolean addColumnFamily(ColumnFamily newTable);

  public ColumnFamily getColumnFamily(long id);

  public boolean addRepairSegments(Collection<RepairSegment> newSegments);

  public boolean updateRepairSegment(RepairSegment newRepairSegment);

  public RepairSegment getNextFreeSegment(long runId);

  public RepairSegment getNextFreeSegmentInRange(long runId, long start, long end);
}
