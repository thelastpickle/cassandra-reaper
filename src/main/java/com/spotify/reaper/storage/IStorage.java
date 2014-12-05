package com.spotify.reaper.storage;

import com.google.common.collect.Range;

import com.spotify.reaper.core.Cluster;
import com.spotify.reaper.core.ColumnFamily;
import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.core.RepairSegment;

import java.math.BigInteger;
import java.util.Collection;

/**
 * API definition for cassandra-reaper.
 */
public interface IStorage {

  Cluster addCluster(Cluster cluster);

  boolean updateCluster(Cluster newCluster);

  Cluster getCluster(String clusterName);

  RepairRun addRepairRun(RepairRun.Builder repairRun);

  boolean updateRepairRun(RepairRun repairRun);

  /**
   * Get new RepairRun instance fetched from database with matching ID.
   * Notice that the repairRunLock object must be given every time, as the
   * lock must be shared instance with every separate RepairRun instance
   * for proper synchronization.
   *
   * @param id The storage id of the RepairRun to fetch from storage.
   * @param repairRunLock Lock object used for synchronization in RepairRunner.
   *                      Shared between all instances of RepairRun having same id.
   * @return The fetched RepairRun instance matching given id, or null if not found.
   */
  RepairRun getRepairRun(long id, Object repairRunLock);

  ColumnFamily addColumnFamily(ColumnFamily.Builder newTable);

  ColumnFamily getColumnFamily(long id);
  ColumnFamily getColumnFamily(String cluster, String keyspace, String table);

  Collection<RepairSegment> addRepairSegments(Collection<RepairSegment.Builder> newSegments);

  boolean updateRepairSegment(RepairSegment newRepairSegment);

  RepairSegment getRepairSegment(long id);
  RepairSegment getNextFreeSegment(long runId);
  RepairSegment getNextFreeSegmentInRange(long runId, BigInteger start, BigInteger end);
}
