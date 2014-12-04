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

  RepairRun getRepairRun(long id);

  ColumnFamily addColumnFamily(ColumnFamily.Builder newTable);

  ColumnFamily getColumnFamily(long id);
  ColumnFamily getColumnFamily(String cluster, String keyspace, String table);

  Collection<RepairSegment> addRepairSegments(Collection<RepairSegment.Builder> newSegments);

  boolean updateRepairSegment(RepairSegment newRepairSegment);

  RepairSegment getRepairSegment(long id);
  RepairSegment getNextFreeSegment(long runId);
  RepairSegment getNextFreeSegmentInRange(long runId, Range<BigInteger> range);
}
