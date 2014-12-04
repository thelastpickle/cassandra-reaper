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

  @Override
  public boolean addCluster(Cluster cluster) {
    clusters.put(cluster.getName(), cluster);
    return true;
  }

  @Override
  public boolean updateCluster(Cluster cluster) {
    return addCluster(cluster);
  }

  @Override
  public Cluster getCluster(String clusterName) {
    return clusters.get(clusterName);
  }

  @Override
  public boolean addRepairRun(RepairRun newRepairRun) {
    assert newRepairRun.getId() == null : "new RepairRun instance must NOT have ID set";
    newRepairRun.setId(REPAIR_RUN_ID.incrementAndGet());
    repairRuns.put(newRepairRun.getId(), newRepairRun);
    return true;
  }

  @Override
  public RepairRun getRepairRun(long id) {
    return repairRuns.get(id);
  }

  @Override
  public boolean addColumnFamily(ColumnFamily newColumnFamily) {
    assert newColumnFamily.getId() == null : "new ColumnFamily instance must NOT have ID set";
    newColumnFamily.setId(COLUMN_FAMILY_ID.incrementAndGet());
    columnFamilies.put(newColumnFamily.getId(), newColumnFamily);
    return true;
  }

  @Override
  public ColumnFamily getColumnFamily(long id) {
    return columnFamilies.get(id);
  }

  @Override
  public boolean addRepairSegments(Collection<RepairSegment> newSegments) {
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
