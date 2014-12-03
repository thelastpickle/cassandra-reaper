package com.spotify.reaper.storage;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.spotify.reaper.core.Cluster;
import com.spotify.reaper.core.ColumnFamily;
import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.core.RepairSegment;

import org.joda.time.DateTime;

import java.util.List;
import java.util.Map;

/**
 * Implements the StorageAPI using transient Java classes.
 */
public class MemoryStorage implements IStorage {

  private Map<String, Cluster> clusters = Maps.newHashMap();
  private List<RepairRun> repairRuns = Lists.newArrayList();
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
  public RepairRun addRepairRun(String cause, String owner, DateTime creationTime,
                                double intensity) {
    int id = repairRuns.size();
    RepairRun repairRun = new RepairRun.Builder().id(id).cause(cause).owner(owner).state(
        RepairRun.State.NOT_STARTED).creationTime(creationTime).intensity(intensity).build();
    repairRuns.add(repairRun);
    return repairRun;
  }

  @Override
  public RepairRun getRepairRun(long id) {
    return repairRuns.get((int)id);
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
