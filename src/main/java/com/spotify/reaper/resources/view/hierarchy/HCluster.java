package com.spotify.reaper.resources.view.hierarchy;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.spotify.reaper.core.Cluster;
import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.core.RepairSchedule;
import com.spotify.reaper.core.RepairSegment;
import com.spotify.reaper.core.RepairUnit;
import com.spotify.reaper.service.RingRange;
import com.spotify.reaper.storage.IStorage;
import com.spotify.reaper.storage.MemoryStorage;

import org.apache.cassandra.repair.RepairParallelism;
import org.joda.time.DateTime;

import java.math.BigInteger;
import java.util.Collection;
import java.util.Set;

import javax.annotation.Nullable;

public class HCluster {

  public final String name;
  public final Set<String> seedHosts;
  public final String partitioner;

  public final Collection<HUnit> units;

  public HCluster(Cluster cluster, Collection<HUnit> units) {
    name = cluster.getName();
    seedHosts = cluster.getSeedHosts();
    partitioner = cluster.getPartitioner();
    this.units = units;
  }

  public HCluster(IStorage storage, String clusterName) {
    this(storage.getCluster(clusterName).get(), getUnitsForCluster(storage, clusterName));
  }

  // Bj0rn: Either we want this in storage, or "getKeyspacesForCluster" and "getUnitsForKeyspace".
  private static Collection<HUnit> getUnitsForCluster(final IStorage storage, String clusterName) {
    Collection<RepairRun> allRuns = storage.getRepairRunsForCluster(clusterName);
    Collection<Long> uniqueUnitIds = Sets.newHashSet(Collections2.transform(allRuns,
        new Function<RepairRun, Long>() {
          @Nullable
          @Override
          public Long apply(RepairRun run) {
            return run.getRepairUnitId();
          }
        }));
    return Collections2.transform(uniqueUnitIds, new Function<Long, HUnit>() {
      @Nullable
      @Override
      public HUnit apply(Long unitId) {
        return new HUnit(storage, unitId);
      }
    });
  }
}
