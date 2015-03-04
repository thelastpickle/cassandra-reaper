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

import javax.annotation.Nullable;

public class HCluster {
  public static void main(String[] args) throws JsonProcessingException {
    IStorage storage = new MemoryStorage();

    Cluster cluster = new Cluster("example", "ExamplePartitioner",
        Sets.newHashSet("host1", "host1"));
    storage.addCluster(cluster);
    RepairUnit unit = storage.addRepairUnit(new RepairUnit.Builder(cluster.getName(), "exampleKS",
        Sets.newHashSet("exampleCF1", "exampleCF2")));
    RepairRun run = storage.addRepairRun(new RepairRun.Builder(cluster.getName(), unit.getId(),
        DateTime.now(), 0.1337, 1, RepairParallelism.DATACENTER_AWARE));
    RepairSchedule schedule = storage.addRepairSchedule(new RepairSchedule.Builder(unit.getId(),
        RepairSchedule.State.PAUSED, 8, DateTime.now(), ImmutableList.of(run.getId()), 1,
        RepairParallelism.PARALLEL, 0.99, DateTime.now()));

    storage.addRepairSegments(Lists.newArrayList(
        new RepairSegment.Builder(
            run.getId(), new RingRange(BigInteger.ZERO, BigInteger.ONE), unit.getId())
    ), run.getId());

    Object hObj = new HCluster(storage, cluster.getName());
    System.out.println(new ObjectMapper()
        .setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES)
        .writeValueAsString(hObj));
  }

  public final String name;
  public final Collection<HUnit> units;

  public HCluster(Cluster cluster, Collection<HUnit> units) {
    name = cluster.getName();
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
