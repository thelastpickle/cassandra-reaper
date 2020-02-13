/*
 * Copyright 2017-2017 Spotify AB
 * Copyright 2017-2019 The Last Pickle Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.cassandrareaper.service;

import io.cassandrareaper.AppContext;
import io.cassandrareaper.ReaperApplicationConfiguration;
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.RepairRun;
import io.cassandrareaper.core.RepairSegment;
import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.core.Segment;
import io.cassandrareaper.storage.CassandraStorage;
import io.cassandrareaper.storage.IDistributedStorage;
import io.cassandrareaper.storage.IStorage;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.cassandra.repair.RepairParallelism;
import org.fest.assertions.api.Assertions;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class RepairManagerTest {

  private static final Set<String> TABLES = ImmutableSet.of("table1");

  /**
   * Verifies that when a RUNNING segment exists that has no leader it will get aborted. Will happen
   * even if a repair runner exists for the run, when using a IDistributedStorage backend
   *
   * @throws ReaperException if some goes wrong :)
   * @throws InterruptedException if some goes wrong :)
   */
  @Test
  public void abortRunningSegmentWithNoLeader() throws ReaperException, InterruptedException {
    final String clusterName = "reaper";
    final String ksName = "reaper";
    final Set<String> cfNames = Sets.newHashSet("reaper");
    final boolean incrementalRepair = false;
    final Set<String> nodes = Sets.newHashSet("127.0.0.1");
    final Set<String> datacenters = Collections.emptySet();
    final double intensity = 0.5f;
    final int repairThreadCount = 1;

    // use CassandraStorage so we get both IStorage and IDistributedStorage
    final IStorage storage = mock(CassandraStorage.class);

    storage.addCluster(Cluster.builder().withName(clusterName).withSeedHosts(ImmutableSet.of("127.0.0.1")).build());

    AppContext context = new AppContext();
    context.storage = storage;
    context.config = new ReaperApplicationConfiguration();

    RepairManager repairManager = RepairManager.create(
        context,
        Executors.newScheduledThreadPool(1),
        500,
        TimeUnit.MILLISECONDS,
        1,
        TimeUnit.MILLISECONDS);

    repairManager = Mockito.spy(repairManager);
    context.repairManager = repairManager;

    final RepairUnit cf = RepairUnit.builder()
        .clusterName(clusterName)
        .keyspaceName(ksName)
        .columnFamilies(cfNames)
        .incrementalRepair(incrementalRepair)
        .nodes(nodes)
        .datacenters(datacenters)
        .repairThreadCount(repairThreadCount)
        .build(UUIDs.timeBased());

    final RepairRun run = RepairRun.builder(clusterName, cf.getId())
            .intensity(intensity)
            .segmentCount(1)
            .repairParallelism(RepairParallelism.PARALLEL)
            .tables(TABLES)
            .build(UUIDs.timeBased());

    final RepairSegment segment = RepairSegment.builder(
                Segment.builder().withTokenRange(new RingRange("-1", "1")).build(), cf.getId())
            .withRunId(run.getId())
            .withId(UUIDs.timeBased())
            .build();

    context.repairManager.repairRunners.put(run.getId(), mock(RepairRunner.class));
    Mockito.doNothing().when(context.repairManager).abortSegments(any(), any());
    Mockito.doReturn(run).when(context.repairManager).startRepairRun(run);
    when(context.storage.getRepairRunsWithState(RepairRun.RunState.RUNNING)).thenReturn(Arrays.asList(run));
    when(context.storage.getRepairRunsWithState(RepairRun.RunState.PAUSED)).thenReturn(Collections.emptyList());
    when(context.storage.getSegmentsWithState(any(), any())).thenReturn(Arrays.asList(segment));
    when(((IDistributedStorage) context.storage).getLeaders()).thenReturn(Collections.emptyList());

    context.repairManager.resumeRunningRepairRuns();

    // Check that abortSegments was invoked is at least one segment, meaning abortion occurs
    Mockito.verify(context.repairManager, Mockito.times(2)).abortSegments(Mockito.argThat(new NotEmptyList()), any());
  }

  /**
   * Verifies that when a RUNNING segment exists that has a leader it will not get aborted. When
   * using a IDistributedStorage backend
   *
   * @throws ReaperException if some goes wrong :)
   * @throws InterruptedException if some goes wrong :)
   */
  @Test
  public void doNotAbortRunningSegmentWithLeader() throws ReaperException, InterruptedException {
    final String clusterName = "reaper";
    final String ksName = "reaper";
    final Set<String> cfNames = Sets.newHashSet("reaper");
    final boolean incrementalRepair = false;
    final Set<String> nodes = Sets.newHashSet("127.0.0.1");
    final Set<String> datacenters = Collections.emptySet();
    final double intensity = 0.5f;
    final int repairThreadCount = 1;

    // use CassandraStorage so we get both IStorage and IDistributedStorage
    final IStorage storage = mock(CassandraStorage.class);

    storage.addCluster(Cluster.builder().withName(clusterName).withSeedHosts(ImmutableSet.of("127.0.0.1")).build());

    AppContext context = new AppContext();
    context.storage = storage;
    context.config = new ReaperApplicationConfiguration();

    RepairManager repairManager = RepairManager.create(
        context,
        Executors.newScheduledThreadPool(1),
        500,
        TimeUnit.MILLISECONDS,
        1,
        TimeUnit.MILLISECONDS);

    repairManager = Mockito.spy(repairManager);
    context.repairManager = repairManager;

    final RepairUnit cf = RepairUnit.builder()
        .clusterName(clusterName)
        .keyspaceName(ksName)
        .columnFamilies(cfNames)
        .incrementalRepair(incrementalRepair)
        .nodes(nodes)
        .datacenters(datacenters)
        .repairThreadCount(repairThreadCount)
        .build(UUIDs.timeBased());

    final RepairRun run = RepairRun.builder(clusterName, cf.getId())
            .intensity(intensity)
            .segmentCount(1)
            .repairParallelism(RepairParallelism.PARALLEL)
            .tables(TABLES)
            .build(UUIDs.timeBased());

    final RepairSegment segment = RepairSegment.builder(
                Segment.builder().withTokenRange(new RingRange("-1", "1")).build(), cf.getId())
            .withRunId(run.getId())
            .withId(UUIDs.timeBased())
            .build();

    context.repairManager.repairRunners.put(run.getId(), mock(RepairRunner.class));

    Mockito.doNothing().when(context.repairManager).abortSegments(any(), any());
    Mockito.doNothing().when(context.repairManager).abortSegments(any(), any());
    Mockito.doReturn(run).when(context.repairManager).startRepairRun(run);
    when(context.storage.getRepairRunsWithState(RepairRun.RunState.RUNNING)).thenReturn(Arrays.asList(run));
    when(context.storage.getRepairRunsWithState(RepairRun.RunState.PAUSED)).thenReturn(Collections.emptyList());
    when(context.storage.getSegmentsWithState(any(), any())).thenReturn(Arrays.asList(segment));
    when(((IDistributedStorage) context.storage).getLeaders()).thenReturn(Arrays.asList(segment.getId()));

    context.repairManager.resumeRunningRepairRuns();

    // Check that abortSegments was invoked with an empty list, meaning no abortion occurs
    Mockito.verify(context.repairManager, Mockito.times(2)).abortSegments(Mockito.argThat(new EmptyList()), any());
  }

  /**
   * Verifies that when a RUNNING segment exists it will not get aborted when using a non
   * IDistributedStorage backend if a repair runner exists
   *
   * @throws ReaperException if some goes wrong :)
   * @throws InterruptedException if some goes wrong :)
   */
  @Test
  public void doNotAbortRunningSegmentWithRepairRunnerAndNoDistributedStorage()
      throws ReaperException, InterruptedException {
    final String clusterName = "reaper";
    final String ksName = "reaper";
    final Set<String> cfNames = Sets.newHashSet("reaper");
    final boolean incrementalRepair = false;
    final Set<String> nodes = Sets.newHashSet("127.0.0.1");
    final Set<String> datacenters = Collections.emptySet();
    final double intensity = 0.5f;
    final int repairThreadCount = 1;

    final IStorage storage = mock(IStorage.class);

    storage.addCluster(Cluster.builder().withName(clusterName).withSeedHosts(ImmutableSet.of("127.0.0.1")).build());

    AppContext context = new AppContext();
    context.config = new ReaperApplicationConfiguration();
    context.storage = storage;

    RepairManager repairManager = RepairManager.create(
        context,
        Executors.newScheduledThreadPool(1),
        500,
        TimeUnit.MILLISECONDS,
        1,
        TimeUnit.MILLISECONDS);

    repairManager = Mockito.spy(repairManager);
    context.repairManager = repairManager;

    final RepairUnit cf = RepairUnit.builder()
        .clusterName(clusterName)
        .keyspaceName(ksName)
        .columnFamilies(cfNames)
        .incrementalRepair(incrementalRepair)
        .nodes(nodes)
        .datacenters(datacenters)
        .repairThreadCount(repairThreadCount)
        .build(UUIDs.timeBased());

    final RepairRun run = RepairRun.builder(clusterName, cf.getId())
            .intensity(intensity)
            .segmentCount(1)
            .repairParallelism(RepairParallelism.PARALLEL)
            .tables(TABLES)
            .build(UUIDs.timeBased());

    final RepairSegment segment = RepairSegment.builder(
                Segment.builder().withTokenRange(new RingRange("-1", "1")).build(), cf.getId())
            .withRunId(run.getId())
            .withId(UUIDs.timeBased())
            .build();

    context.repairManager.repairRunners.put(run.getId(), mock(RepairRunner.class));
    Mockito.doNothing().when(context.repairManager).abortSegments(any(), any());
    Mockito.doReturn(run).when(context.repairManager).startRepairRun(run);
    when(context.storage.getRepairRunsWithState(RepairRun.RunState.RUNNING)).thenReturn(Arrays.asList(run));
    when(context.storage.getRepairRunsWithState(RepairRun.RunState.PAUSED)).thenReturn(Collections.emptyList());
    when(context.storage.getSegmentsWithState(any(), any())).thenReturn(Arrays.asList(segment));

    context.repairManager.resumeRunningRepairRuns();

    // Check that abortSegments was not invoked at all, meaning no abortion occurs
    Mockito.verify(context.repairManager, Mockito.times(0)).abortSegments(any(), any());
  }

  /**
   * Verifies that when a RUNNING segment exists it will get aborted when using a non
   * IDistributedStorage backend if no repair runner exists (first boot or Reaper)
   *
   * @throws ReaperException if some goes wrong :)
   * @throws InterruptedException if some goes wrong :)
   */
  @Test
  public void abortRunningSegmentWithNoRepairRunnerAndNoDistributedStorage()
      throws ReaperException, InterruptedException {
    final String clusterName = "reaper";
    final String ksName = "reaper";
    final Set<String> cfNames = Sets.newHashSet("reaper");
    final boolean incrementalRepair = false;
    final Set<String> nodes = Sets.newHashSet("127.0.0.1");
    final Set<String> datacenters = Collections.emptySet();
    final double intensity = 0.5f;
    final int repairThreadCount = 1;

    final IStorage storage = mock(IStorage.class);

    storage.addCluster(Cluster.builder().withName(clusterName).withSeedHosts(ImmutableSet.of("127.0.0.1")).build());

    AppContext context = new AppContext();
    context.storage = storage;
    context.config = new ReaperApplicationConfiguration();

    RepairManager repairManager = RepairManager.create(
        context,
        Executors.newScheduledThreadPool(1),
        500,
        TimeUnit.MILLISECONDS,
        1,
        TimeUnit.MILLISECONDS);

    repairManager = Mockito.spy(repairManager);
    context.repairManager = repairManager;

    final RepairUnit cf = RepairUnit.builder()
        .clusterName(clusterName)
        .keyspaceName(ksName)
        .columnFamilies(cfNames)
        .incrementalRepair(incrementalRepair)
        .nodes(nodes)
        .datacenters(datacenters)
        .repairThreadCount(repairThreadCount)
        .build(UUIDs.timeBased());

    final RepairRun run = RepairRun.builder(clusterName, cf.getId())
            .intensity(intensity)
            .segmentCount(1)
            .repairParallelism(RepairParallelism.PARALLEL)
            .tables(TABLES)
            .build(UUIDs.timeBased());

    final RepairSegment segment = RepairSegment.builder(
                Segment.builder().withTokenRange(new RingRange("-1", "1")).build(), cf.getId())
            .withRunId(run.getId())
            .withId(UUIDs.timeBased())
            .build();

    Mockito.doNothing().when(context.repairManager).abortSegments(any(), any());
    Mockito.doReturn(run).when(context.repairManager).startRepairRun(run);
    when(context.storage.getRepairRunsWithState(RepairRun.RunState.RUNNING)).thenReturn(Arrays.asList(run));
    when(context.storage.getRepairRunsWithState(RepairRun.RunState.PAUSED)).thenReturn(Collections.emptyList());
    when(context.storage.getSegmentsWithState(any(), any())).thenReturn(Arrays.asList(segment));

    context.repairManager.resumeRunningRepairRuns();

    // Check that abortSegments was invoked with an non empty list, meaning abortion occurs
    Mockito.verify(context.repairManager, Mockito.times(2)).abortSegments(Mockito.argThat(new NotEmptyList()), any());
  }

  @Test
  public void updateRepairRunIntensityTest() throws ReaperException, InterruptedException {
    final String clusterName = "reaper";

    AppContext context = new AppContext();
    context.config = new ReaperApplicationConfiguration();
    context.storage = mock(IStorage.class);

    context.storage
        .addCluster(Cluster.builder().withName(clusterName).withSeedHosts(ImmutableSet.of("127.0.0.1")).build());

    context.repairManager = RepairManager.create(
        context,
        Executors.newScheduledThreadPool(1),
        500,
        TimeUnit.MILLISECONDS,
        1,
        TimeUnit.MILLISECONDS);

    final String ksName = "reaper";
    final Set<String> cfNames = Sets.newHashSet("reaper");
    final boolean incrementalRepair = false;
    final Set<String> nodes = Sets.newHashSet("127.0.0.1");
    final Set<String> datacenters = Collections.emptySet();
    final int repairThreadCount = 1;

    final RepairUnit cf = RepairUnit.builder()
        .clusterName(clusterName)
        .keyspaceName(ksName)
        .columnFamilies(cfNames)
        .incrementalRepair(incrementalRepair)
        .nodes(nodes)
        .datacenters(datacenters)
        .repairThreadCount(repairThreadCount)
        .build(UUIDs.timeBased());

    double intensity = 0.5f;

    final RepairRun run = RepairRun.builder(clusterName, cf.getId())
            .intensity(intensity)
            .segmentCount(1)
            .repairParallelism(RepairParallelism.PARALLEL)
            .tables(TABLES)
            .build(UUIDs.timeBased());

    when(context.storage.updateRepairRun(any(), any())).thenReturn(true);

    intensity = 0.1;
    RepairRun updated = context.repairManager.updateRepairRunIntensity(run, intensity);

    Assertions.assertThat(updated.getId()).isEqualTo(run.getId());
    Assertions.assertThat(updated.getIntensity()).isEqualTo(intensity);
    Mockito.verify(context.storage, Mockito.times(1)).updateRepairRun(any(), any());
  }

  private static class NotEmptyList implements ArgumentMatcher<Collection<RepairSegment>> {
    @Override
    public boolean matches(Collection<RepairSegment> segments) {
      return !segments.isEmpty();
    }
  }

  private static class EmptyList implements ArgumentMatcher<Collection<RepairSegment>> {
    @Override
    public boolean matches(Collection<RepairSegment> segments) {
      return segments.isEmpty();
    }
  }

}
