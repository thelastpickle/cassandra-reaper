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
import io.cassandrareaper.management.ClusterFacade;
import io.cassandrareaper.storage.IDistributedStorage;
import io.cassandrareaper.storage.IStorageDao;
import io.cassandrareaper.storage.cassandra.CassandraStorageFacade;
import io.cassandrareaper.storage.cluster.IClusterDao;
import io.cassandrareaper.storage.repairrun.IRepairRunDao;
import io.cassandrareaper.storage.repairsegment.IRepairSegmentDao;
import io.cassandrareaper.storage.repairunit.IRepairUnitDao;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.cassandra.repair.RepairParallelism;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class RepairManagerTest {

  private static final Set<String> TABLES = ImmutableSet.of("table1");

  /**
   * Verifies that when a RUNNING segment exists that has no leader it will get aborted. Will happen
   * even if a repair runner exists for the run, when using a IDistributedStorage backend
   *
   * @throws ReaperException      if some goes wrong :)
   * @throws InterruptedException if some goes wrong :)
   */
  @Test
  public void abortRunningSegmentWithNoLeader() throws ReaperException, InterruptedException {
    final String clusterName = "reaper";
    final String ksName = "reaper";
    final Set<String> cfNames = Sets.newHashSet("reaper");
    final boolean incrementalRepair = false;
    final boolean subrangeIncremental = false;
    final Set<String> nodes = Sets.newHashSet("127.0.0.1");
    final Set<String> datacenters = Collections.emptySet();
    final double intensity = 0.5f;
    final int repairThreadCount = 1;
    final int segmentTimeout = 30;

    // use CassandraStorage so we get both IStorage and IDistributedStorage
    final IStorageDao storage = mock(CassandraStorageFacade.class);
    IRepairRunDao mockedRepairRunDao = mock(IRepairRunDao.class);
    final RepairUnit cf = RepairUnit.builder()
        .clusterName(clusterName)
        .keyspaceName(ksName)
        .columnFamilies(cfNames)
        .incrementalRepair(incrementalRepair)
        .subrangeIncrementalRepair(subrangeIncremental)
        .nodes(nodes)
        .datacenters(datacenters)
        .repairThreadCount(repairThreadCount)
        .timeout(segmentTimeout)
        .build(UUIDs.timeBased());
    final RepairRun run = RepairRun.builder(clusterName, cf.getId())
        .intensity(intensity)
        .segmentCount(1)
        .repairParallelism(RepairParallelism.PARALLEL)
        .tables(TABLES)
        .build(UUIDs.timeBased());
    when(mockedRepairRunDao.getRepairRunsWithState(RepairRun.RunState.RUNNING)).thenReturn(Arrays.asList(run));
    when(mockedRepairRunDao.getRepairRunsWithState(RepairRun.RunState.PAUSED)).thenReturn(Collections.emptyList());
    when(storage.getRepairRunDao()).thenReturn(mockedRepairRunDao);
    IClusterDao mockedClusterDao = Mockito.mock(IClusterDao.class);
    Mockito.when(storage.getClusterDao()).thenReturn(mockedClusterDao);
    storage.getClusterDao()
        .addCluster(Cluster.builder().withName(clusterName).withSeedHosts(ImmutableSet.of("127.0.0.1")).build());

    AppContext context = new AppContext();
    context.storage = storage;
    context.config = new ReaperApplicationConfiguration();

    RepairManager repairManager = RepairManager.create(
        context,
        Executors.newScheduledThreadPool(1),
        1,
        TimeUnit.MILLISECONDS,
        1, context.storage.getRepairRunDao());

    repairManager = Mockito.spy(repairManager);
    context.repairManager = repairManager;

    final RepairSegment segment = RepairSegment.builder(
            Segment.builder().withTokenRange(new RingRange("-1", "1")).build(), cf.getId())
        .withRunId(run.getId())
        .withId(UUIDs.timeBased())
        .build();
    IRepairSegmentDao mockedRepairSegmentDao = mock(IRepairSegmentDao.class);
    Mockito.when(context.storage.getRepairSegmentDao()).thenReturn(mockedRepairSegmentDao);
    when(mockedRepairSegmentDao.getSegmentsWithState(any(), any())).thenReturn(Arrays.asList(segment));

    context.repairManager.repairRunners.put(run.getId(), mock(RepairRunner.class));
    Mockito.doNothing().when(context.repairManager).abortSegments(any(), any());
    Mockito.doReturn(run).when(context.repairManager).startRepairRun(run);

    when(((IDistributedStorage) context.storage).getLockedSegmentsForRun(any())).thenReturn(Collections.emptySet());
    IRepairUnitDao mockedRepairUnitDao = mock(IRepairUnitDao.class);
    Mockito.when(((CassandraStorageFacade) context.storage).getRepairUnitDao()).thenReturn(mockedRepairUnitDao);
    Mockito.when(mockedRepairUnitDao.getRepairUnit(any(UUID.class))).thenReturn(cf);


    context.repairManager.resumeRunningRepairRuns();

    // Check that abortSegments was invoked is at least one segment, meaning abortion occurs
    Mockito.verify(context.repairManager, Mockito.times(2)).abortSegments(Mockito.argThat(new NotEmptyList()), any());
  }

  /**
   * Verifies that when a RUNNING segment exists that has a leader it will not get aborted. When
   * using a IDistributedStorage backend
   *
   * @throws ReaperException      if some goes wrong :)
   * @throws InterruptedException if some goes wrong :)
   */
  @Test
  public void doNotAbortRunningSegmentWithLeader() throws ReaperException, InterruptedException {
    final String clusterName = "reaper";
    final String ksName = "reaper";
    final Set<String> cfNames = Sets.newHashSet("reaper");
    final boolean incrementalRepair = false;
    final boolean subrangeIncremental = false;
    final Set<String> nodes = Sets.newHashSet("127.0.0.1");
    final Set<String> datacenters = Collections.emptySet();
    final double intensity = 0.5f;
    final int repairThreadCount = 1;
    final int segmentTimeout = 30;

    // use CassandraStorage so we get both IStorage and IDistributedStorage
    final IStorageDao storage = mock(CassandraStorageFacade.class);
    IRepairRunDao mockedRepairRunDao = mock(IRepairRunDao.class);
    when(storage.getRepairRunDao()).thenReturn(mockedRepairRunDao);

    IClusterDao mockedClusterDao = Mockito.mock(IClusterDao.class);
    Mockito.when(storage.getClusterDao()).thenReturn(mockedClusterDao);

    storage.getClusterDao()
        .addCluster(Cluster.builder().withName(clusterName).withSeedHosts(ImmutableSet.of("127.0.0.1")).build());

    AppContext context = new AppContext();
    context.storage = storage;
    context.config = new ReaperApplicationConfiguration();

    RepairManager repairManager = RepairManager.create(
        context,
        Executors.newScheduledThreadPool(1),
        1,
        TimeUnit.MILLISECONDS,
        1,
        context.storage.getRepairRunDao());

    repairManager = Mockito.spy(repairManager);
    context.repairManager = repairManager;

    final RepairUnit cf = RepairUnit.builder()
        .clusterName(clusterName)
        .keyspaceName(ksName)
        .columnFamilies(cfNames)
        .incrementalRepair(incrementalRepair)
        .subrangeIncrementalRepair(subrangeIncremental)
        .nodes(nodes)
        .datacenters(datacenters)
        .repairThreadCount(repairThreadCount)
        .timeout(segmentTimeout)
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
    when(mockedRepairRunDao.getRepairRunsWithState(RepairRun.RunState.RUNNING)).thenReturn(Arrays.asList(run));
    when(mockedRepairRunDao.getRepairRunsWithState(RepairRun.RunState.PAUSED)).thenReturn(Collections.emptyList());
    IRepairSegmentDao mockedRepairSegmentDao = mock(IRepairSegmentDao.class);
    Mockito.when(context.storage.getRepairSegmentDao()).thenReturn(mockedRepairSegmentDao);
    when(mockedRepairSegmentDao.getSegmentsWithState(any(), any())).thenReturn(Arrays.asList(segment));
    IRepairUnitDao mockedRepairUnitDao = mock(IRepairUnitDao.class);
    Mockito.when(((CassandraStorageFacade) context.storage).getRepairUnitDao()).thenReturn(mockedRepairUnitDao);
    Mockito.when(mockedRepairUnitDao.getRepairUnit(any(UUID.class))).thenReturn(cf);

    when(((IDistributedStorage) context.storage).getLockedSegmentsForRun(any())).thenReturn(
        new HashSet<UUID>(Arrays.asList(segment.getId())));

    context.repairManager.resumeRunningRepairRuns();

    // Check that abortSegments was invoked with an empty list, meaning no abortion occurs
    Mockito.verify(context.repairManager, Mockito.times(2)).abortSegments(Mockito.argThat(new EmptyList()), any());
  }

  /**
   * Verifies that when a RUNNING segment exists it will not get aborted when using a non
   * IDistributedStorage backend if a repair runner exists
   *
   * @throws ReaperException      if some goes wrong :)
   * @throws InterruptedException if some goes wrong :)
   */
  @Test
  public void doNotAbortRunningSegmentWithRepairRunnerAndNoDistributedStorage()
      throws ReaperException, InterruptedException {
    final String clusterName = "reaper";
    final String ksName = "reaper";
    final Set<String> cfNames = Sets.newHashSet("reaper");
    final boolean incrementalRepair = false;
    final boolean subrangeIncremental = false;
    final Set<String> nodes = Sets.newHashSet("127.0.0.1");
    final Set<String> datacenters = Collections.emptySet();
    final double intensity = 0.5f;
    final int repairThreadCount = 1;
    final int segmentTimeout = 30;

    final IStorageDao storage = mock(IStorageDao.class);
    IRepairRunDao mockedRepairRunDao = mock(IRepairRunDao.class);
    when(storage.getRepairRunDao()).thenReturn(mockedRepairRunDao);
    IClusterDao mockedClusterDao = Mockito.mock(IClusterDao.class);
    Mockito.when(storage.getClusterDao()).thenReturn(mockedClusterDao);
    storage.getClusterDao()
        .addCluster(Cluster.builder().withName(clusterName).withSeedHosts(ImmutableSet.of("127.0.0.1")).build());

    final RepairUnit cf = RepairUnit.builder()
        .clusterName(clusterName)
        .keyspaceName(ksName)
        .columnFamilies(cfNames)
        .incrementalRepair(incrementalRepair)
        .subrangeIncrementalRepair(subrangeIncremental)
        .nodes(nodes)
        .datacenters(datacenters)
        .repairThreadCount(repairThreadCount)
        .timeout(segmentTimeout)
        .build(UUIDs.timeBased());

    IRepairUnitDao mockedRepairUnitDao = mock(IRepairUnitDao.class);
    Mockito.when(storage.getRepairUnitDao()).thenReturn(mockedRepairUnitDao);
    Mockito.when(mockedRepairUnitDao.getRepairUnit(any(UUID.class))).thenReturn(cf);

    AppContext context = new AppContext();
    context.config = new ReaperApplicationConfiguration();
    context.storage = storage;

    RepairManager repairManager = RepairManager.create(
        context,
        Executors.newScheduledThreadPool(1),
        1,
        TimeUnit.MILLISECONDS,
        1,
        context.storage.getRepairRunDao());

    repairManager = Mockito.spy(repairManager);
    context.repairManager = repairManager;

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

    IRepairSegmentDao mockedRepairSegmentDao = mock(IRepairSegmentDao.class);
    Mockito.when(context.storage.getRepairSegmentDao()).thenReturn(mockedRepairSegmentDao);
    when(mockedRepairSegmentDao.getSegmentsWithState(any(), any())).thenReturn(Arrays.asList(segment));

    context.repairManager.repairRunners.put(run.getId(), mock(RepairRunner.class));
    Mockito.doNothing().when(context.repairManager).abortSegments(any(), any());
    Mockito.doReturn(run).when(context.repairManager).startRepairRun(run);
    when(mockedRepairRunDao.getRepairRunsWithState(RepairRun.RunState.RUNNING)).thenReturn(Arrays.asList(run));
    when(mockedRepairRunDao.getRepairRunsWithState(RepairRun.RunState.PAUSED)).thenReturn(Collections.emptyList());


    context.repairManager.resumeRunningRepairRuns();

    // Check that abortSegments was not invoked at all, meaning no abortion occurs
    Mockito.verify(context.repairManager, Mockito.times(0)).abortSegments(any(), any());
  }

  /**
   * Verifies that when a RUNNING segment exists it will get aborted when using a non
   * IDistributedStorage backend if no repair runner exists (first boot or Reaper)
   *
   * @throws ReaperException      if some goes wrong :)
   * @throws InterruptedException if some goes wrong :)
   */
  @Test
  public void abortRunningSegmentWithNoRepairRunnerAndNoDistributedStorage()
      throws ReaperException, InterruptedException {
    final String clusterName = "reaper";
    final String ksName = "reaper";
    final Set<String> cfNames = Sets.newHashSet("reaper");
    final boolean incrementalRepair = false;
    final boolean subrangeIncremental = false;
    final Set<String> nodes = Sets.newHashSet("127.0.0.1");
    final Set<String> datacenters = Collections.emptySet();
    final double intensity = 0.5f;
    final int repairThreadCount = 1;
    final int segmentTimeout = 30;

    final IStorageDao storage = mock(IStorageDao.class);
    IRepairRunDao mockedRepairRunDao = mock(IRepairRunDao.class);
    when(storage.getRepairRunDao()).thenReturn(mockedRepairRunDao);
    IClusterDao mockedClusterDao = Mockito.mock(IClusterDao.class);
    Mockito.when(storage.getClusterDao()).thenReturn(mockedClusterDao);
    storage.getClusterDao()
        .addCluster(Cluster.builder().withName(clusterName).withSeedHosts(ImmutableSet.of("127.0.0.1")).build());

    AppContext context = new AppContext();
    context.storage = storage;
    context.config = new ReaperApplicationConfiguration();

    RepairManager repairManager = RepairManager.create(
        context,
        Executors.newScheduledThreadPool(1),
        1,
        TimeUnit.MILLISECONDS,
        1,
        context.storage.getRepairRunDao());

    repairManager = Mockito.spy(repairManager);
    context.repairManager = repairManager;

    final RepairUnit cf = RepairUnit.builder()
        .clusterName(clusterName)
        .keyspaceName(ksName)
        .columnFamilies(cfNames)
        .incrementalRepair(incrementalRepair)
        .subrangeIncrementalRepair(subrangeIncremental)
        .nodes(nodes)
        .datacenters(datacenters)
        .repairThreadCount(repairThreadCount)
        .timeout(segmentTimeout)
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

    IRepairSegmentDao mockedRepairSegmentDao = mock(IRepairSegmentDao.class);
    Mockito.when(context.storage.getRepairSegmentDao()).thenReturn(mockedRepairSegmentDao);
    when(mockedRepairSegmentDao.getSegmentsWithState(any(), any())).thenReturn(Arrays.asList(segment));

    when(mockedRepairRunDao.getRepairRunsWithState(RepairRun.RunState.RUNNING)).thenReturn(Arrays.asList(run));
    when(mockedRepairRunDao.getRepairRunsWithState(RepairRun.RunState.PAUSED)).thenReturn(Collections.emptyList());
    IRepairUnitDao mockedRepairUnitDao = mock(IRepairUnitDao.class);
    Mockito.when(context.storage.getRepairUnitDao()).thenReturn(mockedRepairUnitDao);
    Mockito.when(mockedRepairUnitDao.getRepairUnit(any(UUID.class))).thenReturn(cf);


    context.repairManager.resumeRunningRepairRuns();

    // Check that abortSegments was invoked with an non empty list, meaning abortion occurs
    Mockito.verify(context.repairManager, Mockito.times(2)).abortSegments(Mockito.argThat(new NotEmptyList()), any());
  }

  @Test
  public void updateRepairRunIntensityTest() throws ReaperException, InterruptedException {
    final String clusterName = "reaper";

    AppContext context = new AppContext();
    context.config = new ReaperApplicationConfiguration();
    context.storage = mock(CassandraStorageFacade.class);
    IRepairRunDao mockedRepairRunDao = mock(IRepairRunDao.class);
    when(context.storage.getRepairRunDao()).thenReturn(mockedRepairRunDao);
    when(mockedRepairRunDao.updateRepairRun(any(), any())).thenReturn(true);

    IClusterDao mockedClusterDao = Mockito.mock(IClusterDao.class);
    Mockito.when(context.storage.getClusterDao()).thenReturn(mockedClusterDao);


    mockedClusterDao
        .addCluster(Cluster.builder().withName(clusterName)
            .withSeedHosts(ImmutableSet.of("127.0.0.1")).build());

    context.repairManager = RepairManager.create(
        context,
        Executors.newScheduledThreadPool(1),
        1,
        TimeUnit.MILLISECONDS,
        1,
        context.storage.getRepairRunDao());

    final String ksName = "reaper";
    final Set<String> cfNames = Sets.newHashSet("reaper");
    final boolean incrementalRepair = false;
    final boolean subrangeIncremental = false;
    final Set<String> nodes = Sets.newHashSet("127.0.0.1");
    final Set<String> datacenters = Collections.emptySet();
    final int repairThreadCount = 1;
    final int segmentTimeout = 30;

    final RepairUnit cf = RepairUnit.builder()
        .clusterName(clusterName)
        .keyspaceName(ksName)
        .columnFamilies(cfNames)
        .incrementalRepair(incrementalRepair)
        .subrangeIncrementalRepair(subrangeIncremental)
        .nodes(nodes)
        .datacenters(datacenters)
        .repairThreadCount(repairThreadCount)
        .timeout(segmentTimeout)
        .build(UUIDs.timeBased());

    double intensity = 0.5f;

    final RepairRun run = RepairRun.builder(clusterName, cf.getId())
        .intensity(intensity)
        .segmentCount(1)
        .repairParallelism(RepairParallelism.PARALLEL)
        .tables(TABLES)
        .build(UUIDs.timeBased());

    intensity = 0.1;
    RepairRun updated = context.repairManager.updateRepairRunIntensity(run, intensity);

    Assertions.assertThat(updated.getId()).isEqualTo(run.getId());
    Assertions.assertThat(updated.getIntensity()).isEqualTo(intensity);

    Mockito.verify(mockedRepairRunDao, Mockito.times(1)).updateRepairRun(any(), any());
  }

  @Test
  public void countRepairRunnersPerClusterTest() throws ReaperException, InterruptedException {
    AppContext context = new AppContext();
    context.config = new ReaperApplicationConfiguration();
    context.storage = mock(CassandraStorageFacade.class);
    IRepairRunDao mockedRepairRunDao = mock(IRepairRunDao.class);
    when(context.storage.getRepairRunDao()).thenReturn(mockedRepairRunDao);
    doReturn(true).when(mockedRepairRunDao).updateRepairRun(any());

    IClusterDao mockedClusterDao = Mockito.mock(IClusterDao.class);
    Mockito.when(context.storage.getClusterDao()).thenReturn(mockedClusterDao);

    ClusterFacade clusterFacade = mock(ClusterFacade.class);
    String cluster1 = "cluster1";
    String cluster2 = "cluster2";
    doReturn(Cluster.builder().withName(cluster1).withSeedHosts(ImmutableSet.of("127.0.0.1")).build()).when(
        mockedClusterDao).getCluster(eq(cluster1));
    doReturn(Cluster.builder().withName(cluster2).withSeedHosts(ImmutableSet.of("127.0.0.2")).build()).when(
        mockedClusterDao).getCluster(eq(cluster2));

    Map<UUID, RepairRun> repairRuns = Maps.newConcurrentMap();
    Map<UUID, RepairUnit> repairUnits = Maps.newConcurrentMap();

    Random random = new Random();
    // Create repairs for cluster1
    int clust1 = 0;
    for (clust1 = 0; clust1 < random.nextInt(10) + 2; clust1++) {
      RepairUnit repairUnit = createRepairUnit(cluster1);
      RepairRun repairRun = createRepairRun(context, clusterFacade, cluster1, repairUnit);
      repairUnits.put(repairUnit.getId(), repairUnit);
      repairRuns.put(repairRun.getId(), repairRun);
    }

    // Create repairs for cluster2
    int clust2 = 0;
    for (clust2 = 0; clust2 < random.nextInt(10) + 2; clust2++) {
      RepairUnit repairUnit = createRepairUnit(cluster2);
      RepairRun repairRun = createRepairRun(context, clusterFacade, cluster2, repairUnit);
      repairUnits.put(repairUnit.getId(), repairUnit);
      repairRuns.put(repairRun.getId(), repairRun);
    }

    IRepairUnitDao mockedRepairUnitDao = mock(IRepairUnitDao.class);
    Mockito.when(((CassandraStorageFacade) context.storage).getRepairUnitDao()).thenReturn(mockedRepairUnitDao);
    Mockito.when(mockedRepairUnitDao.getRepairUnit(any(UUID.class))).thenAnswer(
        new Answer<RepairUnit>() {
          @Override
          public RepairUnit answer(InvocationOnMock invocation) {
            return repairUnits.get(invocation.getArgument(0));
          }
        });

    when(context.storage.getRepairRunDao().getRepairRun(any(UUID.class))).thenAnswer(
        new Answer<Optional<RepairRun>>() {
          @Override
          public Optional<RepairRun> answer(InvocationOnMock invocation) {
            return Optional.of(repairRuns.get(invocation.getArgument(0)));
          }
        });
    RepairManager repairManager = RepairManager.create(
        context,
        Executors.newScheduledThreadPool(1),
        1,
        TimeUnit.MILLISECONDS,
        100,
        context.storage.getRepairRunDao());
    repairRuns.entrySet().stream().forEach(run -> {
      try {
        repairManager.startRepairRun(run.getValue());
      } catch (ReaperException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    });

    assertEquals(clust1, repairManager.countRepairRunnersForCluster(cluster1));
    assertEquals(clust2, repairManager.countRepairRunnersForCluster(cluster2));
  }

  private RepairUnit createRepairUnit(String clusterName) {
    final String ksName = "reaper";
    final Set<String> cfNames = Sets.newHashSet("reaper");
    final boolean incrementalRepair = false;
    final boolean subrangeIncremental = false;
    final Set<String> nodes = Sets.newHashSet("127.0.0.1");
    final Set<String> datacenters = Collections.emptySet();
    final int repairThreadCount = 1;
    final int segmentTimeout = 30;

    final RepairUnit repairUnit = RepairUnit.builder()
        .clusterName(clusterName)
        .keyspaceName(ksName)
        .columnFamilies(cfNames)
        .incrementalRepair(incrementalRepair)
        .subrangeIncrementalRepair(subrangeIncremental)
        .nodes(nodes)
        .datacenters(datacenters)
        .repairThreadCount(repairThreadCount)
        .timeout(segmentTimeout)
        .build(UUIDs.timeBased());

    return repairUnit;
  }

  private RepairRun createRepairRun(
      AppContext context,
      ClusterFacade clusterFacade,
      String clusterName,
      RepairUnit repairUnit) throws ReaperException {
    double intensity = 0.5f;

    final RepairRun run = RepairRun.builder(clusterName, repairUnit.getId())
        .intensity(intensity)
        .segmentCount(1)
        .repairParallelism(RepairParallelism.PARALLEL)
        .tables(TABLES)
        .build(UUIDs.timeBased());
    return run;
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