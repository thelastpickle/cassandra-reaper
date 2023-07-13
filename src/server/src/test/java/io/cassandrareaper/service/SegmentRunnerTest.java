/*
 * Copyright 2015-2017 Spotify AB
 * Copyright 2016-2019 The Last Pickle Ltd
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
import io.cassandrareaper.ReaperApplicationConfiguration.DatacenterAvailability;
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.CompactionStats;
import io.cassandrareaper.core.Node;
import io.cassandrareaper.core.RepairRun;
import io.cassandrareaper.core.RepairSegment;
import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.core.Segment;
import io.cassandrareaper.crypto.NoopCrypotograph;
import io.cassandrareaper.jmx.ClusterFacade;
import io.cassandrareaper.jmx.JmxConnectionFactory;
import io.cassandrareaper.jmx.JmxProxy;
import io.cassandrareaper.jmx.JmxProxyTest;
import io.cassandrareaper.jmx.RepairStatusHandler;
import io.cassandrareaper.storage.IStorage;
import io.cassandrareaper.storage.MemoryStorageFacade;
import io.cassandrareaper.storage.repairsegment.IRepairSegment;

import java.io.IOException;
import java.math.BigInteger;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import javax.management.MalformedObjectNameException;
import javax.management.ReflectionException;

import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import jersey.repackaged.com.google.common.collect.Maps;
import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageServiceMBean;
import org.apache.cassandra.utils.progress.ProgressEventType;
import org.apache.commons.lang3.mutable.MutableObject;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.apache.cassandra.repair.RepairParallelism.PARALLEL;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class SegmentRunnerTest {
  // TODO: Clean up tests. There's a lot of code duplication across these tests.

  private static final Set<String> TABLES = ImmutableSet.of("table1");
  private static final Set<String> COORDS = Collections.singleton("");

  @Before
  public void setUp() throws Exception {
    SegmentRunner.SEGMENT_RUNNERS.clear();
  }

  @Test
  public void timeoutTest() throws InterruptedException, ReaperException, ExecutionException,
      MalformedObjectNameException, ReflectionException, IOException {
    final AppContext context = new AppContext();
    final int segmentTimeout = 30;
    context.config = Mockito.mock(ReaperApplicationConfiguration.class);
    when(context.config.getJmxConnectionTimeoutInSeconds()).thenReturn(30);
    when(context.config.getDatacenterAvailability()).thenReturn(DatacenterAvailability.ALL);
    context.storage = new MemoryStorageFacade();

    RepairUnit cf = context.storage.addRepairUnit(
        RepairUnit.builder()
            .clusterName("reaper")
            .keyspaceName("reaper")
            .columnFamilies(Sets.newHashSet("reaper"))
            .incrementalRepair(false)
            .nodes(Sets.newHashSet("127.0.0.1"))
            .repairThreadCount(1)
            .timeout(segmentTimeout));

    Map<String, String> replicas = Maps.newHashMap();
    replicas.put("127.0.0.1", "dc1");
    RepairRun run = context.storage.getRepairRunDao().addRepairRun(
        RepairRun.builder("reaper", cf.getId())
            .intensity(0.5)
            .segmentCount(1)
            .repairParallelism(PARALLEL)
            .tables(TABLES),
        Collections.singleton(
            RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(new RingRange(BigInteger.ONE, BigInteger.ZERO))
                    .withReplicas(replicas)
                    .build(),
                cf.getId())));

    context.storage.addCluster(Cluster.builder()
        .withName(cf.getClusterName())
        .withPartitioner("Murmur3Partitioner")
        .withSeedHosts(cf.getNodes())
        .withJmxPort(7199)
        .withState(Cluster.State.ACTIVE)
        .build());

    final UUID runId = run.getId();
    final UUID segmentId = context.storage.getRepairSegmentDao().getNextFreeSegments(
        run.getId()).get(0).getId();

    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final MutableObject<Future<?>> future = new MutableObject<>();

    final JmxProxy jmx = JmxProxyTest.mockJmxProxyImpl();
    when(jmx.getClusterName()).thenReturn("reaper");
    when(jmx.isConnectionAlive()).thenReturn(true);

    EndpointSnitchInfoMBean endpointSnitchInfoMBean = mock(EndpointSnitchInfoMBean.class);
    when(endpointSnitchInfoMBean.getDatacenter()).thenReturn("dc1");
    try {
      when(endpointSnitchInfoMBean.getDatacenter(anyString())).thenReturn("dc1");
    } catch (UnknownHostException ex) {
      throw new AssertionError(ex);
    }
    JmxProxyTest.mockGetEndpointSnitchInfoMBean(jmx, endpointSnitchInfoMBean);

    when(jmx.triggerRepair(any(), any(), any(), any(), any(), anyBoolean(), any(), any(), any(), anyInt()))
        .then(
            (invocation) -> {
              assertEquals(
                  RepairSegment.State.STARTED,
                  context.storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState());

              future.setValue(
                  executor.submit(
                      new Thread() {
                        @Override
                        public void run() {
                          ((RepairStatusHandler) invocation.getArgument(7))
                              .handle(
                                  1,
                                  Optional.of(ActiveRepairService.Status.STARTED),
                                  Optional.empty(),
                                  "Repair command 1 has started",
                                  jmx);

                          assertEquals(
                              RepairSegment.State.RUNNING,
                              context.storage.getRepairSegmentDao().getRepairSegment(runId,
                                  segmentId).get().getState());
                        }
                      }));
              return 1;
            });

    context.jmxConnectionFactory = new JmxConnectionFactory(context, new NoopCrypotograph()) {
      @Override
      public JmxProxy connectImpl(Node host) throws ReaperException {
        return jmx;
      }
    };

    RepairRunner rr = mock(RepairRunner.class);
    RepairUnit ru = mock(RepairUnit.class);
    when(ru.getKeyspaceName()).thenReturn("reaper");

    ClusterFacade clusterFacade = mock(ClusterFacade.class);
    when(clusterFacade.connect(any(Cluster.class), any())).thenReturn(jmx);
    when(clusterFacade.nodeIsAccessibleThroughJmx(any(), any())).thenReturn(true);

    when(clusterFacade.tokenRangeToEndpoint(any(), anyString(), any()))
        .thenReturn(Lists.newArrayList(cf.getNodes()));

    when(clusterFacade.nodeIsAccessibleThroughJmx(any(), any())).thenReturn(true);
    when(clusterFacade.listActiveCompactions(any())).thenReturn(CompactionStats.builder().withActiveCompactions(
        Collections.emptyList()).withPendingCompactions(Optional.of(0)).build());

    SegmentRunner sr = SegmentRunner
        .create(context, clusterFacade, segmentId, COORDS, 100, 0.5, PARALLEL, "reaper", ru, TABLES, rr);

    sr.run();

    future.getValue().get();
    executor.shutdown();

    assertEquals(RepairSegment.State.NOT_STARTED,
        context.storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState());
    assertEquals(1, context.storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getFailCount());
  }

  @Test
  public void successTest() throws InterruptedException, ReaperException,
      ExecutionException, MalformedObjectNameException,
      ReflectionException, IOException {
    final IStorage storage = new MemoryStorageFacade();
    final int segmentTimeout = 30;
    RepairUnit cf = storage.addRepairUnit(
        RepairUnit.builder()
            .clusterName("reaper")
            .keyspaceName("reaper")
            .columnFamilies(Sets.newHashSet("reaper"))
            .incrementalRepair(false)
            .nodes(Sets.newHashSet("127.0.0.1"))
            .repairThreadCount(1)
            .timeout(segmentTimeout));

    Map<String, String> replicas = Maps.newHashMap();
    replicas.put("127.0.0.1", "dc1");
    RepairRun run = storage.getRepairRunDao().addRepairRun(
        RepairRun.builder("reaper", cf.getId())
            .intensity(0.5)
            .segmentCount(1)
            .repairParallelism(PARALLEL)
            .tables(TABLES),
        Collections.singleton(
            RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(new RingRange(BigInteger.ONE, BigInteger.ZERO))
                    .withReplicas(replicas)
                    .build(),
                cf.getId())));
    storage.addCluster(Cluster.builder()
        .withName(cf.getClusterName())
        .withPartitioner("Murmur3Partitioner")
        .withSeedHosts(cf.getNodes())
        .withJmxPort(7199)
        .withState(Cluster.State.ACTIVE)
        .build());

    final UUID runId = run.getId();
    final UUID segmentId = storage.getRepairSegmentDao().getNextFreeSegments(run.getId()).get(0).getId();
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final MutableObject<Future<?>> future = new MutableObject<>();
    AppContext context = new AppContext();
    context.storage = storage;
    context.config = Mockito.mock(ReaperApplicationConfiguration.class);
    when(context.config.getJmxConnectionTimeoutInSeconds()).thenReturn(30);
    when(context.config.getDatacenterAvailability()).thenReturn(DatacenterAvailability.ALL);
    final JmxProxy jmx = JmxProxyTest.mockJmxProxyImpl();
    when(jmx.getClusterName()).thenReturn("reaper");
    when(jmx.isConnectionAlive()).thenReturn(true);
    EndpointSnitchInfoMBean endpointSnitchInfoMBean = mock(EndpointSnitchInfoMBean.class);
    when(endpointSnitchInfoMBean.getDatacenter()).thenReturn("dc1");
    try {
      when(endpointSnitchInfoMBean.getDatacenter(anyString())).thenReturn("dc1");
    } catch (UnknownHostException ex) {
      throw new AssertionError(ex);
    }
    JmxProxyTest.mockGetEndpointSnitchInfoMBean(jmx, endpointSnitchInfoMBean);
    when(jmx.triggerRepair(any(), any(), any(), any(), any(), anyBoolean(), any(), any(), any(), anyInt()))
        .then(
            (invocation) -> {
              assertEquals(
                  RepairSegment.State.STARTED,
                  storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState());

              future.setValue(
                  executor.submit(
                      () -> {
                        ((RepairStatusHandler) invocation.getArgument(7))
                            .handle(
                                1,
                                Optional.of(ActiveRepairService.Status.STARTED),
                                Optional.empty(),
                                "Repair command 1 has started",
                                jmx);

                        assertEquals(
                            RepairSegment.State.RUNNING,
                            storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState());

                        // test an unrelated repair. Should throw exception
                        try {
                          ((RepairStatusHandler) invocation.getArgument(7))
                              .handle(
                                  2,
                                  Optional.of(ActiveRepairService.Status.SESSION_FAILED),
                                  Optional.empty(),
                                  "Repair command 2 has failed",
                                  jmx);
                          throw new AssertionError("illegal handle of wrong repairNo");
                        } catch (IllegalArgumentException ignore) {
                        }

                        ((RepairStatusHandler) invocation.getArgument(7))
                            .handle(
                                1,
                                Optional.of(ActiveRepairService.Status.SESSION_SUCCESS),
                                Optional.empty(),
                                "Repair session succeeded in command 1",
                                jmx);

                        assertEquals(
                            RepairSegment.State.DONE,
                            storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState());

                        ((RepairStatusHandler) invocation.getArgument(7))
                            .handle(
                                1,
                                Optional.of(ActiveRepairService.Status.FINISHED),
                                Optional.empty(),
                                "Repair command 1 has finished",
                                jmx);

                        assertEquals(
                            RepairSegment.State.DONE,
                            storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState());
                      }));
              return 1;
            });
    context.jmxConnectionFactory = new JmxConnectionFactory(context, new NoopCrypotograph()) {
      @Override
      protected JmxProxy connectImpl(Node host) throws ReaperException {
        return jmx;
      }
    };

    RepairRunner rr = mock(RepairRunner.class);
    RepairUnit ru = mock(RepairUnit.class);
    when(ru.getKeyspaceName()).thenReturn("reaper");

    ClusterFacade clusterFacade = mock(ClusterFacade.class);
    when(clusterFacade.connect(any(Cluster.class), any())).thenReturn(jmx);
    when(clusterFacade.nodeIsAccessibleThroughJmx(any(), any())).thenReturn(true);

    when(clusterFacade.tokenRangeToEndpoint(any(), anyString(), any()))
        .thenReturn(Lists.newArrayList(cf.getNodes()));
    when(clusterFacade.listActiveCompactions(any())).thenReturn(CompactionStats.builder().withActiveCompactions(
        Collections.emptyList()).withPendingCompactions(Optional.of(0)).build());

    SegmentRunner sr = SegmentRunner
        .create(context, clusterFacade, segmentId, COORDS, 5000, 0.5, PARALLEL, "reaper", ru, TABLES, rr);

    sr.run();

    future.getValue().get();
    executor.shutdown();

    assertEquals(RepairSegment.State.DONE,
        storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState());
    assertEquals(0, storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getFailCount());
  }

  @Test
  public void failureTest() throws InterruptedException, ReaperException, ExecutionException,
      MalformedObjectNameException, ReflectionException, IOException {
    final IStorage storage = new MemoryStorageFacade();
    final int segmentTimeout = 30;

    RepairUnit cf = storage.addRepairUnit(
        RepairUnit.builder()
            .clusterName("reaper")
            .keyspaceName("reaper")
            .columnFamilies(Sets.newHashSet("reaper"))
            .incrementalRepair(false)
            .nodes(Sets.newHashSet("127.0.0.1"))
            .repairThreadCount(1)
            .timeout(segmentTimeout));

    Map<String, String> replicas = Maps.newHashMap();
    replicas.put("127.0.0.1", "dc1");
    RepairRun run = storage.getRepairRunDao().addRepairRun(
        RepairRun.builder("reaper", cf.getId())
            .intensity(0.5)
            .segmentCount(1)
            .repairParallelism(PARALLEL)
            .tables(TABLES),
        Collections.singleton(
            RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(new RingRange(BigInteger.ONE, BigInteger.ZERO))
                    .withReplicas(replicas)
                    .build(),
                cf.getId())));

    storage.addCluster(Cluster.builder()
        .withName(cf.getClusterName())
        .withPartitioner("Murmur3Partitioner")
        .withSeedHosts(cf.getNodes())
        .withJmxPort(7199)
        .withState(Cluster.State.ACTIVE)
        .build());

    final UUID runId = run.getId();
    final UUID segmentId = storage.getRepairSegmentDao().getNextFreeSegments(run.getId()).get(0).getId();

    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final MutableObject<Future<?>> future = new MutableObject<>();

    AppContext context = new AppContext();
    context.storage = storage;
    context.config = Mockito.mock(ReaperApplicationConfiguration.class);
    when(context.config.getJmxConnectionTimeoutInSeconds()).thenReturn(30);
    when(context.config.getDatacenterAvailability()).thenReturn(DatacenterAvailability.ALL);

    final JmxProxy jmx = JmxProxyTest.mockJmxProxyImpl();
    when(jmx.getClusterName()).thenReturn("reaper");
    when(jmx.isConnectionAlive()).thenReturn(true);

    EndpointSnitchInfoMBean endpointSnitchInfoMBean = mock(EndpointSnitchInfoMBean.class);
    when(endpointSnitchInfoMBean.getDatacenter()).thenReturn("dc1");
    try {
      when(endpointSnitchInfoMBean.getDatacenter(anyString())).thenReturn("dc1");
    } catch (UnknownHostException ex) {
      throw new AssertionError(ex);
    }
    JmxProxyTest.mockGetEndpointSnitchInfoMBean(jmx, endpointSnitchInfoMBean);

    when(jmx.triggerRepair(any(), any(), any(), any(), any(), anyBoolean(), any(), any(), any(), anyInt()))
        .then(
            (invocation) -> {
              assertEquals(
                  RepairSegment.State.STARTED,
                  storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState());

              future.setValue(
                  executor.submit(
                      () -> {
                        ((RepairStatusHandler) invocation.getArgument(7))
                            .handle(
                                1,
                                Optional.of(ActiveRepairService.Status.STARTED),
                                Optional.empty(),
                                "Repair command 1 has started",
                                jmx);

                        assertEquals(
                            RepairSegment.State.RUNNING,
                            storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState());

                        ((RepairStatusHandler) invocation.getArgument(7))
                            .handle(
                                1,
                                Optional.of(ActiveRepairService.Status.SESSION_FAILED),
                                Optional.empty(),
                                "Repair command 1 has failed",
                                jmx);

                        assertEquals(
                            RepairSegment.State.NOT_STARTED,
                            storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState());

                        ((RepairStatusHandler) invocation.getArgument(7))
                            .handle(
                                1,
                                Optional.of(ActiveRepairService.Status.FINISHED),
                                Optional.empty(),
                                "Repair command 1 has finished",
                                jmx);

                        assertEquals(
                            RepairSegment.State.NOT_STARTED,
                            storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState());
                      }));

              return 1;
            });

    context.jmxConnectionFactory = new JmxConnectionFactory(context, new NoopCrypotograph()) {
      @Override
      protected JmxProxy connectImpl(Node host) throws ReaperException {
        return jmx;
      }
    };

    RepairRunner rr = mock(RepairRunner.class);
    RepairUnit ru = mock(RepairUnit.class);
    when(ru.getKeyspaceName()).thenReturn("reaper");

    ClusterFacade clusterFacade = mock(ClusterFacade.class);
    when(clusterFacade.connect(any(Cluster.class), any())).thenReturn(jmx);
    when(clusterFacade.nodeIsAccessibleThroughJmx(any(), any())).thenReturn(true);

    when(clusterFacade.tokenRangeToEndpoint(any(), anyString(), any()))
        .thenReturn(Lists.newArrayList(cf.getNodes()));
    when(clusterFacade.listActiveCompactions(any())).thenReturn(CompactionStats.builder().withActiveCompactions(
        Collections.emptyList()).withPendingCompactions(Optional.of(0)).build());

    SegmentRunner sr = SegmentRunner
        .create(context, clusterFacade, segmentId, COORDS, 5000, 0.5, PARALLEL, "reaper", ru, TABLES, rr);

    sr.run();

    future.getValue().get();
    executor.shutdown();

    assertEquals(RepairSegment.State.NOT_STARTED,
        storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState());
    assertEquals(2, storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getFailCount());
  }

  @Test
  public void outOfOrderSuccessCass21Test()
      throws InterruptedException, ReaperException, ExecutionException,
      MalformedObjectNameException, ReflectionException, IOException {
    final IStorage storage = new MemoryStorageFacade();

    RepairUnit cf = storage.addRepairUnit(
        RepairUnit.builder()
            .clusterName("reaper")
            .keyspaceName("reaper")
            .columnFamilies(Sets.newHashSet("reaper"))
            .incrementalRepair(false)
            .nodes(Sets.newHashSet("127.0.0.1"))
            .repairThreadCount(1)
            .timeout(30));

    Map<String, String> replicas = Maps.newHashMap();
    replicas.put("127.0.0.1", "dc1");
    RepairRun run = storage.getRepairRunDao().addRepairRun(
        RepairRun.builder("reaper", cf.getId())
            .intensity(0.5)
            .segmentCount(1)
            .repairParallelism(PARALLEL)
            .tables(TABLES),
        Collections.singleton(
            RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(new RingRange(BigInteger.ONE, BigInteger.ZERO))
                    .withReplicas(replicas)
                    .build(),
                cf.getId())));

    storage.addCluster(Cluster.builder()
        .withName(cf.getClusterName())
        .withPartitioner("Murmur3Partitioner")
        .withSeedHosts(cf.getNodes())
        .withJmxPort(7199)
        .withState(Cluster.State.ACTIVE)
        .build());

    final UUID runId = run.getId();
    final UUID segmentId = storage.getRepairSegmentDao().getNextFreeSegments(run.getId()).get(0).getId();

    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final MutableObject<Future<?>> future = new MutableObject<>();

    AppContext context = new AppContext();
    context.storage = storage;
    context.config = Mockito.mock(ReaperApplicationConfiguration.class);
    when(context.config.getJmxConnectionTimeoutInSeconds()).thenReturn(30);
    when(context.config.getDatacenterAvailability()).thenReturn(DatacenterAvailability.ALL);

    final JmxProxy jmx = JmxProxyTest.mockJmxProxyImpl();
    when(jmx.getClusterName()).thenReturn("reaper");
    when(jmx.isConnectionAlive()).thenReturn(true);

    EndpointSnitchInfoMBean endpointSnitchInfoMBean = mock(EndpointSnitchInfoMBean.class);
    when(endpointSnitchInfoMBean.getDatacenter()).thenReturn("dc1");
    try {
      when(endpointSnitchInfoMBean.getDatacenter(anyString())).thenReturn("dc1");
    } catch (UnknownHostException ex) {
      throw new AssertionError(ex);
    }
    JmxProxyTest.mockGetEndpointSnitchInfoMBean(jmx, endpointSnitchInfoMBean);

    when(jmx.triggerRepair(any(), any(), any(), any(), any(), anyBoolean(), any(), any(), any(), anyInt()))
        .then(
            invocation -> {
              assertEquals(
                  RepairSegment.State.STARTED,
                  storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState());

              future.setValue(
                  executor.submit(
                      () -> {
                        ((RepairStatusHandler) invocation.getArgument(7))
                            .handle(
                                1,
                                Optional.of(ActiveRepairService.Status.STARTED),
                                Optional.empty(),
                                "Repair command 1 has started",
                                jmx);

                        ((RepairStatusHandler) invocation.getArgument(7))
                            .handle(
                                1,
                                Optional.of(ActiveRepairService.Status.FINISHED),
                                Optional.empty(),
                                "Repair command 1 has finished",
                                jmx);

                        assertEquals(
                            RepairSegment.State.RUNNING,
                            storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState());

                        ((RepairStatusHandler) invocation.getArgument(7))
                            .handle(
                                1,
                                Optional.of(ActiveRepairService.Status.SESSION_SUCCESS),
                                Optional.empty(),
                                "Repair session succeeded in command 1",
                                jmx);

                        assertEquals(
                            RepairSegment.State.DONE,
                            storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState());
                      }));
              return 1;
            });

    context.jmxConnectionFactory = new JmxConnectionFactory(context, new NoopCrypotograph()) {
      @Override
      protected JmxProxy connectImpl(Node host) throws ReaperException {
        return jmx;
      }
    };

    RepairRunner rr = mock(RepairRunner.class);
    RepairUnit ru = mock(RepairUnit.class);
    when(ru.getKeyspaceName()).thenReturn("reaper");

    ClusterFacade clusterFacade = mock(ClusterFacade.class);
    when(clusterFacade.connect(any(Cluster.class), any())).thenReturn(jmx);
    when(clusterFacade.nodeIsAccessibleThroughJmx(any(), any())).thenReturn(true);

    when(clusterFacade.tokenRangeToEndpoint(any(), anyString(), any()))
        .thenReturn(Lists.newArrayList(cf.getNodes()));
    when(clusterFacade.listActiveCompactions(any())).thenReturn(CompactionStats.builder().withActiveCompactions(
        Collections.emptyList()).withPendingCompactions(Optional.of(0)).build());

    SegmentRunner sr = SegmentRunner
        .create(context, clusterFacade, segmentId, COORDS, 5000, 0.5, PARALLEL, "reaper", ru, TABLES, rr);

    sr.run();

    future.getValue().get();
    executor.shutdown();

    assertEquals(
        RepairSegment.State.DONE, storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState());
    assertEquals(0, storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getFailCount());
  }

  @Test
  public void outOfOrderSuccessCass22Test()
      throws InterruptedException, ReaperException, ExecutionException,
      MalformedObjectNameException, ReflectionException, IOException {
    final IStorage storage = new MemoryStorageFacade();

    RepairUnit cf = storage.addRepairUnit(
        RepairUnit.builder()
            .clusterName("reaper")
            .keyspaceName("reaper")
            .columnFamilies(Sets.newHashSet("reaper"))
            .incrementalRepair(false)
            .nodes(Sets.newHashSet("127.0.0.1"))
            .repairThreadCount(1)
            .timeout(30));

    Map<String, String> replicas = Maps.newHashMap();
    replicas.put("127.0.0.1", "dc1");
    RepairRun run = storage.getRepairRunDao().addRepairRun(
        RepairRun.builder("reaper", cf.getId())
            .intensity(0.5)
            .segmentCount(1)
            .repairParallelism(PARALLEL)
            .tables(TABLES),
        Collections.singleton(
            RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(new RingRange(BigInteger.ONE, BigInteger.ZERO))
                    .withReplicas(replicas)
                    .build(),
                cf.getId())));

    storage.addCluster(Cluster.builder()
        .withName(cf.getClusterName())
        .withPartitioner("Murmur3Partitioner")
        .withSeedHosts(cf.getNodes())
        .withJmxPort(7199)
        .withState(Cluster.State.ACTIVE)
        .build());

    final UUID runId = run.getId();
    final UUID segmentId = storage.getRepairSegmentDao().getNextFreeSegments(run.getId()).get(0).getId();

    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final MutableObject<Future<?>> future = new MutableObject<>();

    AppContext context = new AppContext();
    context.storage = storage;
    context.config = Mockito.mock(ReaperApplicationConfiguration.class);
    when(context.config.getJmxConnectionTimeoutInSeconds()).thenReturn(30);
    when(context.config.getDatacenterAvailability()).thenReturn(DatacenterAvailability.ALL);

    final JmxProxy jmx = JmxProxyTest.mockJmxProxyImpl();
    when(jmx.getClusterName()).thenReturn("reaper");
    when(jmx.isConnectionAlive()).thenReturn(true);

    EndpointSnitchInfoMBean endpointSnitchInfoMBean = mock(EndpointSnitchInfoMBean.class);
    when(endpointSnitchInfoMBean.getDatacenter()).thenReturn("dc1");
    try {
      when(endpointSnitchInfoMBean.getDatacenter(anyString())).thenReturn("dc1");
    } catch (UnknownHostException ex) {
      throw new AssertionError(ex);
    }
    JmxProxyTest.mockGetEndpointSnitchInfoMBean(jmx, endpointSnitchInfoMBean);

    when(jmx.triggerRepair(any(), any(), any(), any(), any(), anyBoolean(), any(), any(), any(), anyInt()))
        .then(
            invocation -> {
              assertEquals(
                  RepairSegment.State.STARTED,
                  storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState());

              future.setValue(
                  executor.submit(
                      () -> {
                        ((RepairStatusHandler) invocation.getArgument(7))
                            .handle(
                                1,
                                Optional.empty(),
                                Optional.of(ProgressEventType.START),
                                "Repair command 1 has started",
                                jmx);

                        ((RepairStatusHandler) invocation.getArgument(7))
                            .handle(
                                1,
                                Optional.empty(),
                                Optional.of(ProgressEventType.COMPLETE),
                                "Repair command 1 has finished",
                                jmx);

                        assertEquals(
                            RepairSegment.State.RUNNING,
                            storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState());

                        ((RepairStatusHandler) invocation.getArgument(7))
                            .handle(
                                1,
                                Optional.empty(),
                                Optional.of(ProgressEventType.SUCCESS),
                                "Repair session succeeded in command 1",
                                jmx);

                        assertEquals(
                            RepairSegment.State.DONE,
                            storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState());
                      }));
              return 1;
            });

    context.jmxConnectionFactory = new JmxConnectionFactory(context, new NoopCrypotograph()) {
      @Override
      protected JmxProxy connectImpl(Node host) throws ReaperException {
        return jmx;
      }
    };

    RepairRunner rr = mock(RepairRunner.class);
    RepairUnit ru = mock(RepairUnit.class);
    when(ru.getKeyspaceName()).thenReturn("reaper");

    ClusterFacade clusterFacade = mock(ClusterFacade.class);
    when(clusterFacade.connect(any(Cluster.class), any())).thenReturn(jmx);
    when(clusterFacade.nodeIsAccessibleThroughJmx(any(), any())).thenReturn(true);

    when(clusterFacade.tokenRangeToEndpoint(any(), anyString(), any()))
        .thenReturn(Lists.newArrayList(cf.getNodes()));
    when(clusterFacade.listActiveCompactions(any())).thenReturn(CompactionStats.builder().withActiveCompactions(
        Collections.emptyList()).withPendingCompactions(Optional.of(0)).build());

    SegmentRunner sr = SegmentRunner
        .create(context, clusterFacade, segmentId, COORDS, 5000, 0.5, PARALLEL, "reaper", ru, TABLES, rr);

    sr.run();

    future.getValue().get();
    executor.shutdown();

    assertEquals(
        RepairSegment.State.DONE, storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState());

    assertEquals(0, storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getFailCount());
  }

  @Test
  public void outOfOrderFailureCass21Test()
      throws InterruptedException, ReaperException, ExecutionException,
      MalformedObjectNameException, ReflectionException, IOException {
    final IStorage storage = new MemoryStorageFacade();
    final int segmentTimeout = 30;

    RepairUnit cf = storage.addRepairUnit(
        RepairUnit.builder()
            .clusterName("reaper")
            .keyspaceName("reaper")
            .columnFamilies(Sets.newHashSet("reaper"))
            .incrementalRepair(false)
            .nodes(Sets.newHashSet("127.0.0.1"))
            .repairThreadCount(1)
            .timeout(30));

    Map<String, String> replicas = Maps.newHashMap();
    replicas.put("127.0.0.1", "dc1");
    RepairRun run = storage.getRepairRunDao().addRepairRun(
        RepairRun.builder("reaper", cf.getId())
            .intensity(0.5)
            .segmentCount(1)
            .repairParallelism(PARALLEL)
            .tables(TABLES),
        Collections.singleton(
            RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(new RingRange(BigInteger.ONE, BigInteger.ZERO))
                    .withReplicas(replicas)
                    .build(),
                cf.getId())));

    storage.addCluster(Cluster.builder()
        .withName(cf.getClusterName())
        .withPartitioner("Murmur3Partitioner")
        .withSeedHosts(cf.getNodes())
        .withJmxPort(7199)
        .withState(Cluster.State.ACTIVE)
        .build());

    final UUID runId = run.getId();
    final UUID segmentId = storage.getRepairSegmentDao().getNextFreeSegments(run.getId()).get(0).getId();

    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final MutableObject<Future<?>> future = new MutableObject<>();

    AppContext context = new AppContext();
    context.storage = storage;
    context.config = Mockito.mock(ReaperApplicationConfiguration.class);
    when(context.config.getJmxConnectionTimeoutInSeconds()).thenReturn(30);
    when(context.config.getDatacenterAvailability()).thenReturn(DatacenterAvailability.ALL);

    final JmxProxy jmx = JmxProxyTest.mockJmxProxyImpl();
    when(jmx.getClusterName()).thenReturn("reaper");
    when(jmx.isConnectionAlive()).thenReturn(true);

    EndpointSnitchInfoMBean endpointSnitchInfoMBean = mock(EndpointSnitchInfoMBean.class);
    when(endpointSnitchInfoMBean.getDatacenter()).thenReturn("dc1");
    try {
      when(endpointSnitchInfoMBean.getDatacenter(anyString())).thenReturn("dc1");
    } catch (UnknownHostException ex) {
      throw new AssertionError(ex);
    }
    JmxProxyTest.mockGetEndpointSnitchInfoMBean(jmx, endpointSnitchInfoMBean);

    when(jmx.triggerRepair(any(), any(), any(), any(), any(), anyBoolean(), any(), any(), any(), anyInt()))
        .then(
            invocation -> {
              assertEquals(
                  RepairSegment.State.STARTED,
                  storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState());

              future.setValue(
                  executor.submit(
                      () -> {
                        ((RepairStatusHandler) invocation.getArgument(7))
                            .handle(
                                1,
                                Optional.of(ActiveRepairService.Status.STARTED),
                                Optional.empty(),
                                "Repair command 1 has started",
                                jmx);

                        ((RepairStatusHandler) invocation.getArgument(7))
                            .handle(
                                1,
                                Optional.of(ActiveRepairService.Status.FINISHED),
                                Optional.empty(),
                                "Repair command 1 has finished",
                                jmx);

                        assertEquals(
                            RepairSegment.State.RUNNING,
                            storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState());

                        ((RepairStatusHandler) invocation.getArgument(7))
                            .handle(
                                1,
                                Optional.of(ActiveRepairService.Status.SESSION_FAILED),
                                Optional.empty(),
                                "Repair session succeeded in command 1",
                                jmx);

                        assertEquals(
                            RepairSegment.State.NOT_STARTED,
                            storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState());
                      }));
              return 1;
            });

    context.jmxConnectionFactory = new JmxConnectionFactory(context, new NoopCrypotograph()) {
      @Override
      protected JmxProxy connectImpl(Node host) throws ReaperException {
        return jmx;
      }
    };

    RepairRunner rr = mock(RepairRunner.class);
    RepairUnit ru = mock(RepairUnit.class);
    when(ru.getKeyspaceName()).thenReturn("reaper");

    ClusterFacade clusterFacade = mock(ClusterFacade.class);
    when(clusterFacade.connect(any(Cluster.class), any())).thenReturn(jmx);
    when(clusterFacade.nodeIsAccessibleThroughJmx(any(), any())).thenReturn(true);

    when(clusterFacade.tokenRangeToEndpoint(any(), anyString(), any()))
        .thenReturn(Lists.newArrayList(cf.getNodes()));
    when(clusterFacade.listActiveCompactions(any())).thenReturn(CompactionStats.builder().withActiveCompactions(
        Collections.emptyList()).withPendingCompactions(Optional.of(0)).build());

    SegmentRunner sr = SegmentRunner
        .create(context, clusterFacade, segmentId, COORDS, 5000, 0.5, PARALLEL, "reaper", ru, TABLES, rr);

    sr.run();

    future.getValue().get();
    executor.shutdown();

    assertEquals(
        RepairSegment.State.NOT_STARTED,
        storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState());

    assertEquals(2, storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getFailCount());
  }

  @Test
  public void outOfOrderFailureTestCass22()
      throws InterruptedException, ReaperException, ExecutionException,
      MalformedObjectNameException, ReflectionException, IOException {
    final IStorage storage = new MemoryStorageFacade();
    final int segmentTimeout = 30;

    RepairUnit cf = storage.addRepairUnit(
        RepairUnit.builder()
            .clusterName("reaper")
            .keyspaceName("reaper")
            .columnFamilies(Sets.newHashSet("reaper"))
            .incrementalRepair(false)
            .nodes(Sets.newHashSet("127.0.0.1"))
            .repairThreadCount(1)
            .timeout(30));

    Map<String, String> replicas = Maps.newHashMap();
    replicas.put("127.0.0.1", "dc1");
    RepairRun run = storage.getRepairRunDao().addRepairRun(
        RepairRun.builder("reaper", cf.getId())
            .intensity(0.5)
            .segmentCount(1)
            .repairParallelism(PARALLEL)
            .tables(TABLES),
        Collections.singleton(
            RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(new RingRange(BigInteger.ONE, BigInteger.ZERO))
                    .withReplicas(replicas)
                    .build(),
                cf.getId())));

    storage.addCluster(Cluster.builder()
        .withName(cf.getClusterName())
        .withPartitioner("Murmur3Partitioner")
        .withSeedHosts(cf.getNodes())
        .withJmxPort(7199)
        .withState(Cluster.State.ACTIVE)
        .build());

    final UUID runId = run.getId();
    final UUID segmentId = storage.getRepairSegmentDao().getNextFreeSegments(run.getId()).get(0).getId();

    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final MutableObject<Future<?>> future = new MutableObject<>();

    AppContext context = new AppContext();
    context.storage = storage;
    context.config = Mockito.mock(ReaperApplicationConfiguration.class);
    when(context.config.getJmxConnectionTimeoutInSeconds()).thenReturn(30);
    when(context.config.getDatacenterAvailability()).thenReturn(DatacenterAvailability.ALL);

    final JmxProxy jmx = JmxProxyTest.mockJmxProxyImpl();
    when(jmx.getClusterName()).thenReturn("reaper");
    when(jmx.isConnectionAlive()).thenReturn(true);

    EndpointSnitchInfoMBean endpointSnitchInfoMBean = mock(EndpointSnitchInfoMBean.class);
    when(endpointSnitchInfoMBean.getDatacenter()).thenReturn("dc1");
    try {
      when(endpointSnitchInfoMBean.getDatacenter(anyString())).thenReturn("dc1");
    } catch (UnknownHostException ex) {
      throw new AssertionError(ex);
    }
    JmxProxyTest.mockGetEndpointSnitchInfoMBean(jmx, endpointSnitchInfoMBean);

    when(jmx.triggerRepair(any(), any(), any(), any(), any(), anyBoolean(), any(), any(), any(), anyInt()))
        .then(
            invocation -> {
              assertEquals(
                  RepairSegment.State.STARTED,
                  storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState());

              future.setValue(
                  executor.submit(
                      () -> {
                        ((RepairStatusHandler) invocation.getArgument(7))
                            .handle(
                                1,
                                Optional.empty(),
                                Optional.of(ProgressEventType.START),
                                "Repair command 1 has started",
                                jmx);

                        ((RepairStatusHandler) invocation.getArgument(7))
                            .handle(
                                1,
                                Optional.empty(),
                                Optional.of(ProgressEventType.COMPLETE),
                                "Repair command 1 has finished",
                                jmx);

                        assertEquals(
                            RepairSegment.State.RUNNING,
                            storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState());

                        ((RepairStatusHandler) invocation.getArgument(7))
                            .handle(
                                1,
                                Optional.empty(),
                                Optional.of(ProgressEventType.ERROR),
                                "Repair session succeeded in command 1",
                                jmx);

                        assertEquals(
                            RepairSegment.State.NOT_STARTED,
                            storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState());
                      }));
              return 1;
            });

    context.jmxConnectionFactory = new JmxConnectionFactory(context, new NoopCrypotograph()) {
      @Override
      protected JmxProxy connectImpl(Node host) throws ReaperException {
        return jmx;
      }
    };

    RepairRunner rr = mock(RepairRunner.class);
    RepairUnit ru = mock(RepairUnit.class);
    when(ru.getKeyspaceName()).thenReturn("reaper");

    ClusterFacade clusterFacade = mock(ClusterFacade.class);
    when(clusterFacade.connect(any(Cluster.class), any())).thenReturn(jmx);
    when(clusterFacade.nodeIsAccessibleThroughJmx(any(), any())).thenReturn(true);

    when(clusterFacade.tokenRangeToEndpoint(any(), anyString(), any()))
        .thenReturn(Lists.newArrayList(cf.getNodes()));
    when(clusterFacade.listActiveCompactions(any())).thenReturn(CompactionStats.builder().withActiveCompactions(
        Collections.emptyList()).withPendingCompactions(Optional.of(0)).build());

    SegmentRunner sr = SegmentRunner
        .create(context, clusterFacade, segmentId, COORDS, 5000, 0.5, PARALLEL, "reaper", ru, TABLES, rr);

    sr.run();

    future.getValue().get();
    executor.shutdown();

    assertEquals(
        RepairSegment.State.NOT_STARTED,
        storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState());

    assertEquals(2, storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getFailCount());
  }

  @Test
  public void parseRepairIdTest() {
    String msg = "Repair session 883fd090-12f1-11e5-94c5-03d4762e50b7 for range (1,2] failed with";
    assertEquals("883fd090-12f1-11e5-94c5-03d4762e50b7", SegmentRunner.parseRepairId(msg));
    msg = "Two IDs: 883fd090-12f1-11e5-94c5-03d4762e50b7 883fd090-12f1-11e5-94c5-03d4762e50b7";
    assertEquals("883fd090-12f1-11e5-94c5-03d4762e50b7", SegmentRunner.parseRepairId(msg));
    msg = "No ID: foo bar baz";
    assertEquals(null, SegmentRunner.parseRepairId(msg));
    msg = "A message with bad ID 883fd090-fooo-11e5-94c5-03d4762e50b7";
    assertEquals(null, SegmentRunner.parseRepairId(msg));
    msg = "A message with good ID 883fd090-baad-11e5-94c5-03d4762e50b7";
    assertEquals("883fd090-baad-11e5-94c5-03d4762e50b7", SegmentRunner.parseRepairId(msg));
  }

  @Test(expected = ReaperException.class)
  public void alreadyRunningSegmentRunnerCreationFailure() throws ReaperException {
    UUID segmentId = UUIDs.timeBased();
    SegmentRunner.SEGMENT_RUNNERS.put(segmentId, mock(SegmentRunner.class));
    SegmentRunner.create(
        mock(AppContext.class),
        mock(ClusterFacade.class),
        segmentId,
        Collections.emptyList(),
        1,
        1,
        RepairParallelism.DATACENTER_AWARE,
        "clusterName",
        mock(RepairUnit.class),
        Collections.emptySet(),
        mock(RepairRunner.class));
  }

  @Test
  public void triggerFailureTest() throws InterruptedException, ReaperException, ExecutionException,
      MalformedObjectNameException, ReflectionException, IOException {
    final IStorage storage = new MemoryStorageFacade();
    final int segmentTimeout = 30;

    RepairUnit cf = storage.addRepairUnit(
        RepairUnit.builder()
            .clusterName("reaper")
            .keyspaceName("reaper")
            .columnFamilies(Sets.newHashSet("reaper"))
            .incrementalRepair(false)
            .nodes(Sets.newHashSet("127.0.0.1"))
            .repairThreadCount(1)
            .timeout(segmentTimeout));

    Map<String, String> replicas = Maps.newHashMap();
    replicas.put("127.0.0.1", "dc1");
    RepairRun run = storage.getRepairRunDao().addRepairRun(
        RepairRun.builder("reaper", cf.getId())
            .intensity(0.5)
            .segmentCount(1)
            .repairParallelism(PARALLEL)
            .tables(TABLES),
        Collections.singleton(
            RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(new RingRange(BigInteger.ONE, BigInteger.ZERO))
                    .withReplicas(replicas)
                    .build(),
                cf.getId())));

    storage.addCluster(Cluster.builder()
        .withName(cf.getClusterName())
        .withPartitioner("Murmur3Partitioner")
        .withSeedHosts(cf.getNodes())
        .withJmxPort(7199)
        .withState(Cluster.State.ACTIVE)
        .build());

    final UUID runId = run.getId();
    final UUID segmentId = storage.getRepairSegmentDao().getNextFreeSegments(run.getId()).get(0).getId();

    AppContext context = new AppContext();
    context.storage = storage;
    context.config = Mockito.mock(ReaperApplicationConfiguration.class);
    when(context.config.getJmxConnectionTimeoutInSeconds()).thenReturn(30);
    when(context.config.getDatacenterAvailability()).thenReturn(DatacenterAvailability.ALL);

    final JmxProxy jmx = JmxProxyTest.mockJmxProxyImpl();
    when(jmx.getClusterName()).thenReturn("reaper");
    when(jmx.isConnectionAlive()).thenReturn(true);

    EndpointSnitchInfoMBean endpointSnitchInfoMBean = mock(EndpointSnitchInfoMBean.class);
    when(endpointSnitchInfoMBean.getDatacenter()).thenReturn("dc1");
    try {
      when(endpointSnitchInfoMBean.getDatacenter(anyString())).thenReturn("dc1");
    } catch (UnknownHostException ex) {
      throw new AssertionError(ex);
    }
    JmxProxyTest.mockGetEndpointSnitchInfoMBean(jmx, endpointSnitchInfoMBean);

    when(jmx.triggerRepair(any(), any(), any(), any(), any(), anyBoolean(), any(), any(), any(), anyInt()))
        .thenThrow(new ReaperException("failure"));

    context.jmxConnectionFactory = new JmxConnectionFactory(context, new NoopCrypotograph()) {
      @Override
      protected JmxProxy connectImpl(Node host) throws ReaperException {
        return jmx;
      }
    };

    RepairRunner rr = mock(RepairRunner.class);
    RepairUnit ru = mock(RepairUnit.class);
    when(ru.getKeyspaceName()).thenReturn("reaper");

    ClusterFacade clusterFacade = mock(ClusterFacade.class);
    when(clusterFacade.connect(any(Cluster.class), any())).thenReturn(jmx);
    when(clusterFacade.nodeIsAccessibleThroughJmx(any(), any())).thenReturn(true);

    when(clusterFacade.tokenRangeToEndpoint(any(), anyString(), any()))
        .thenReturn(Lists.newArrayList(cf.getNodes()));
    when(clusterFacade.listActiveCompactions(any())).thenReturn(CompactionStats.builder().withActiveCompactions(
        Collections.emptyList()).withPendingCompactions(Optional.of(0)).build());

    SegmentRunner sr = SegmentRunner
        .create(context, clusterFacade, segmentId, COORDS, 5000, 0.5, PARALLEL, "reaper", ru, TABLES, rr);

    sr.run();

    assertEquals(RepairSegment.State.NOT_STARTED,
        storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState());
    assertEquals(1, storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getFailCount());
  }

  @Test
  public void nothingToRepairTest() throws InterruptedException, ReaperException, ExecutionException,
      MalformedObjectNameException, ReflectionException, IOException {
    final IStorage storage = new MemoryStorageFacade();
    final int segmentTimeout = 30;

    RepairUnit cf = storage.addRepairUnit(
        RepairUnit.builder()
            .clusterName("reaper")
            .keyspaceName("reaper")
            .columnFamilies(Sets.newHashSet("reaper"))
            .incrementalRepair(false)
            .nodes(Sets.newHashSet("127.0.0.1"))
            .repairThreadCount(1)
            .timeout(segmentTimeout));

    Map<String, String> replicas = Maps.newHashMap();
    replicas.put("127.0.0.1", "dc1");
    RepairRun run = storage.getRepairRunDao().addRepairRun(
        RepairRun.builder("reaper", cf.getId())
            .intensity(0.5)
            .segmentCount(1)
            .repairParallelism(PARALLEL)
            .tables(TABLES),
        Collections.singleton(
            RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(new RingRange(BigInteger.ONE, BigInteger.ZERO))
                    .withReplicas(replicas)
                    .build(),
                cf.getId())));

    storage.addCluster(Cluster.builder()
        .withName(cf.getClusterName())
        .withPartitioner("Murmur3Partitioner")
        .withSeedHosts(cf.getNodes())
        .withJmxPort(7199)
        .withState(Cluster.State.ACTIVE)
        .build());

    final UUID runId = run.getId();
    final UUID segmentId = storage.getRepairSegmentDao().getNextFreeSegments(run.getId()).get(0).getId();

    AppContext context = new AppContext();
    context.storage = storage;
    context.config = Mockito.mock(ReaperApplicationConfiguration.class);
    when(context.config.getJmxConnectionTimeoutInSeconds()).thenReturn(30);
    when(context.config.getDatacenterAvailability()).thenReturn(DatacenterAvailability.ALL);

    final JmxProxy jmx = JmxProxyTest.mockJmxProxyImpl();
    when(jmx.getClusterName()).thenReturn("reaper");
    when(jmx.isConnectionAlive()).thenReturn(true);

    EndpointSnitchInfoMBean endpointSnitchInfoMBean = mock(EndpointSnitchInfoMBean.class);
    when(endpointSnitchInfoMBean.getDatacenter()).thenReturn("dc1");
    try {
      when(endpointSnitchInfoMBean.getDatacenter(anyString())).thenReturn("dc1");
    } catch (UnknownHostException ex) {
      throw new AssertionError(ex);
    }
    JmxProxyTest.mockGetEndpointSnitchInfoMBean(jmx, endpointSnitchInfoMBean);

    when(jmx.triggerRepair(any(), any(), any(), any(), any(), anyBoolean(), any(), any(), any(), anyInt()))
        .thenReturn(0);

    context.jmxConnectionFactory = new JmxConnectionFactory(context, new NoopCrypotograph()) {
      @Override
      protected JmxProxy connectImpl(Node host) throws ReaperException {
        return jmx;
      }
    };

    RepairRunner rr = mock(RepairRunner.class);
    RepairUnit ru = mock(RepairUnit.class);
    when(ru.getKeyspaceName()).thenReturn("reaper");

    ClusterFacade clusterFacade = mock(ClusterFacade.class);
    when(clusterFacade.connect(any(Cluster.class), any())).thenReturn(jmx);
    when(clusterFacade.nodeIsAccessibleThroughJmx(any(), any())).thenReturn(true);

    when(clusterFacade.tokenRangeToEndpoint(any(), anyString(), any()))
        .thenReturn(Lists.newArrayList(cf.getNodes()));
    when(clusterFacade.listActiveCompactions(any())).thenReturn(CompactionStats.builder().withActiveCompactions(
        Collections.emptyList()).withPendingCompactions(Optional.of(0)).build());

    SegmentRunner sr = SegmentRunner
        .create(context, clusterFacade, segmentId, COORDS, 5000, 0.5, PARALLEL, "reaper", ru, TABLES, rr);

    sr.run();

    assertEquals(RepairSegment.State.DONE,
        storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState());
    assertEquals(0, storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getFailCount());
  }

  @Test
  public void failComputingIntensityDelayTest() throws InterruptedException, ReaperException, ExecutionException,
      MalformedObjectNameException, ReflectionException, IOException {
    final IStorage storage = mock(IStorage.class);
    AppContext context = new AppContext();
    context.storage = storage;

    RepairRunner rr = mock(RepairRunner.class);
    when(rr.getRepairRunId()).thenReturn(UUIDs.timeBased());
    RepairUnit ru = mock(RepairUnit.class);
    RepairSegment segment = mock(RepairSegment.class);
    when(segment.getStartTime()).thenReturn(DateTime.now());
    when(segment.getEndTime()).thenReturn(null);
    when(ru.getKeyspaceName()).thenReturn("reaper");

    IRepairSegment mockedRepairSegmentDao = mock(IRepairSegment.class);
    Mockito.when(storage.getRepairSegmentDao()).thenReturn(mockedRepairSegmentDao);
    when(mockedRepairSegmentDao.getRepairSegment(any(), any())).thenReturn(Optional.of(segment));

    ClusterFacade clusterFacade = mock(ClusterFacade.class);

    SegmentRunner sr = SegmentRunner
        .create(context, clusterFacade, UUIDs.timeBased(), COORDS, 5000, 0.5, PARALLEL, "reaper", ru, TABLES, rr);

    assertEquals("Intensity could apply fine although it shouldn't", 0, sr.intensityBasedDelayMillis(new Double(1)));
  }

  @Test
  public void clearSnapshotTest() throws InterruptedException, ReaperException, ExecutionException,
      MalformedObjectNameException, ReflectionException, IOException {
    final IStorage storage = new MemoryStorageFacade();
    final int segmentTimeout = 30;

    RepairUnit cf = storage.addRepairUnit(
        RepairUnit.builder()
            .clusterName("reaper")
            .keyspaceName("reaper")
            .columnFamilies(Sets.newHashSet("reaper"))
            .incrementalRepair(false)
            .nodes(Sets.newHashSet("127.0.0.1"))
            .repairThreadCount(1)
            .timeout(segmentTimeout));

    Map<String, String> replicas = Maps.newHashMap();
    replicas.put("127.0.0.1", "dc1");
    RepairRun run = storage.getRepairRunDao().addRepairRun(
        RepairRun.builder("reaper", cf.getId())
            .intensity(0.5)
            .segmentCount(1)
            .repairParallelism(PARALLEL)
            .tables(TABLES),
        Collections.singleton(
            RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(new RingRange(BigInteger.ONE, BigInteger.ZERO))
                    .withReplicas(replicas)
                    .build(),
                cf.getId())));
    storage.addCluster(Cluster.builder()
        .withName(cf.getClusterName())
        .withPartitioner("Murmur3Partitioner")
        .withSeedHosts(cf.getNodes())
        .withJmxPort(7199)
        .withState(Cluster.State.ACTIVE)
        .build());

    final UUID runId = run.getId();
    final UUID segmentId = storage.getRepairSegmentDao().getNextFreeSegments(run.getId()).get(0).getId();
    AppContext context = new AppContext();
    context.storage = storage;
    context.config = Mockito.mock(ReaperApplicationConfiguration.class);
    when(context.config.getJmxConnectionTimeoutInSeconds()).thenReturn(30);
    when(context.config.getDatacenterAvailability()).thenReturn(DatacenterAvailability.ALL);
    final JmxProxy jmx = JmxProxyTest.mockJmxProxyImpl();
    when(jmx.getClusterName()).thenReturn("reaper");
    when(jmx.isConnectionAlive()).thenReturn(true);
    EndpointSnitchInfoMBean endpointSnitchInfoMBean = mock(EndpointSnitchInfoMBean.class);
    when(endpointSnitchInfoMBean.getDatacenter()).thenReturn("dc1");
    try {
      when(endpointSnitchInfoMBean.getDatacenter(anyString())).thenReturn("dc1");
    } catch (UnknownHostException ex) {
      throw new AssertionError(ex);
    }
    JmxProxyTest.mockGetEndpointSnitchInfoMBean(jmx, endpointSnitchInfoMBean);

    StorageServiceMBean storageServiceMbeanMock = mock(StorageServiceMBean.class);
    JmxProxyTest.mockGetStorageServiceMBean(jmx, storageServiceMbeanMock);
    doThrow(new IOException("failure"))
        .when(storageServiceMbeanMock).clearSnapshot(any(), any());

    RepairRunner rr = mock(RepairRunner.class);
    RepairUnit ru = mock(RepairUnit.class);
    when(ru.getKeyspaceName()).thenReturn("reaper");

    ClusterFacade clusterFacade = mock(ClusterFacade.class);
    when(clusterFacade.connect(any(Cluster.class), any())).thenReturn(jmx);
    when(clusterFacade.nodeIsAccessibleThroughJmx(any(), any())).thenReturn(true);

    when(clusterFacade.tokenRangeToEndpoint(any(), anyString(), any()))
        .thenReturn(Lists.newArrayList(cf.getNodes()));
    when(clusterFacade.listActiveCompactions(any())).thenReturn(CompactionStats.builder().withActiveCompactions(
        Collections.emptyList()).withPendingCompactions(Optional.of(0)).build());

    SegmentRunner sr = SegmentRunner
        .create(context, clusterFacade, segmentId, COORDS, 5000, 0.5, PARALLEL, "reaper", ru, TABLES, rr);

    sr.tryClearSnapshots(UUIDs.timeBased().toString());
    Mockito.verify(storageServiceMbeanMock, Mockito.times(1)).clearSnapshot(any(), any());
  }

  @Test
  public void clearSnapshotFailTest() throws InterruptedException, ReaperException, ExecutionException,
      MalformedObjectNameException, ReflectionException, IOException {
    final IStorage storage = new MemoryStorageFacade();
    final int segmentTimeout = 30;

    RepairUnit cf = storage.addRepairUnit(
        RepairUnit.builder()
            .clusterName("reaper")
            .keyspaceName("reaper")
            .columnFamilies(Sets.newHashSet("reaper"))
            .incrementalRepair(false)
            .nodes(Sets.newHashSet("127.0.0.1"))
            .repairThreadCount(1)
            .timeout(segmentTimeout));

    Map<String, String> replicas = Maps.newHashMap();
    replicas.put("127.0.0.1", "dc1");
    RepairRun run = storage.getRepairRunDao().addRepairRun(
        RepairRun.builder("reaper", cf.getId())
            .intensity(0.5)
            .segmentCount(1)
            .repairParallelism(PARALLEL)
            .tables(TABLES),
        Collections.singleton(
            RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(new RingRange(BigInteger.ONE, BigInteger.ZERO))
                    .withReplicas(replicas)
                    .build(),
                cf.getId())));
    storage.addCluster(Cluster.builder()
        .withName(cf.getClusterName())
        .withPartitioner("Murmur3Partitioner")
        .withSeedHosts(cf.getNodes())
        .withJmxPort(7199)
        .withState(Cluster.State.ACTIVE)
        .build());

    final UUID segmentId = storage.getRepairSegmentDao().getNextFreeSegments(run.getId()).get(0).getId();
    AppContext context = new AppContext();
    context.storage = storage;
    context.config = Mockito.mock(ReaperApplicationConfiguration.class);

    RepairUnit ru = mock(RepairUnit.class);
    when(ru.getKeyspaceName()).thenReturn("reaper");

    ClusterFacade clusterFacade = mock(ClusterFacade.class);
    when(clusterFacade.connect(any(Cluster.class), any())).thenThrow(new ReaperException("failure"));
    StorageServiceMBean storageServiceMbeanMock = mock(StorageServiceMBean.class);
    final JmxProxy jmx = JmxProxyTest.mockJmxProxyImpl();
    JmxProxyTest.mockGetStorageServiceMBean(jmx, storageServiceMbeanMock);
    doThrow(new IOException("failure"))
        .when(storageServiceMbeanMock).clearSnapshot(any(), any());

    RepairRunner rr = mock(RepairRunner.class);
    SegmentRunner sr = SegmentRunner
        .create(context, clusterFacade, segmentId, COORDS, 5000, 0.5, PARALLEL, "reaper", ru, TABLES, rr);

    sr.tryClearSnapshots(UUIDs.timeBased().toString());
    Mockito.verify(storageServiceMbeanMock, Mockito.times(0)).clearSnapshot(any(), any());
  }
}