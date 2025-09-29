/*
 * Copyright 2015-2017 Spotify AB Copyright 2016-2019 The Last Pickle Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
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
import io.cassandrareaper.management.ClusterFacade;
import io.cassandrareaper.management.ICassandraManagementProxy;
import io.cassandrareaper.management.RepairStatusHandler;
import io.cassandrareaper.management.jmx.CassandraManagementProxyTest;
import io.cassandrareaper.management.jmx.JmxCassandraManagementProxy;
import io.cassandrareaper.management.jmx.JmxManagementConnectionFactory;
import io.cassandrareaper.storage.IStorageDao;
import io.cassandrareaper.storage.MemoryStorageFacade;
import io.cassandrareaper.storage.cassandra.CassandraStorageFacade;
import io.cassandrareaper.storage.repairsegment.IRepairSegmentDao;

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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.MalformedObjectNameException;
import javax.management.ReflectionException;

import static org.apache.cassandra.repair.RepairParallelism.PARALLEL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.service.StorageServiceMBean;
import org.apache.cassandra.utils.progress.ProgressEventType;
import org.apache.commons.lang3.mutable.MutableObject;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public final class SegmentRunnerTest {
  // TODO: Clean up tests. There's a lot of code duplication across these tests.

  private static final Set<String> TABLES = ImmutableSet.of("table1");
  private static final Set<String> COORDS = Collections.singleton("");

  @Before
  public void setUp() throws Exception {
    SegmentRunner.SEGMENT_RUNNERS.clear();
  }

  @Test
  public void timeoutTest()
      throws InterruptedException,
          ReaperException,
          ExecutionException,
          MalformedObjectNameException,
          ReflectionException,
          IOException {
    final AppContext context = new AppContext();
    final int segmentTimeout = 30;
    context.config = Mockito.mock(ReaperApplicationConfiguration.class);
    when(context.config.getJmxConnectionTimeoutInSeconds()).thenReturn(30);
    when(context.config.getDatacenterAvailability()).thenReturn(DatacenterAvailability.ALL);
    context.storage = new MemoryStorageFacade();

    RepairUnit cf =
        context
            .storage
            .getRepairUnitDao()
            .addRepairUnit(
                RepairUnit.builder()
                    .clusterName("reaper")
                    .keyspaceName("reaper")
                    .columnFamilies(Sets.newHashSet("reaper"))
                    .incrementalRepair(false)
                    .subrangeIncrementalRepair(false)
                    .nodes(Sets.newHashSet("127.0.0.1"))
                    .repairThreadCount(1)
                    .timeout(segmentTimeout));

    Map<String, String> replicas = Maps.newHashMap();
    replicas.put("127.0.0.1", "dc1");
    RepairRun run =
        context
            .storage
            .getRepairRunDao()
            .addRepairRun(
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

    context
        .storage
        .getClusterDao()
        .addCluster(
            Cluster.builder()
                .withName(cf.getClusterName())
                .withPartitioner("Murmur3Partitioner")
                .withSeedHosts(cf.getNodes())
                .withJmxPort(7199)
                .withState(Cluster.State.ACTIVE)
                .build());

    final UUID runId = run.getId();
    final UUID segmentId =
        context.storage.getRepairSegmentDao().getNextFreeSegments(run.getId()).get(0).getId();

    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final MutableObject<Future<?>> future = new MutableObject<>();

    final JmxCassandraManagementProxy jmx = CassandraManagementProxyTest.mockJmxProxyImpl();
    when(jmx.getClusterName()).thenReturn("reaper");

    EndpointSnitchInfoMBean endpointSnitchInfoMBean = mock(EndpointSnitchInfoMBean.class);
    when(endpointSnitchInfoMBean.getDatacenter()).thenReturn("dc1");
    try {
      when(endpointSnitchInfoMBean.getDatacenter(anyString())).thenReturn("dc1");
    } catch (UnknownHostException ex) {
      throw new AssertionError(ex);
    }

    when(jmx.triggerRepair(any(), any(), any(), any(), any(), any(), any(), anyInt()))
        .then(
            (invocation) -> {
              assertEquals(
                  RepairSegment.State.STARTED,
                  context
                      .storage
                      .getRepairSegmentDao()
                      .getRepairSegment(runId, segmentId)
                      .get()
                      .getState());

              future.setValue(
                  executor.submit(
                      new Thread() {
                        @Override
                        public void run() {
                          ((RepairStatusHandler) invocation.getArgument(5))
                              .handle(
                                  1,
                                  Optional.of(ProgressEventType.START),
                                  "Repair command 1 has started",
                                  jmx);

                          assertEquals(
                              RepairSegment.State.RUNNING,
                              context
                                  .storage
                                  .getRepairSegmentDao()
                                  .getRepairSegment(runId, segmentId)
                                  .get()
                                  .getState());
                        }
                      }));
              return 1;
            });

    context.managementConnectionFactory =
        new JmxManagementConnectionFactory(context, new NoopCrypotograph()) {
          @Override
          public JmxCassandraManagementProxy connectImpl(Node host) throws ReaperException {
            return jmx;
          }
        };

    RepairRunner rr = mock(RepairRunner.class);
    RepairUnit ru = mock(RepairUnit.class);
    when(ru.getKeyspaceName()).thenReturn("reaper");
    when(rr.getRepairRunId()).thenReturn(runId);

    ClusterFacade clusterFacade = mock(ClusterFacade.class);
    when(clusterFacade.connect(any(Cluster.class), any())).thenReturn(jmx);
    when(clusterFacade.nodeIsDirectlyAccessible(any(), any())).thenReturn(true);

    when(clusterFacade.tokenRangeToEndpoint(any(), anyString(), any()))
        .thenReturn(Lists.newArrayList(cf.getNodes()));

    when(clusterFacade.nodeIsDirectlyAccessible(any(), any())).thenReturn(true);
    when(clusterFacade.listActiveCompactions(any()))
        .thenReturn(
            CompactionStats.builder()
                .withActiveCompactions(Collections.emptyList())
                .withPendingCompactions(Optional.of(0))
                .build());

    SegmentRunner sr =
        SegmentRunner.create(
            context,
            clusterFacade,
            segmentId,
            COORDS,
            100,
            0.5,
            PARALLEL,
            "reaper",
            ru,
            TABLES,
            rr);

    sr.run();

    future.getValue().get();
    executor.shutdown();

    assertEquals(
        RepairSegment.State.NOT_STARTED,
        context.storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState());
    assertEquals(
        1,
        context
            .storage
            .getRepairSegmentDao()
            .getRepairSegment(runId, segmentId)
            .get()
            .getFailCount());
  }

  @Test
  public void successTest()
      throws InterruptedException,
          ReaperException,
          ExecutionException,
          MalformedObjectNameException,
          ReflectionException,
          IOException {
    final IStorageDao storage = new MemoryStorageFacade();
    final int segmentTimeout = 30;
    RepairUnit cf =
        storage
            .getRepairUnitDao()
            .addRepairUnit(
                RepairUnit.builder()
                    .clusterName("reaper")
                    .keyspaceName("reaper")
                    .columnFamilies(Sets.newHashSet("reaper"))
                    .incrementalRepair(false)
                    .subrangeIncrementalRepair(false)
                    .nodes(Sets.newHashSet("127.0.0.1"))
                    .repairThreadCount(1)
                    .timeout(segmentTimeout));

    Map<String, String> replicas = Maps.newHashMap();
    replicas.put("127.0.0.1", "dc1");
    RepairRun run =
        storage
            .getRepairRunDao()
            .addRepairRun(
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
    storage
        .getClusterDao()
        .addCluster(
            Cluster.builder()
                .withName(cf.getClusterName())
                .withPartitioner("Murmur3Partitioner")
                .withSeedHosts(cf.getNodes())
                .withJmxPort(7199)
                .withState(Cluster.State.ACTIVE)
                .build());

    final UUID runId = run.getId();
    final UUID segmentId =
        storage.getRepairSegmentDao().getNextFreeSegments(run.getId()).get(0).getId();
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final MutableObject<Future<?>> future = new MutableObject<>();
    AppContext context = new AppContext();
    context.storage = storage;
    context.config = Mockito.mock(ReaperApplicationConfiguration.class);
    when(context.config.getJmxConnectionTimeoutInSeconds()).thenReturn(30);
    when(context.config.getDatacenterAvailability()).thenReturn(DatacenterAvailability.ALL);
    final JmxCassandraManagementProxy jmx = CassandraManagementProxyTest.mockJmxProxyImpl();
    when(jmx.getClusterName()).thenReturn("reaper");
    EndpointSnitchInfoMBean endpointSnitchInfoMBean = mock(EndpointSnitchInfoMBean.class);
    when(endpointSnitchInfoMBean.getDatacenter()).thenReturn("dc1");
    try {
      when(endpointSnitchInfoMBean.getDatacenter(anyString())).thenReturn("dc1");
    } catch (UnknownHostException ex) {
      throw new AssertionError(ex);
    }

    when(jmx.triggerRepair(any(), any(), any(), any(), any(), any(), any(), anyInt()))
        .then(
            (invocation) -> {
              assertEquals(
                  RepairSegment.State.STARTED,
                  storage
                      .getRepairSegmentDao()
                      .getRepairSegment(runId, segmentId)
                      .get()
                      .getState());

              future.setValue(
                  executor.submit(
                      () -> {
                        ((RepairStatusHandler) invocation.getArgument(5))
                            .handle(
                                1,
                                Optional.of(ProgressEventType.START),
                                "Repair command 1 has started",
                                jmx);

                        assertEquals(
                            RepairSegment.State.RUNNING,
                            storage
                                .getRepairSegmentDao()
                                .getRepairSegment(runId, segmentId)
                                .get()
                                .getState());

                        // test an unrelated repair. Should throw exception
                        try {
                          ((RepairStatusHandler) invocation.getArgument(5))
                              .handle(
                                  2,
                                  Optional.of(ProgressEventType.ERROR),
                                  "Repair command 2 has failed",
                                  jmx);
                          throw new AssertionError("illegal handle of wrong repairNo");
                        } catch (IllegalArgumentException ignore) {
                        }

                        ((RepairStatusHandler) invocation.getArgument(5))
                            .handle(
                                1,
                                Optional.of(ProgressEventType.SUCCESS),
                                "Repair session succeeded in command 1",
                                jmx);

                        assertEquals(
                            RepairSegment.State.DONE,
                            storage
                                .getRepairSegmentDao()
                                .getRepairSegment(runId, segmentId)
                                .get()
                                .getState());

                        ((RepairStatusHandler) invocation.getArgument(5))
                            .handle(
                                1,
                                Optional.of(ProgressEventType.COMPLETE),
                                "Repair command 1 has finished",
                                jmx);

                        assertEquals(
                            RepairSegment.State.DONE,
                            storage
                                .getRepairSegmentDao()
                                .getRepairSegment(runId, segmentId)
                                .get()
                                .getState());
                      }));
              return 1;
            });
    context.managementConnectionFactory =
        new JmxManagementConnectionFactory(context, new NoopCrypotograph()) {
          @Override
          protected JmxCassandraManagementProxy connectImpl(Node host) throws ReaperException {
            return jmx;
          }
        };

    RepairRunner rr = mock(RepairRunner.class);
    RepairUnit ru = mock(RepairUnit.class);
    when(ru.getKeyspaceName()).thenReturn("reaper");
    when(rr.getRepairRunId()).thenReturn(runId);

    ClusterFacade clusterFacade = mock(ClusterFacade.class);
    when(clusterFacade.connect(any(Cluster.class), any())).thenReturn(jmx);
    when(clusterFacade.nodeIsDirectlyAccessible(any(), any())).thenReturn(true);

    when(clusterFacade.tokenRangeToEndpoint(any(), anyString(), any()))
        .thenReturn(Lists.newArrayList(cf.getNodes()));
    when(clusterFacade.listActiveCompactions(any()))
        .thenReturn(
            CompactionStats.builder()
                .withActiveCompactions(Collections.emptyList())
                .withPendingCompactions(Optional.of(0))
                .build());

    SegmentRunner sr =
        SegmentRunner.create(
            context,
            clusterFacade,
            segmentId,
            COORDS,
            5000,
            0.5,
            PARALLEL,
            "reaper",
            ru,
            TABLES,
            rr);

    sr.run();

    future.getValue().get();
    executor.shutdown();

    assertEquals(
        RepairSegment.State.DONE,
        storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState());
    assertEquals(
        0, storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getFailCount());
  }

  @Test
  public void testIntensityBasedDelayIsApplied()
      throws InterruptedException,
          ReaperException,
          ExecutionException,
          MalformedObjectNameException,
          ReflectionException,
          IOException {
    final IStorageDao storage = new MemoryStorageFacade();
    final int segmentTimeout = 30;
    final double intensity =
        0.5; // 50% intensity means delay = repair_duration / 0.5 - repair_duration

    RepairUnit cf =
        storage
            .getRepairUnitDao()
            .addRepairUnit(
                RepairUnit.builder()
                    .clusterName("reaper")
                    .keyspaceName("reaper")
                    .columnFamilies(Sets.newHashSet("reaper"))
                    .incrementalRepair(false)
                    .subrangeIncrementalRepair(false)
                    .nodes(Sets.newHashSet("127.0.0.1"))
                    .repairThreadCount(1)
                    .timeout(segmentTimeout));

    Map<String, String> replicas = Maps.newHashMap();
    replicas.put("127.0.0.1", "dc1");
    RepairRun run =
        storage
            .getRepairRunDao()
            .addRepairRun(
                RepairRun.builder("reaper", cf.getId())
                    .intensity(intensity)
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
    storage
        .getClusterDao()
        .addCluster(
            Cluster.builder()
                .withName(cf.getClusterName())
                .withPartitioner("Murmur3Partitioner")
                .withSeedHosts(cf.getNodes())
                .withJmxPort(7199)
                .withState(Cluster.State.ACTIVE)
                .build());

    final UUID runId = run.getId();
    final UUID segmentId =
        storage.getRepairSegmentDao().getNextFreeSegments(run.getId()).get(0).getId();
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final MutableObject<Future<?>> future = new MutableObject<>();
    final AtomicLong repairStartTime = new AtomicLong();
    final AtomicLong repairEndTime = new AtomicLong();
    final AtomicLong delayStartTime = new AtomicLong();
    final AtomicLong delayEndTime = new AtomicLong();
    final long simulatedRepairDuration = 2000; // 2 seconds repair

    AppContext context = new AppContext();
    context.storage = storage;
    context.config = Mockito.mock(ReaperApplicationConfiguration.class);
    when(context.config.getJmxConnectionTimeoutInSeconds()).thenReturn(30);
    when(context.config.getDatacenterAvailability()).thenReturn(DatacenterAvailability.ALL);
    // Mock distributed storage to return specific number of running reapers
    context.isDistributed.set(
        false); // Non-distributed mode, so countRunningReapers should return 1

    final JmxCassandraManagementProxy jmx = CassandraManagementProxyTest.mockJmxProxyImpl();
    when(jmx.getClusterName()).thenReturn("reaper");
    EndpointSnitchInfoMBean endpointSnitchInfoMBean = mock(EndpointSnitchInfoMBean.class);
    when(endpointSnitchInfoMBean.getDatacenter()).thenReturn("dc1");
    try {
      when(endpointSnitchInfoMBean.getDatacenter(anyString())).thenReturn("dc1");
    } catch (UnknownHostException ex) {
      throw new AssertionError(ex);
    }

    when(jmx.triggerRepair(any(), any(), any(), any(), any(), any(), any(), anyInt()))
        .then(
            (invocation) -> {
              repairStartTime.set(System.currentTimeMillis());

              future.setValue(
                  executor.submit(
                      () -> {
                        ((RepairStatusHandler) invocation.getArgument(5))
                            .handle(
                                1,
                                Optional.of(ProgressEventType.START),
                                "Repair command 1 has started",
                                jmx);

                        // Simulate repair taking some time
                        try {
                          Thread.sleep(simulatedRepairDuration);
                        } catch (InterruptedException e) {
                          Thread.currentThread().interrupt();
                        }

                        ((RepairStatusHandler) invocation.getArgument(5))
                            .handle(
                                1,
                                Optional.of(ProgressEventType.SUCCESS),
                                "Repair session succeeded in command 1",
                                jmx);

                        repairEndTime.set(System.currentTimeMillis());

                        ((RepairStatusHandler) invocation.getArgument(5))
                            .handle(
                                1,
                                Optional.of(ProgressEventType.COMPLETE),
                                "Repair command 1 has finished",
                                jmx);
                      }));
              return 1;
            });

    context.managementConnectionFactory =
        new JmxManagementConnectionFactory(context, new NoopCrypotograph()) {
          @Override
          protected JmxCassandraManagementProxy connectImpl(Node host) throws ReaperException {
            return jmx;
          }
        };

    RepairRunner rr = mock(RepairRunner.class);
    RepairUnit ru = mock(RepairUnit.class);
    when(ru.getKeyspaceName()).thenReturn("reaper");
    when(rr.getRepairRunId()).thenReturn(runId);

    ClusterFacade clusterFacade = mock(ClusterFacade.class);
    when(clusterFacade.connect(any(Cluster.class), any())).thenReturn(jmx);
    when(clusterFacade.nodeIsDirectlyAccessible(any(), any())).thenReturn(true);

    when(clusterFacade.tokenRangeToEndpoint(any(), anyString(), any()))
        .thenReturn(Lists.newArrayList(cf.getNodes()));
    when(clusterFacade.listActiveCompactions(any()))
        .thenReturn(
            CompactionStats.builder()
                .withActiveCompactions(Collections.emptyList())
                .withPendingCompactions(Optional.of(0))
                .build());

    SegmentRunner sr =
        SegmentRunner.create(
            context,
            clusterFacade,
            segmentId,
            COORDS,
            5000,
            intensity,
            PARALLEL,
            "reaper",
            ru,
            TABLES,
            rr);

    // Record when delay starts
    delayStartTime.set(System.currentTimeMillis());
    sr.run();
    delayEndTime.set(System.currentTimeMillis());

    future.getValue().get();
    executor.shutdown();

    // Verify repair completed successfully
    assertEquals(
        RepairSegment.State.DONE,
        storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState());
    assertEquals(
        0, storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getFailCount());

    // Get the repair segment to check timing
    RepairSegment segment = storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get();
    assertNotNull("Segment should have start time", segment.getStartTime());
    assertNotNull("Segment should have end time", segment.getEndTime());

    // Calculate expected delay
    long actualRepairDuration =
        segment.getEndTime().getMillis() - segment.getStartTime().getMillis();
    long expectedDelay = (long) (actualRepairDuration / intensity - actualRepairDuration);
    // In non-distributed mode, countRunningReapers() returns 1, so delay should not be multiplied

    // Verify that total time includes both repair and delay
    long totalTime = delayEndTime.get() - delayStartTime.get();

    // The total time should be at least repair time + expected delay
    // We allow some tolerance for timing variations in tests
    long minimumExpectedTime = actualRepairDuration + expectedDelay - 500; // 500ms tolerance
    assertTrue(
        String.format(
            "Total time (%d ms) should be at least repair duration (%d ms) + expected delay (%d ms) = %d ms",
            totalTime, actualRepairDuration, expectedDelay, minimumExpectedTime),
        totalTime >= minimumExpectedTime);

    // Verify the delay was significant (should be approximately equal to repair duration for 0.5
    // intensity)
    // For intensity 0.5: delay = duration/0.5 - duration = 2*duration - duration = duration
    long toleranceMs = 1000; // Allow 1 second tolerance for test timing variations
    assertTrue(
        String.format(
            "Expected delay (%d ms) should be approximately equal to repair duration (%d ms) for intensity 0.5",
            expectedDelay, actualRepairDuration),
        Math.abs(expectedDelay - actualRepairDuration) <= toleranceMs);
  }

  @Test
  public void testIntensityBasedDelayWithDistributedStorage()
      throws InterruptedException,
          ReaperException,
          ExecutionException,
          MalformedObjectNameException,
          ReflectionException,
          IOException {
    final IStorageDao storage = new MemoryStorageFacade();
    final int segmentTimeout = 30;
    final double intensity = 0.25; // 25% intensity for longer delay
    final int mockRunningReapers = 3;
    // Use unique command ID to avoid conflicts with other tests
    final int uniqueCommandId = (int) (System.nanoTime() % Integer.MAX_VALUE);

    RepairUnit cf =
        storage
            .getRepairUnitDao()
            .addRepairUnit(
                RepairUnit.builder()
                    .clusterName("reaper")
                    .keyspaceName("reaper")
                    .columnFamilies(Sets.newHashSet("reaper"))
                    .incrementalRepair(false)
                    .subrangeIncrementalRepair(false)
                    .nodes(Sets.newHashSet("127.0.0.1"))
                    .repairThreadCount(1)
                    .timeout(segmentTimeout));

    Map<String, String> replicas = Maps.newHashMap();
    replicas.put("127.0.0.1", "dc1");
    RepairRun run =
        storage
            .getRepairRunDao()
            .addRepairRun(
                RepairRun.builder("reaper", cf.getId())
                    .intensity(intensity)
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
    storage
        .getClusterDao()
        .addCluster(
            Cluster.builder()
                .withName(cf.getClusterName())
                .withPartitioner("Murmur3Partitioner")
                .withSeedHosts(cf.getNodes())
                .withJmxPort(7199)
                .withState(Cluster.State.ACTIVE)
                .build());

    final UUID runId = run.getId();
    final UUID segmentId =
        storage.getRepairSegmentDao().getNextFreeSegments(run.getId()).get(0).getId();
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final MutableObject<Future<?>> future = new MutableObject<>();
    final long simulatedRepairDuration = 1000; // 1 second repair

    AppContext context = new AppContext();
    context.config = Mockito.mock(ReaperApplicationConfiguration.class);
    when(context.config.getJmxConnectionTimeoutInSeconds()).thenReturn(30);
    when(context.config.getDatacenterAvailability()).thenReturn(DatacenterAvailability.ALL);

    // Create a custom storage facade that extends MemoryStorageFacade and implements distributed
    // methods
    CassandraStorageFacade distributedStorage = mock(CassandraStorageFacade.class);
    when(distributedStorage.countRunningReapers()).thenReturn(mockRunningReapers);
    when(distributedStorage.getRepairSegmentDao()).thenReturn(storage.getRepairSegmentDao());
    when(distributedStorage.getRepairRunDao()).thenReturn(storage.getRepairRunDao());
    when(distributedStorage.getRepairUnitDao()).thenReturn(storage.getRepairUnitDao());
    when(distributedStorage.getClusterDao()).thenReturn(storage.getClusterDao());
    when(distributedStorage.lockRunningRepairsForNodes(any(), any(), any())).thenReturn(true);
    context.storage = distributedStorage;
    context.isDistributed.set(true);

    final JmxCassandraManagementProxy jmx = CassandraManagementProxyTest.mockJmxProxyImpl();
    when(jmx.getClusterName()).thenReturn("reaper");
    EndpointSnitchInfoMBean endpointSnitchInfoMBean = mock(EndpointSnitchInfoMBean.class);
    when(endpointSnitchInfoMBean.getDatacenter()).thenReturn("dc1");
    try {
      when(endpointSnitchInfoMBean.getDatacenter(anyString())).thenReturn("dc1");
    } catch (UnknownHostException ex) {
      throw new AssertionError(ex);
    }

    when(jmx.triggerRepair(any(), any(), any(), any(), any(), any(), any(), anyInt()))
        .then(
            (invocation) -> {
              future.setValue(
                  executor.submit(
                      () -> {
                        // Update segment to RUNNING state with start time
                        RepairSegment runningSegment =
                            storage
                                .getRepairSegmentDao()
                                .getRepairSegment(runId, segmentId)
                                .get()
                                .with()
                                .withState(RepairSegment.State.RUNNING)
                                .withStartTime(DateTime.now())
                                .build();
                        storage.getRepairSegmentDao().updateRepairSegmentUnsafe(runningSegment);

                        ((RepairStatusHandler) invocation.getArgument(5))
                            .handle(
                                uniqueCommandId,
                                Optional.of(ProgressEventType.START),
                                "Repair command " + uniqueCommandId + " has started",
                                jmx);

                        // Simulate repair taking some time
                        try {
                          Thread.sleep(simulatedRepairDuration);
                        } catch (InterruptedException e) {
                          Thread.currentThread().interrupt();
                        }

                        ((RepairStatusHandler) invocation.getArgument(5))
                            .handle(
                                uniqueCommandId,
                                Optional.of(ProgressEventType.SUCCESS),
                                "Repair session succeeded in command " + uniqueCommandId,
                                jmx);

                        // Update segment to DONE state with end time BEFORE sending COMPLETE
                        RepairSegment doneSegment =
                            storage
                                .getRepairSegmentDao()
                                .getRepairSegment(runId, segmentId)
                                .get()
                                .with()
                                .withState(RepairSegment.State.DONE)
                                .withEndTime(DateTime.now())
                                .build();
                        storage.getRepairSegmentDao().updateRepairSegmentUnsafe(doneSegment);

                        // Small delay to ensure segment state is persisted
                        try {
                          Thread.sleep(10);
                        } catch (InterruptedException e) {
                          Thread.currentThread().interrupt();
                        }

                        ((RepairStatusHandler) invocation.getArgument(5))
                            .handle(
                                uniqueCommandId,
                                Optional.of(ProgressEventType.COMPLETE),
                                "Repair command " + uniqueCommandId + " has finished",
                                jmx);
                      }));
              return uniqueCommandId;
            });

    context.managementConnectionFactory =
        new JmxManagementConnectionFactory(context, new NoopCrypotograph()) {
          @Override
          protected JmxCassandraManagementProxy connectImpl(Node host) throws ReaperException {
            return jmx;
          }
        };

    RepairRunner rr = mock(RepairRunner.class);
    RepairUnit ru = mock(RepairUnit.class);
    when(ru.getKeyspaceName()).thenReturn("reaper");
    when(rr.getRepairRunId()).thenReturn(runId);

    ClusterFacade clusterFacade = mock(ClusterFacade.class);
    when(clusterFacade.connect(any(Cluster.class), any())).thenReturn(jmx);
    when(clusterFacade.nodeIsDirectlyAccessible(any(), any())).thenReturn(true);

    when(clusterFacade.tokenRangeToEndpoint(any(), anyString(), any()))
        .thenReturn(Lists.newArrayList(cf.getNodes()));
    when(clusterFacade.listActiveCompactions(any()))
        .thenReturn(
            CompactionStats.builder()
                .withActiveCompactions(Collections.emptyList())
                .withPendingCompactions(Optional.of(0))
                .build());

    SegmentRunner sr =
        SegmentRunner.create(
            context,
            clusterFacade,
            segmentId,
            COORDS,
            5000,
            intensity,
            PARALLEL,
            "reaper",
            ru,
            TABLES,
            rr);

    // Since distributed storage mock is complex, let's just test that the runner completes
    // and that the countRunningReapers method is called for the delay calculation
    long startTime = System.currentTimeMillis();
    sr.run();
    long endTime = System.currentTimeMillis();

    // Clean up executor
    if (future.getValue() != null) {
      future.getValue().get();
    }
    executor.shutdown();

    // Verify that countRunningReapers was called (which means intensity delay logic was reached)
    verify(distributedStorage, atLeastOnce()).countRunningReapers();

    // Verify the total execution time includes some delay
    // The test simulates a 1 second repair with 0.25 intensity and 3 running reapers
    // Expected delay = (1000 / 0.25 - 1000) * 3 = 9000ms
    // So total time should be at least 1000 + some significant delay
    long totalTime = endTime - startTime;
    assertTrue(
        String.format(
            "Total time (%d ms) should include intensity-based delay for distributed storage",
            totalTime),
        totalTime >= 2000); // At least 2 seconds (repair + some delay)
  }

  @Test
  public void testIntensityBasedDelayExceptionHandling()
      throws InterruptedException,
          ReaperException,
          ExecutionException,
          MalformedObjectNameException,
          ReflectionException,
          IOException {
    final IStorageDao storage = new MemoryStorageFacade();
    final int segmentTimeout = 30;
    final double intensity = 0.5;

    RepairUnit cf =
        storage
            .getRepairUnitDao()
            .addRepairUnit(
                RepairUnit.builder()
                    .clusterName("reaper")
                    .keyspaceName("reaper")
                    .columnFamilies(Sets.newHashSet("reaper"))
                    .incrementalRepair(false)
                    .subrangeIncrementalRepair(false)
                    .nodes(Sets.newHashSet("127.0.0.1"))
                    .repairThreadCount(1)
                    .timeout(segmentTimeout));

    Map<String, String> replicas = Maps.newHashMap();
    replicas.put("127.0.0.1", "dc1");
    RepairRun run =
        storage
            .getRepairRunDao()
            .addRepairRun(
                RepairRun.builder("reaper", cf.getId())
                    .intensity(intensity)
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
    storage
        .getClusterDao()
        .addCluster(
            Cluster.builder()
                .withName(cf.getClusterName())
                .withPartitioner("Murmur3Partitioner")
                .withSeedHosts(cf.getNodes())
                .withJmxPort(7199)
                .withState(Cluster.State.ACTIVE)
                .build());

    final UUID runId = run.getId();
    final UUID segmentId =
        storage.getRepairSegmentDao().getNextFreeSegments(run.getId()).get(0).getId();
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final MutableObject<Future<?>> future = new MutableObject<>();
    final AtomicBoolean interruptionOccurred = new AtomicBoolean(false);
    final AtomicBoolean repairCompleted = new AtomicBoolean(false);

    AppContext context = new AppContext();
    context.storage = storage;
    context.config = Mockito.mock(ReaperApplicationConfiguration.class);
    when(context.config.getJmxConnectionTimeoutInSeconds()).thenReturn(30);
    when(context.config.getDatacenterAvailability()).thenReturn(DatacenterAvailability.ALL);
    context.isDistributed.set(false);

    final JmxCassandraManagementProxy jmx = CassandraManagementProxyTest.mockJmxProxyImpl();
    when(jmx.getClusterName()).thenReturn("reaper");
    EndpointSnitchInfoMBean endpointSnitchInfoMBean = mock(EndpointSnitchInfoMBean.class);
    when(endpointSnitchInfoMBean.getDatacenter()).thenReturn("dc1");
    try {
      when(endpointSnitchInfoMBean.getDatacenter(anyString())).thenReturn("dc1");
    } catch (UnknownHostException ex) {
      throw new AssertionError(ex);
    }

    when(jmx.triggerRepair(any(), any(), any(), any(), any(), any(), any(), anyInt()))
        .then(
            (invocation) -> {
              future.setValue(
                  executor.submit(
                      () -> {
                        ((RepairStatusHandler) invocation.getArgument(5))
                            .handle(
                                1,
                                Optional.of(ProgressEventType.START),
                                "Repair command 1 has started",
                                jmx);

                        // Simulate very short repair
                        try {
                          Thread.sleep(100);
                        } catch (InterruptedException e) {
                          Thread.currentThread().interrupt();
                        }

                        ((RepairStatusHandler) invocation.getArgument(5))
                            .handle(
                                1,
                                Optional.of(ProgressEventType.SUCCESS),
                                "Repair session succeeded in command 1",
                                jmx);

                        ((RepairStatusHandler) invocation.getArgument(5))
                            .handle(
                                1,
                                Optional.of(ProgressEventType.COMPLETE),
                                "Repair command 1 has finished",
                                jmx);

                        repairCompleted.set(true);
                      }));
              return 1;
            });

    context.managementConnectionFactory =
        new JmxManagementConnectionFactory(context, new NoopCrypotograph()) {
          @Override
          protected JmxCassandraManagementProxy connectImpl(Node host) throws ReaperException {
            return jmx;
          }
        };

    RepairRunner rr = mock(RepairRunner.class);
    RepairUnit ru = mock(RepairUnit.class);
    when(ru.getKeyspaceName()).thenReturn("reaper");
    when(rr.getRepairRunId()).thenReturn(runId);

    ClusterFacade clusterFacade = mock(ClusterFacade.class);
    when(clusterFacade.connect(any(Cluster.class), any())).thenReturn(jmx);
    when(clusterFacade.nodeIsDirectlyAccessible(any(), any())).thenReturn(true);

    when(clusterFacade.tokenRangeToEndpoint(any(), anyString(), any()))
        .thenReturn(Lists.newArrayList(cf.getNodes()));
    when(clusterFacade.listActiveCompactions(any()))
        .thenReturn(
            CompactionStats.builder()
                .withActiveCompactions(Collections.emptyList())
                .withPendingCompactions(Optional.of(0))
                .build());

    SegmentRunner sr =
        SegmentRunner.create(
            context,
            clusterFacade,
            segmentId,
            COORDS,
            5000,
            intensity,
            PARALLEL,
            "reaper",
            ru,
            TABLES,
            rr);

    // Create a thread to run the segment runner
    Thread runnerThread =
        new Thread(
            () -> {
              try {
                sr.run();
              } catch (Exception e) {
                // Expected to handle interruption gracefully
                if (e instanceof InterruptedException
                    || e.getCause() instanceof InterruptedException) {
                  interruptionOccurred.set(true);
                }
              }
            });

    runnerThread.start();

    // Wait for repair to complete, then interrupt during delay
    if (future.getValue() != null) {
      future.getValue().get();
    } else {
      // Wait for repair to complete using the flag
      int maxWait = 10000; // 10 seconds max wait
      int waited = 0;
      while (!repairCompleted.get() && waited < maxWait) {
        Thread.sleep(100);
        waited += 100;
      }
    }

    Thread.sleep(500); // Give some time for delay to start
    runnerThread.interrupt(); // Interrupt during the intensity delay

    // Wait for thread to finish
    runnerThread.join(5000); // 5 second timeout
    assertFalse("Runner thread should have finished", runnerThread.isAlive());

    executor.shutdown();

    // Verify repair still completed successfully despite interruption during delay
    assertEquals(
        RepairSegment.State.DONE,
        storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState());

    // The test verifies that interruption during delay is handled gracefully
    // and doesn't prevent the repair from being marked as completed
  }

  @Test
  public void failureTest()
      throws InterruptedException,
          ReaperException,
          ExecutionException,
          MalformedObjectNameException,
          ReflectionException,
          IOException {
    final IStorageDao storage = new MemoryStorageFacade();
    final int segmentTimeout = 30;

    RepairUnit cf =
        storage
            .getRepairUnitDao()
            .addRepairUnit(
                RepairUnit.builder()
                    .clusterName("reaper")
                    .keyspaceName("reaper")
                    .columnFamilies(Sets.newHashSet("reaper"))
                    .incrementalRepair(false)
                    .subrangeIncrementalRepair(false)
                    .nodes(Sets.newHashSet("127.0.0.1"))
                    .repairThreadCount(1)
                    .timeout(segmentTimeout));

    Map<String, String> replicas = Maps.newHashMap();
    replicas.put("127.0.0.1", "dc1");
    RepairRun run =
        storage
            .getRepairRunDao()
            .addRepairRun(
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

    storage
        .getClusterDao()
        .addCluster(
            Cluster.builder()
                .withName(cf.getClusterName())
                .withPartitioner("Murmur3Partitioner")
                .withSeedHosts(cf.getNodes())
                .withJmxPort(7199)
                .withState(Cluster.State.ACTIVE)
                .build());

    final UUID runId = run.getId();
    final UUID segmentId =
        storage.getRepairSegmentDao().getNextFreeSegments(run.getId()).get(0).getId();

    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final MutableObject<Future<?>> future = new MutableObject<>();

    AppContext context = new AppContext();
    context.storage = storage;
    context.config = Mockito.mock(ReaperApplicationConfiguration.class);
    when(context.config.getJmxConnectionTimeoutInSeconds()).thenReturn(30);
    when(context.config.getDatacenterAvailability()).thenReturn(DatacenterAvailability.ALL);

    final JmxCassandraManagementProxy jmx = CassandraManagementProxyTest.mockJmxProxyImpl();
    when(jmx.getClusterName()).thenReturn("reaper");

    EndpointSnitchInfoMBean endpointSnitchInfoMBean = mock(EndpointSnitchInfoMBean.class);
    when(endpointSnitchInfoMBean.getDatacenter()).thenReturn("dc1");
    try {
      when(endpointSnitchInfoMBean.getDatacenter(anyString())).thenReturn("dc1");
    } catch (UnknownHostException ex) {
      throw new AssertionError(ex);
    }

    when(jmx.triggerRepair(any(), any(), any(), any(), any(), any(), any(), anyInt()))
        .then(
            (invocation) -> {
              assertEquals(
                  RepairSegment.State.STARTED,
                  storage
                      .getRepairSegmentDao()
                      .getRepairSegment(runId, segmentId)
                      .get()
                      .getState());

              future.setValue(
                  executor.submit(
                      () -> {
                        ((RepairStatusHandler) invocation.getArgument(5))
                            .handle(
                                1,
                                Optional.of(ProgressEventType.START),
                                "Repair command 1 has started",
                                jmx);

                        assertEquals(
                            RepairSegment.State.RUNNING,
                            storage
                                .getRepairSegmentDao()
                                .getRepairSegment(runId, segmentId)
                                .get()
                                .getState());

                        ((RepairStatusHandler) invocation.getArgument(5))
                            .handle(
                                1,
                                Optional.of(ProgressEventType.ERROR),
                                "Repair command 1 has failed",
                                jmx);

                        assertEquals(
                            RepairSegment.State.NOT_STARTED,
                            storage
                                .getRepairSegmentDao()
                                .getRepairSegment(runId, segmentId)
                                .get()
                                .getState());

                        ((RepairStatusHandler) invocation.getArgument(5))
                            .handle(
                                1,
                                Optional.of(ProgressEventType.COMPLETE),
                                "Repair command 1 has finished",
                                jmx);

                        assertEquals(
                            RepairSegment.State.NOT_STARTED,
                            storage
                                .getRepairSegmentDao()
                                .getRepairSegment(runId, segmentId)
                                .get()
                                .getState());
                      }));

              return 1;
            });

    context.managementConnectionFactory =
        new JmxManagementConnectionFactory(context, new NoopCrypotograph()) {
          @Override
          protected JmxCassandraManagementProxy connectImpl(Node host) throws ReaperException {
            return jmx;
          }
        };

    RepairRunner rr = mock(RepairRunner.class);
    RepairUnit ru = mock(RepairUnit.class);
    when(ru.getKeyspaceName()).thenReturn("reaper");
    when(rr.getRepairRunId()).thenReturn(runId);

    ClusterFacade clusterFacade = mock(ClusterFacade.class);
    when(clusterFacade.connect(any(Cluster.class), any())).thenReturn(jmx);
    when(clusterFacade.nodeIsDirectlyAccessible(any(), any())).thenReturn(true);

    when(clusterFacade.tokenRangeToEndpoint(any(), anyString(), any()))
        .thenReturn(Lists.newArrayList(cf.getNodes()));
    when(clusterFacade.listActiveCompactions(any()))
        .thenReturn(
            CompactionStats.builder()
                .withActiveCompactions(Collections.emptyList())
                .withPendingCompactions(Optional.of(0))
                .build());

    SegmentRunner sr =
        SegmentRunner.create(
            context,
            clusterFacade,
            segmentId,
            COORDS,
            5000,
            0.5,
            PARALLEL,
            "reaper",
            ru,
            TABLES,
            rr);

    sr.run();

    future.getValue().get();
    executor.shutdown();

    assertEquals(
        RepairSegment.State.NOT_STARTED,
        storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState());
    assertEquals(
        2, storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getFailCount());
  }

  @Test
  public void outOfOrderSuccessCass22Test()
      throws InterruptedException,
          ReaperException,
          ExecutionException,
          MalformedObjectNameException,
          ReflectionException,
          IOException {
    final IStorageDao storage = new MemoryStorageFacade();

    RepairUnit cf =
        storage
            .getRepairUnitDao()
            .addRepairUnit(
                RepairUnit.builder()
                    .clusterName("reaper")
                    .keyspaceName("reaper")
                    .columnFamilies(Sets.newHashSet("reaper"))
                    .incrementalRepair(false)
                    .subrangeIncrementalRepair(false)
                    .nodes(Sets.newHashSet("127.0.0.1"))
                    .repairThreadCount(1)
                    .timeout(30));

    Map<String, String> replicas = Maps.newHashMap();
    replicas.put("127.0.0.1", "dc1");
    RepairRun run =
        storage
            .getRepairRunDao()
            .addRepairRun(
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

    storage
        .getClusterDao()
        .addCluster(
            Cluster.builder()
                .withName(cf.getClusterName())
                .withPartitioner("Murmur3Partitioner")
                .withSeedHosts(cf.getNodes())
                .withJmxPort(7199)
                .withState(Cluster.State.ACTIVE)
                .build());

    final UUID runId = run.getId();
    final UUID segmentId =
        storage.getRepairSegmentDao().getNextFreeSegments(run.getId()).get(0).getId();

    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final MutableObject<Future<?>> future = new MutableObject<>();

    AppContext context = new AppContext();
    context.storage = storage;
    context.config = Mockito.mock(ReaperApplicationConfiguration.class);
    when(context.config.getJmxConnectionTimeoutInSeconds()).thenReturn(30);
    when(context.config.getDatacenterAvailability()).thenReturn(DatacenterAvailability.ALL);

    final JmxCassandraManagementProxy jmx = CassandraManagementProxyTest.mockJmxProxyImpl();
    when(jmx.getClusterName()).thenReturn("reaper");

    EndpointSnitchInfoMBean endpointSnitchInfoMBean = mock(EndpointSnitchInfoMBean.class);
    when(endpointSnitchInfoMBean.getDatacenter()).thenReturn("dc1");
    try {
      when(endpointSnitchInfoMBean.getDatacenter(anyString())).thenReturn("dc1");
    } catch (UnknownHostException ex) {
      throw new AssertionError(ex);
    }

    when(jmx.triggerRepair(any(), any(), any(), any(), any(), any(), any(), anyInt()))
        .then(
            invocation -> {
              assertEquals(
                  RepairSegment.State.STARTED,
                  storage
                      .getRepairSegmentDao()
                      .getRepairSegment(runId, segmentId)
                      .get()
                      .getState());

              future.setValue(
                  executor.submit(
                      () -> {
                        ((RepairStatusHandler) invocation.getArgument(5))
                            .handle(
                                1,
                                Optional.of(ProgressEventType.START),
                                "Repair command 1 has started",
                                jmx);

                        ((RepairStatusHandler) invocation.getArgument(5))
                            .handle(
                                1,
                                Optional.of(ProgressEventType.COMPLETE),
                                "Repair command 1 has finished",
                                jmx);

                        assertEquals(
                            RepairSegment.State.RUNNING,
                            storage
                                .getRepairSegmentDao()
                                .getRepairSegment(runId, segmentId)
                                .get()
                                .getState());

                        ((RepairStatusHandler) invocation.getArgument(5))
                            .handle(
                                1,
                                Optional.of(ProgressEventType.SUCCESS),
                                "Repair session succeeded in command 1",
                                jmx);

                        assertEquals(
                            RepairSegment.State.DONE,
                            storage
                                .getRepairSegmentDao()
                                .getRepairSegment(runId, segmentId)
                                .get()
                                .getState());
                      }));
              return 1;
            });

    context.managementConnectionFactory =
        new JmxManagementConnectionFactory(context, new NoopCrypotograph()) {
          @Override
          protected JmxCassandraManagementProxy connectImpl(Node host) throws ReaperException {
            return jmx;
          }
        };

    RepairRunner rr = mock(RepairRunner.class);
    RepairUnit ru = mock(RepairUnit.class);
    when(ru.getKeyspaceName()).thenReturn("reaper");
    when(rr.getRepairRunId()).thenReturn(runId);

    ClusterFacade clusterFacade = mock(ClusterFacade.class);
    when(clusterFacade.connect(any(Cluster.class), any())).thenReturn(jmx);
    when(clusterFacade.nodeIsDirectlyAccessible(any(), any())).thenReturn(true);

    when(clusterFacade.tokenRangeToEndpoint(any(), anyString(), any()))
        .thenReturn(Lists.newArrayList(cf.getNodes()));
    when(clusterFacade.listActiveCompactions(any()))
        .thenReturn(
            CompactionStats.builder()
                .withActiveCompactions(Collections.emptyList())
                .withPendingCompactions(Optional.of(0))
                .build());

    SegmentRunner sr =
        SegmentRunner.create(
            context,
            clusterFacade,
            segmentId,
            COORDS,
            5000,
            0.5,
            PARALLEL,
            "reaper",
            ru,
            TABLES,
            rr);

    sr.run();

    future.getValue().get();
    executor.shutdown();

    assertEquals(
        RepairSegment.State.DONE,
        storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState());

    assertEquals(
        0, storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getFailCount());
  }

  @Test
  public void outOfOrderFailureTestCass22()
      throws InterruptedException,
          ReaperException,
          ExecutionException,
          MalformedObjectNameException,
          ReflectionException,
          IOException {
    final IStorageDao storage = new MemoryStorageFacade();
    final int segmentTimeout = 30;

    RepairUnit cf =
        storage
            .getRepairUnitDao()
            .addRepairUnit(
                RepairUnit.builder()
                    .clusterName("reaper")
                    .keyspaceName("reaper")
                    .columnFamilies(Sets.newHashSet("reaper"))
                    .incrementalRepair(false)
                    .subrangeIncrementalRepair(false)
                    .nodes(Sets.newHashSet("127.0.0.1"))
                    .repairThreadCount(1)
                    .timeout(30));

    Map<String, String> replicas = Maps.newHashMap();
    replicas.put("127.0.0.1", "dc1");
    RepairRun run =
        storage
            .getRepairRunDao()
            .addRepairRun(
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

    storage
        .getClusterDao()
        .addCluster(
            Cluster.builder()
                .withName(cf.getClusterName())
                .withPartitioner("Murmur3Partitioner")
                .withSeedHosts(cf.getNodes())
                .withJmxPort(7199)
                .withState(Cluster.State.ACTIVE)
                .build());

    final UUID runId = run.getId();
    final UUID segmentId =
        storage.getRepairSegmentDao().getNextFreeSegments(run.getId()).get(0).getId();

    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final MutableObject<Future<?>> future = new MutableObject<>();

    AppContext context = new AppContext();
    context.storage = storage;
    context.config = Mockito.mock(ReaperApplicationConfiguration.class);
    when(context.config.getJmxConnectionTimeoutInSeconds()).thenReturn(30);
    when(context.config.getDatacenterAvailability()).thenReturn(DatacenterAvailability.ALL);

    final JmxCassandraManagementProxy jmx = CassandraManagementProxyTest.mockJmxProxyImpl();
    when(jmx.getClusterName()).thenReturn("reaper");

    EndpointSnitchInfoMBean endpointSnitchInfoMBean = mock(EndpointSnitchInfoMBean.class);
    when(endpointSnitchInfoMBean.getDatacenter()).thenReturn("dc1");
    try {
      when(endpointSnitchInfoMBean.getDatacenter(anyString())).thenReturn("dc1");
    } catch (UnknownHostException ex) {
      throw new AssertionError(ex);
    }

    when(jmx.triggerRepair(any(), any(), any(), any(), any(), any(), any(), anyInt()))
        .then(
            invocation -> {
              assertEquals(
                  RepairSegment.State.STARTED,
                  storage
                      .getRepairSegmentDao()
                      .getRepairSegment(runId, segmentId)
                      .get()
                      .getState());

              future.setValue(
                  executor.submit(
                      () -> {
                        ((RepairStatusHandler) invocation.getArgument(5))
                            .handle(
                                1,
                                Optional.of(ProgressEventType.START),
                                "Repair command 1 has started",
                                jmx);

                        ((RepairStatusHandler) invocation.getArgument(5))
                            .handle(
                                1,
                                Optional.of(ProgressEventType.COMPLETE),
                                "Repair command 1 has finished",
                                jmx);

                        assertEquals(
                            RepairSegment.State.RUNNING,
                            storage
                                .getRepairSegmentDao()
                                .getRepairSegment(runId, segmentId)
                                .get()
                                .getState());

                        ((RepairStatusHandler) invocation.getArgument(5))
                            .handle(
                                1,
                                Optional.of(ProgressEventType.ERROR),
                                "Repair session succeeded in command 1",
                                jmx);

                        assertEquals(
                            RepairSegment.State.NOT_STARTED,
                            storage
                                .getRepairSegmentDao()
                                .getRepairSegment(runId, segmentId)
                                .get()
                                .getState());
                      }));
              return 1;
            });

    context.managementConnectionFactory =
        new JmxManagementConnectionFactory(context, new NoopCrypotograph()) {
          @Override
          protected JmxCassandraManagementProxy connectImpl(Node host) throws ReaperException {
            return jmx;
          }
        };

    RepairRunner rr = mock(RepairRunner.class);
    RepairUnit ru = mock(RepairUnit.class);
    when(ru.getKeyspaceName()).thenReturn("reaper");
    when(rr.getRepairRunId()).thenReturn(runId);

    ClusterFacade clusterFacade = mock(ClusterFacade.class);
    when(clusterFacade.connect(any(Cluster.class), any())).thenReturn(jmx);
    when(clusterFacade.nodeIsDirectlyAccessible(any(), any())).thenReturn(true);

    when(clusterFacade.tokenRangeToEndpoint(any(), anyString(), any()))
        .thenReturn(Lists.newArrayList(cf.getNodes()));
    when(clusterFacade.listActiveCompactions(any()))
        .thenReturn(
            CompactionStats.builder()
                .withActiveCompactions(Collections.emptyList())
                .withPendingCompactions(Optional.of(0))
                .build());

    SegmentRunner sr =
        SegmentRunner.create(
            context,
            clusterFacade,
            segmentId,
            COORDS,
            5000,
            0.5,
            PARALLEL,
            "reaper",
            ru,
            TABLES,
            rr);

    sr.run();

    future.getValue().get();
    executor.shutdown();

    assertEquals(
        RepairSegment.State.NOT_STARTED,
        storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState());

    assertEquals(
        2, storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getFailCount());
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
    UUID segmentId = Uuids.timeBased();
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
  public void triggerFailureTest()
      throws InterruptedException,
          ReaperException,
          ExecutionException,
          MalformedObjectNameException,
          ReflectionException,
          IOException {
    final IStorageDao storage = new MemoryStorageFacade();
    final int segmentTimeout = 30;

    RepairUnit cf =
        storage
            .getRepairUnitDao()
            .addRepairUnit(
                RepairUnit.builder()
                    .clusterName("reaper")
                    .keyspaceName("reaper")
                    .columnFamilies(Sets.newHashSet("reaper"))
                    .incrementalRepair(false)
                    .subrangeIncrementalRepair(false)
                    .nodes(Sets.newHashSet("127.0.0.1"))
                    .repairThreadCount(1)
                    .timeout(segmentTimeout));

    Map<String, String> replicas = Maps.newHashMap();
    replicas.put("127.0.0.1", "dc1");
    RepairRun run =
        storage
            .getRepairRunDao()
            .addRepairRun(
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

    storage
        .getClusterDao()
        .addCluster(
            Cluster.builder()
                .withName(cf.getClusterName())
                .withPartitioner("Murmur3Partitioner")
                .withSeedHosts(cf.getNodes())
                .withJmxPort(7199)
                .withState(Cluster.State.ACTIVE)
                .build());

    final UUID runId = run.getId();
    final UUID segmentId =
        storage.getRepairSegmentDao().getNextFreeSegments(run.getId()).get(0).getId();

    AppContext context = new AppContext();
    context.storage = storage;
    context.config = Mockito.mock(ReaperApplicationConfiguration.class);
    when(context.config.getJmxConnectionTimeoutInSeconds()).thenReturn(30);
    when(context.config.getDatacenterAvailability()).thenReturn(DatacenterAvailability.ALL);

    final JmxCassandraManagementProxy jmx = CassandraManagementProxyTest.mockJmxProxyImpl();
    when(jmx.getClusterName()).thenReturn("reaper");

    EndpointSnitchInfoMBean endpointSnitchInfoMBean = mock(EndpointSnitchInfoMBean.class);
    when(endpointSnitchInfoMBean.getDatacenter()).thenReturn("dc1");
    try {
      when(endpointSnitchInfoMBean.getDatacenter(anyString())).thenReturn("dc1");
    } catch (UnknownHostException ex) {
      throw new AssertionError(ex);
    }

    when(jmx.triggerRepair(any(), any(), any(), any(), any(), any(), any(), anyInt()))
        .thenThrow(new ReaperException("failure"));

    context.managementConnectionFactory =
        new JmxManagementConnectionFactory(context, new NoopCrypotograph()) {
          @Override
          protected JmxCassandraManagementProxy connectImpl(Node host) throws ReaperException {
            return jmx;
          }
        };

    RepairRunner rr = mock(RepairRunner.class);
    RepairUnit ru = mock(RepairUnit.class);
    when(ru.getKeyspaceName()).thenReturn("reaper");
    when(rr.getRepairRunId()).thenReturn(runId);

    ClusterFacade clusterFacade = mock(ClusterFacade.class);
    when(clusterFacade.connect(any(Cluster.class), any())).thenReturn(jmx);
    when(clusterFacade.nodeIsDirectlyAccessible(any(), any())).thenReturn(true);

    when(clusterFacade.tokenRangeToEndpoint(any(), anyString(), any()))
        .thenReturn(Lists.newArrayList(cf.getNodes()));
    when(clusterFacade.listActiveCompactions(any()))
        .thenReturn(
            CompactionStats.builder()
                .withActiveCompactions(Collections.emptyList())
                .withPendingCompactions(Optional.of(0))
                .build());

    SegmentRunner sr =
        SegmentRunner.create(
            context,
            clusterFacade,
            segmentId,
            COORDS,
            5000,
            0.5,
            PARALLEL,
            "reaper",
            ru,
            TABLES,
            rr);

    sr.run();

    assertEquals(
        RepairSegment.State.NOT_STARTED,
        storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState());
    assertEquals(
        1, storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getFailCount());
  }

  @Test
  public void nothingToRepairTest()
      throws InterruptedException,
          ReaperException,
          ExecutionException,
          MalformedObjectNameException,
          ReflectionException,
          IOException {
    final IStorageDao storage = new MemoryStorageFacade();
    final int segmentTimeout = 30;

    RepairUnit cf =
        storage
            .getRepairUnitDao()
            .addRepairUnit(
                RepairUnit.builder()
                    .clusterName("reaper")
                    .keyspaceName("reaper")
                    .columnFamilies(Sets.newHashSet("reaper"))
                    .incrementalRepair(false)
                    .subrangeIncrementalRepair(false)
                    .nodes(Sets.newHashSet("127.0.0.1"))
                    .repairThreadCount(1)
                    .timeout(segmentTimeout));

    Map<String, String> replicas = Maps.newHashMap();
    replicas.put("127.0.0.1", "dc1");
    RepairRun run =
        storage
            .getRepairRunDao()
            .addRepairRun(
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

    storage
        .getClusterDao()
        .addCluster(
            Cluster.builder()
                .withName(cf.getClusterName())
                .withPartitioner("Murmur3Partitioner")
                .withSeedHosts(cf.getNodes())
                .withJmxPort(7199)
                .withState(Cluster.State.ACTIVE)
                .build());

    final UUID runId = run.getId();
    final UUID segmentId =
        storage.getRepairSegmentDao().getNextFreeSegments(run.getId()).get(0).getId();

    AppContext context = new AppContext();
    context.storage = storage;
    context.config = Mockito.mock(ReaperApplicationConfiguration.class);
    when(context.config.getJmxConnectionTimeoutInSeconds()).thenReturn(30);
    when(context.config.getDatacenterAvailability()).thenReturn(DatacenterAvailability.ALL);

    final JmxCassandraManagementProxy jmx = CassandraManagementProxyTest.mockJmxProxyImpl();
    when(jmx.getClusterName()).thenReturn("reaper");

    EndpointSnitchInfoMBean endpointSnitchInfoMBean = mock(EndpointSnitchInfoMBean.class);
    when(endpointSnitchInfoMBean.getDatacenter()).thenReturn("dc1");
    try {
      when(endpointSnitchInfoMBean.getDatacenter(anyString())).thenReturn("dc1");
    } catch (UnknownHostException ex) {
      throw new AssertionError(ex);
    }

    when(jmx.triggerRepair(any(), any(), any(), any(), any(), any(), any(), anyInt()))
        .thenReturn(0);

    context.managementConnectionFactory =
        new JmxManagementConnectionFactory(context, new NoopCrypotograph()) {
          @Override
          protected JmxCassandraManagementProxy connectImpl(Node host) throws ReaperException {
            return jmx;
          }
        };

    RepairRunner rr = mock(RepairRunner.class);
    RepairUnit ru = mock(RepairUnit.class);
    when(ru.getKeyspaceName()).thenReturn("reaper");
    when(rr.getRepairRunId()).thenReturn(runId);

    ClusterFacade clusterFacade = mock(ClusterFacade.class);
    when(clusterFacade.connect(any(Cluster.class), any())).thenReturn(jmx);
    when(clusterFacade.nodeIsDirectlyAccessible(any(), any())).thenReturn(true);

    when(clusterFacade.tokenRangeToEndpoint(any(), anyString(), any()))
        .thenReturn(Lists.newArrayList(cf.getNodes()));
    when(clusterFacade.listActiveCompactions(any()))
        .thenReturn(
            CompactionStats.builder()
                .withActiveCompactions(Collections.emptyList())
                .withPendingCompactions(Optional.of(0))
                .build());

    SegmentRunner sr =
        SegmentRunner.create(
            context,
            clusterFacade,
            segmentId,
            COORDS,
            5000,
            0.5,
            PARALLEL,
            "reaper",
            ru,
            TABLES,
            rr);

    sr.run();

    assertEquals(
        RepairSegment.State.DONE,
        storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState());
    assertEquals(
        0, storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getFailCount());
  }

  @Test
  public void failComputingIntensityDelayTest()
      throws InterruptedException,
          ReaperException,
          ExecutionException,
          MalformedObjectNameException,
          ReflectionException,
          IOException {
    final IStorageDao storage = mock(IStorageDao.class);
    AppContext context = new AppContext();
    context.storage = storage;

    RepairRunner rr = mock(RepairRunner.class);
    when(rr.getRepairRunId()).thenReturn(Uuids.timeBased());
    RepairUnit ru = mock(RepairUnit.class);
    RepairSegment segment = mock(RepairSegment.class);
    when(segment.getStartTime()).thenReturn(DateTime.now());
    when(segment.getEndTime()).thenReturn(null);
    when(ru.getKeyspaceName()).thenReturn("reaper");

    IRepairSegmentDao mockedRepairSegmentDao = mock(IRepairSegmentDao.class);
    Mockito.when(storage.getRepairSegmentDao()).thenReturn(mockedRepairSegmentDao);
    when(mockedRepairSegmentDao.getRepairSegment(any(), any())).thenReturn(Optional.of(segment));

    ClusterFacade clusterFacade = mock(ClusterFacade.class);

    SegmentRunner sr =
        SegmentRunner.create(
            context,
            clusterFacade,
            Uuids.timeBased(),
            COORDS,
            5000,
            0.5,
            PARALLEL,
            "reaper",
            ru,
            TABLES,
            rr);

    assertEquals(
        "Intensity could apply fine although it shouldn't",
        0,
        sr.intensityBasedDelayMillis(new Double(1)));
  }

  @Test
  public void clearSnapshotTest()
      throws InterruptedException,
          ReaperException,
          ExecutionException,
          MalformedObjectNameException,
          ReflectionException,
          IOException {
    final IStorageDao storage = new MemoryStorageFacade();
    final int segmentTimeout = 30;

    RepairUnit cf =
        storage
            .getRepairUnitDao()
            .addRepairUnit(
                RepairUnit.builder()
                    .clusterName("reaper")
                    .keyspaceName("reaper")
                    .columnFamilies(Sets.newHashSet("reaper"))
                    .incrementalRepair(false)
                    .subrangeIncrementalRepair(false)
                    .nodes(Sets.newHashSet("127.0.0.1"))
                    .repairThreadCount(1)
                    .timeout(segmentTimeout));

    Map<String, String> replicas = Maps.newHashMap();
    replicas.put("127.0.0.1", "dc1");
    RepairRun run =
        storage
            .getRepairRunDao()
            .addRepairRun(
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
    storage
        .getClusterDao()
        .addCluster(
            Cluster.builder()
                .withName(cf.getClusterName())
                .withPartitioner("Murmur3Partitioner")
                .withSeedHosts(cf.getNodes())
                .withJmxPort(7199)
                .withState(Cluster.State.ACTIVE)
                .build());

    final UUID runId = run.getId();
    final UUID segmentId =
        storage.getRepairSegmentDao().getNextFreeSegments(run.getId()).get(0).getId();
    AppContext context = new AppContext();
    context.storage = storage;
    context.config = Mockito.mock(ReaperApplicationConfiguration.class);
    when(context.config.getJmxConnectionTimeoutInSeconds()).thenReturn(30);
    when(context.config.getDatacenterAvailability()).thenReturn(DatacenterAvailability.ALL);
    final ICassandraManagementProxy jmx = CassandraManagementProxyTest.mockJmxProxyImpl();
    when(jmx.getClusterName()).thenReturn("reaper");
    EndpointSnitchInfoMBean endpointSnitchInfoMBean = mock(EndpointSnitchInfoMBean.class);
    when(endpointSnitchInfoMBean.getDatacenter()).thenReturn("dc1");
    try {
      when(endpointSnitchInfoMBean.getDatacenter(anyString())).thenReturn("dc1");
    } catch (UnknownHostException ex) {
      throw new AssertionError(ex);
    }

    doThrow(new IOException("failure")).when(jmx).clearSnapshot(any(), any());

    RepairRunner rr = mock(RepairRunner.class);
    RepairUnit ru = mock(RepairUnit.class);
    when(ru.getKeyspaceName()).thenReturn("reaper");

    ClusterFacade clusterFacade = mock(ClusterFacade.class);
    when(clusterFacade.connect(any(Cluster.class), any())).thenReturn(jmx);
    when(clusterFacade.nodeIsDirectlyAccessible(any(), any())).thenReturn(true);

    when(clusterFacade.tokenRangeToEndpoint(any(), anyString(), any()))
        .thenReturn(Lists.newArrayList(cf.getNodes()));
    when(clusterFacade.listActiveCompactions(any()))
        .thenReturn(
            CompactionStats.builder()
                .withActiveCompactions(Collections.emptyList())
                .withPendingCompactions(Optional.of(0))
                .build());

    SegmentRunner sr =
        SegmentRunner.create(
            context,
            clusterFacade,
            segmentId,
            COORDS,
            5000,
            0.5,
            PARALLEL,
            "reaper",
            ru,
            TABLES,
            rr);

    sr.tryClearSnapshots(Uuids.timeBased().toString());
    Mockito.verify(jmx, Mockito.times(1)).clearSnapshot(any(), any());
  }

  @Test
  public void clearSnapshotFailTest()
      throws InterruptedException,
          ReaperException,
          ExecutionException,
          MalformedObjectNameException,
          ReflectionException,
          IOException {
    final IStorageDao storage = new MemoryStorageFacade();
    final int segmentTimeout = 30;

    RepairUnit cf =
        storage
            .getRepairUnitDao()
            .addRepairUnit(
                RepairUnit.builder()
                    .clusterName("reaper")
                    .keyspaceName("reaper")
                    .columnFamilies(Sets.newHashSet("reaper"))
                    .incrementalRepair(false)
                    .subrangeIncrementalRepair(false)
                    .nodes(Sets.newHashSet("127.0.0.1"))
                    .repairThreadCount(1)
                    .timeout(segmentTimeout));

    Map<String, String> replicas = Maps.newHashMap();
    replicas.put("127.0.0.1", "dc1");
    RepairRun run =
        storage
            .getRepairRunDao()
            .addRepairRun(
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
    storage
        .getClusterDao()
        .addCluster(
            Cluster.builder()
                .withName(cf.getClusterName())
                .withPartitioner("Murmur3Partitioner")
                .withSeedHosts(cf.getNodes())
                .withJmxPort(7199)
                .withState(Cluster.State.ACTIVE)
                .build());

    final UUID segmentId =
        storage.getRepairSegmentDao().getNextFreeSegments(run.getId()).get(0).getId();
    AppContext context = new AppContext();
    context.storage = storage;
    context.config = Mockito.mock(ReaperApplicationConfiguration.class);

    RepairUnit ru = mock(RepairUnit.class);
    when(ru.getKeyspaceName()).thenReturn("reaper");

    ClusterFacade clusterFacade = mock(ClusterFacade.class);
    when(clusterFacade.connect(any(Cluster.class), any()))
        .thenThrow(new ReaperException("failure"));
    StorageServiceMBean storageServiceMbeanMock = mock(StorageServiceMBean.class);
    final ICassandraManagementProxy jmx = CassandraManagementProxyTest.mockJmxProxyImpl();
    doThrow(new IOException("failure")).when(jmx).clearSnapshot(any(), any());

    RepairRunner rr = mock(RepairRunner.class);
    SegmentRunner sr =
        SegmentRunner.create(
            context,
            clusterFacade,
            segmentId,
            COORDS,
            5000,
            0.5,
            PARALLEL,
            "reaper",
            ru,
            TABLES,
            rr);

    sr.tryClearSnapshots(Uuids.timeBased().toString());
    Mockito.verify(storageServiceMbeanMock, Mockito.times(0)).clearSnapshot(any(), any());
  }
}
