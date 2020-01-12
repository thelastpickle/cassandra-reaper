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
import io.cassandrareaper.storage.MemoryStorage;

import java.math.BigInteger;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.progress.ProgressEventType;
import org.apache.commons.lang3.RandomStringUtils;
import org.assertj.core.util.Maps;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class RepairRunnerTest {

  private static final Logger LOG = LoggerFactory.getLogger(RepairRunnerTest.class);
  private static final Set<String> TABLES = ImmutableSet.of("table1");

  private final Cluster cluster = Cluster.builder()
            .withName("test_" + RandomStringUtils.randomAlphabetic(12))
            .withSeedHosts(ImmutableSet.of("127.0.0.1"))
            .withState(Cluster.State.ACTIVE)
            .build();

  @Before
  public void setUp() throws Exception {
    SegmentRunner.SEGMENT_RUNNERS.clear();
  }

  @Test
  public void testHangingRepair() throws InterruptedException, ReaperException {
    final String KS_NAME = "reaper";
    final Set<String> CF_NAMES = Sets.newHashSet("reaper");
    final boolean INCREMENTAL_REPAIR = false;
    final Set<String> NODES = Sets.newHashSet("127.0.0.1");
    final Set<String> DATACENTERS = Collections.emptySet();
    final Set<String> BLACKLISTED_TABLES = Collections.emptySet();
    final long TIME_RUN = 41L;
    final double INTENSITY = 0.5f;
    final int REPAIR_THREAD_COUNT = 1;
    final IStorage storage = new MemoryStorage();

    storage.addCluster(cluster);

    RepairUnit cf = storage.addRepairUnit(
            RepairUnit.builder()
            .clusterName(cluster.getName())
            .keyspaceName(KS_NAME)
            .columnFamilies(CF_NAMES)
            .incrementalRepair(INCREMENTAL_REPAIR)
            .nodes(NODES)
            .datacenters(DATACENTERS)
            .blacklistedTables(BLACKLISTED_TABLES)
            .repairThreadCount(REPAIR_THREAD_COUNT));
    DateTimeUtils.setCurrentMillisFixed(TIME_RUN);
    RepairRun run = storage.addRepairRun(
            RepairRun.builder(cluster.getName(), cf.getId())
                .intensity(INTENSITY)
                .segmentCount(1)
                .repairParallelism(RepairParallelism.PARALLEL)
                .tables(TABLES),
            Collections.singleton(
                RepairSegment.builder(
                    Segment.builder().withTokenRange(new RingRange(BigInteger.ZERO, new BigInteger("100"))).build(),
                    cf.getId())));
    final UUID RUN_ID = run.getId();
    final UUID SEGMENT_ID = storage.getNextFreeSegmentInRange(run.getId(), Optional.empty()).get().getId();
    assertEquals(storage.getRepairSegment(RUN_ID, SEGMENT_ID).get().getState(), RepairSegment.State.NOT_STARTED);
    AppContext context = new AppContext();
    context.storage = storage;
    context.config = new ReaperApplicationConfiguration();
    final Semaphore mutex = new Semaphore(0);
    final JmxProxy jmx = JmxProxyTest.mockJmxProxyImpl();
    when(jmx.getClusterName()).thenReturn(cluster.getName());
    when(jmx.isConnectionAlive()).thenReturn(true);
    when(jmx.getRangeToEndpointMap(anyString())).thenReturn(RepairRunnerTest.sixNodeCluster());
    EndpointSnitchInfoMBean endpointSnitchInfoMBean = mock(EndpointSnitchInfoMBean.class);
    when(endpointSnitchInfoMBean.getDatacenter()).thenReturn("dc1");
    try {
      when(endpointSnitchInfoMBean.getDatacenter(anyString())).thenReturn("dc1");
    } catch (UnknownHostException ex) {
      throw new AssertionError(ex);
    }
    JmxProxyTest.mockGetEndpointSnitchInfoMBean(jmx, endpointSnitchInfoMBean);
    final AtomicInteger repairAttempts = new AtomicInteger(1);

    when(jmx.triggerRepair(any(), any(), any(), any(), any(), anyBoolean(), any(), any(), any(), anyInt()))
        .then(
            (invocation) -> {
              assertEquals(RepairSegment.State.STARTED, storage.getRepairSegment(RUN_ID, SEGMENT_ID).get().getState());
              final int repairNumber = repairAttempts.getAndIncrement();
              switch (repairNumber) {
                case 1:
                  new Thread() {
                    @Override
                    public void run() {
                      ((RepairStatusHandler)invocation.getArgument(7))
                          .handle(repairNumber,
                              Optional.of(ActiveRepairService.Status.STARTED),
                              Optional.empty(), null, jmx);
                      assertEquals(
                          RepairSegment.State.RUNNING,
                          storage.getRepairSegment(RUN_ID, SEGMENT_ID).get().getState());
                    }
                  }.start();
                  break;
                case 2:
                  new Thread() {
                    @Override
                    public void run() {
                      ((RepairStatusHandler)invocation.getArgument(7))
                          .handle(
                              repairNumber,
                              Optional.of(ActiveRepairService.Status.STARTED),
                              Optional.empty(), null, jmx);
                      assertEquals(
                          RepairSegment.State.RUNNING,
                          storage.getRepairSegment(RUN_ID, SEGMENT_ID).get().getState());
                      ((RepairStatusHandler)invocation.getArgument(7))
                          .handle(
                              repairNumber,
                              Optional.of(ActiveRepairService.Status.SESSION_SUCCESS),
                              Optional.empty(), null, jmx);
                      assertEquals(
                          RepairSegment.State.DONE,
                          storage.getRepairSegment(RUN_ID, SEGMENT_ID).get().getState());
                      ((RepairStatusHandler)invocation.getArgument(7))
                          .handle(repairNumber,
                              Optional.of(ActiveRepairService.Status.FINISHED),
                              Optional.empty(), null, jmx);

                      mutex.release();
                      LOG.info("MUTEX RELEASED");
                    }
                  }.start();
                  break;
                default:
                  fail("triggerRepair should only have been called twice");
              }
              LOG.info("repair number : " + repairNumber);
              return repairNumber;
            });
    ClusterFacade clusterFacade = mock(ClusterFacade.class);
    when(clusterFacade.connect(any(Cluster.class), any())).thenReturn(jmx);
    when(clusterFacade.nodeIsAccessibleThroughJmx(any(), any())).thenReturn(true);
    when(clusterFacade.tokenRangeToEndpoint(any(), anyString(), any())).thenReturn(Lists.newArrayList(NODES));
    when(clusterFacade.getRangeToEndpointMap(any(), anyString()))
        .thenReturn((Map)ImmutableMap.of(Lists.newArrayList("0", "100"), Lists.newArrayList(NODES)));

    context.repairManager = RepairManager.create(
            context,
            clusterFacade,
            Executors.newScheduledThreadPool(1),
            500,
            TimeUnit.MILLISECONDS,
            1,
            TimeUnit.MILLISECONDS);
    context.jmxConnectionFactory = new JmxConnectionFactory(context, new NoopCrypotograph()) {
          @Override
          protected JmxProxy connectImpl(Node host) throws ReaperException {
            return jmx;
          }
        };
    context.repairManager.startRepairRun(run);
    await().with().atMost(20, TimeUnit.SECONDS).until(() -> {
      try {
        mutex.acquire();
        LOG.info("MUTEX ACQUIRED");
        // TODO: refactor so that we can properly wait for the repair runner to finish rather than using this sleep()
        Thread.sleep(1000);
        return true;
      } catch (InterruptedException ex) {
        throw new IllegalStateException(ex);
      }
    });
    assertEquals(RepairRun.RunState.DONE, storage.getRepairRun(RUN_ID).get().getRunState());
  }

  @Test
  public void testHangingRepairNewAPI() throws InterruptedException, ReaperException {
    final String KS_NAME = "reaper";
    final Set<String> CF_NAMES = Sets.newHashSet("reaper");
    final boolean INCREMENTAL_REPAIR = false;
    final Set<String> NODES = Sets.newHashSet("127.0.0.1");
    final Set<String> DATACENTERS = Collections.emptySet();
    final Set<String> BLACKLISTED_TABLES = Collections.emptySet();
    final long TIME_RUN = 41L;
    final double INTENSITY = 0.5f;
    final int REPAIR_THREAD_COUNT = 1;
    final IStorage storage = new MemoryStorage();

    storage.addCluster(cluster);

    DateTimeUtils.setCurrentMillisFixed(TIME_RUN);
    RepairUnit cf = storage.addRepairUnit(
            RepairUnit.builder()
            .clusterName(cluster.getName())
            .keyspaceName(KS_NAME)
            .columnFamilies(CF_NAMES)
            .incrementalRepair(INCREMENTAL_REPAIR)
            .nodes(NODES)
            .datacenters(DATACENTERS)
            .blacklistedTables(BLACKLISTED_TABLES)
            .repairThreadCount(REPAIR_THREAD_COUNT));
    RepairRun run = storage.addRepairRun(
            RepairRun.builder(cluster.getName(), cf.getId())
                .intensity(INTENSITY).segmentCount(1)
                .repairParallelism(RepairParallelism.PARALLEL)
                .tables(TABLES),
            Collections.singleton(
                RepairSegment.builder(
                    Segment.builder().withTokenRange(new RingRange(BigInteger.ZERO, new BigInteger("100"))).build(),
                    cf.getId())));
    final UUID RUN_ID = run.getId();
    final UUID SEGMENT_ID = storage.getNextFreeSegmentInRange(run.getId(), Optional.empty()).get().getId();
    assertEquals(storage.getRepairSegment(RUN_ID, SEGMENT_ID).get().getState(), RepairSegment.State.NOT_STARTED);
    AppContext context = new AppContext();
    context.storage = storage;
    context.config = new ReaperApplicationConfiguration();
    final Semaphore mutex = new Semaphore(0);
    final JmxProxy jmx = JmxProxyTest.mockJmxProxyImpl();
    when(jmx.getClusterName()).thenReturn(cluster.getName());
    when(jmx.isConnectionAlive()).thenReturn(true);
    when(jmx.getRangeToEndpointMap(anyString())).thenReturn(RepairRunnerTest.sixNodeCluster());
    EndpointSnitchInfoMBean endpointSnitchInfoMBean = mock(EndpointSnitchInfoMBean.class);
    when(endpointSnitchInfoMBean.getDatacenter()).thenReturn("dc1");
    try {
      when(endpointSnitchInfoMBean.getDatacenter(anyString())).thenReturn("dc1");
    } catch (UnknownHostException ex) {
      throw new AssertionError(ex);
    }
    JmxProxyTest.mockGetEndpointSnitchInfoMBean(jmx, endpointSnitchInfoMBean);
    final AtomicInteger repairAttempts = new AtomicInteger(1);

    when(jmx.triggerRepair(any(), any(), any(), any(), any(), anyBoolean(), any(), any(), any(), anyInt()))
        .then(
            (invocation) -> {
              assertEquals(
                  RepairSegment.State.STARTED,
                  storage.getRepairSegment(RUN_ID, SEGMENT_ID).get().getState());
              final int repairNumber = repairAttempts.getAndIncrement();
              switch (repairNumber) {
                case 1:
                  new Thread() {
                    @Override
                    public void run() {
                      ((RepairStatusHandler)invocation.getArgument(7))
                          .handle(
                              repairNumber, Optional.empty(),
                              Optional.of(ProgressEventType.START), null, jmx);
                      assertEquals(
                          RepairSegment.State.RUNNING,
                          storage.getRepairSegment(RUN_ID, SEGMENT_ID).get().getState());
                    }
                  }.start();
                  break;
                case 2:
                  new Thread() {
                    @Override
                    public void run() {
                      ((RepairStatusHandler)invocation.getArgument(7))
                          .handle(
                              repairNumber, Optional.empty(),
                              Optional.of(ProgressEventType.START), null, jmx);
                      assertEquals(
                          RepairSegment.State.RUNNING,
                          storage.getRepairSegment(RUN_ID, SEGMENT_ID).get().getState());
                      ((RepairStatusHandler)invocation.getArgument(7))
                          .handle(
                              repairNumber, Optional.empty(),
                              Optional.of(ProgressEventType.SUCCESS), null, jmx);
                      assertEquals(
                          RepairSegment.State.DONE,
                          storage.getRepairSegment(RUN_ID, SEGMENT_ID).get().getState());
                      ((RepairStatusHandler)invocation.getArgument(7))
                          .handle(
                              repairNumber, Optional.empty(),
                              Optional.of(ProgressEventType.COMPLETE), null, jmx);
                      mutex.release();
                      LOG.info("MUTEX RELEASED");
                    }
                  }.start();
                  break;
                default:
                  fail("triggerRepair should only have been called twice");
              }
              return repairNumber;
            });
    ClusterFacade clusterFacade = mock(ClusterFacade.class);
    when(clusterFacade.connect(any(Cluster.class), any())).thenReturn(jmx);
    when(clusterFacade.nodeIsAccessibleThroughJmx(any(), any())).thenReturn(true);
    when(clusterFacade.tokenRangeToEndpoint(any(), anyString(), any()))
        .thenReturn(Lists.newArrayList(NODES));
    when(clusterFacade.getRangeToEndpointMap(any(), anyString()))
        .thenReturn((Map)ImmutableMap.of(Lists.newArrayList("0", "100"), Lists.newArrayList(NODES)));
    context.repairManager
        = RepairManager.create(
            context,
            clusterFacade,
            Executors.newScheduledThreadPool(1),
            500,
            TimeUnit.MILLISECONDS,
            1,
            TimeUnit.MILLISECONDS);
    context.jmxConnectionFactory = new JmxConnectionFactory(context, new NoopCrypotograph()) {
          @Override
          protected JmxProxy connectImpl(Node host) throws ReaperException {
            return jmx;
          }
        };
    context.repairManager.startRepairRun(run);
    await().with().atMost(20, TimeUnit.SECONDS).until(() -> {
      try {
        mutex.acquire();
        LOG.info("MUTEX ACQUIRED");
        // TODO: refactor so that we can properly wait for the repair runner to finish rather than using this sleep()
        Thread.sleep(1000);
        return true;
      } catch (InterruptedException ex) {
        throw new IllegalStateException(ex);
      }
    });
    assertEquals(RepairRun.RunState.DONE, storage.getRepairRun(RUN_ID).get().getRunState());
  }

  @Test
  public void testResumeRepair() throws InterruptedException, ReaperException {
    final String KS_NAME = "reaper";
    final Set<String> CF_NAMES = Sets.newHashSet("reaper");
    final boolean INCREMENTAL_REPAIR = false;
    final Set<String> NODES = Sets.newHashSet("127.0.0.1");
    final Map<String, String> NODES_MAP = Maps.newHashMap("node1", "127.0.0.1");
    final Set<String> DATACENTERS = Collections.emptySet();
    final Set<String> BLACKLISTED_TABLES = Collections.emptySet();
    final long TIME_RUN = 41L;
    final double INTENSITY = 0.5f;
    final int REPAIR_THREAD_COUNT = 1;
    final List<BigInteger> TOKENS = Lists.newArrayList(
        BigInteger.valueOf(0L),
        BigInteger.valueOf(100L),
        BigInteger.valueOf(200L));
    final IStorage storage = new MemoryStorage();
    AppContext context = new AppContext();
    context.storage = storage;
    context.config = new ReaperApplicationConfiguration();

    storage.addCluster(cluster);

    UUID cf = storage.addRepairUnit(
        RepairUnit.builder()
            .clusterName(cluster.getName())
            .keyspaceName(KS_NAME)
            .columnFamilies(CF_NAMES)
            .incrementalRepair(INCREMENTAL_REPAIR)
            .nodes(NODES)
            .datacenters(DATACENTERS)
            .blacklistedTables(BLACKLISTED_TABLES)
            .repairThreadCount(REPAIR_THREAD_COUNT))
        .getId();

    DateTimeUtils.setCurrentMillisFixed(TIME_RUN);

    RepairRun run = storage.addRepairRun(
            RepairRun.builder(cluster.getName(), cf)
                .intensity(INTENSITY)
                .segmentCount(1)
                .repairParallelism(RepairParallelism.PARALLEL)
                .tables(TABLES),
            Lists.newArrayList(
                RepairSegment.builder(
                        Segment.builder()
                            .withTokenRange(new RingRange(BigInteger.ZERO, new BigInteger("100")))
                            .build(),
                        cf)
                    .withState(RepairSegment.State.RUNNING)
                    .withStartTime(DateTime.now())
                    .withCoordinatorHost("reaper"),
                RepairSegment.builder(
                    Segment.builder()
                        .withTokenRange(new RingRange(new BigInteger("100"), new BigInteger("200")))
                        .build(),
                    cf)));

    final UUID RUN_ID = run.getId();
    final UUID SEGMENT_ID = storage.getNextFreeSegmentInRange(run.getId(), Optional.empty()).get().getId();
    assertEquals(storage.getRepairSegment(RUN_ID, SEGMENT_ID).get().getState(), RepairSegment.State.NOT_STARTED);
    final JmxProxy jmx = JmxProxyTest.mockJmxProxyImpl();
    when(jmx.getClusterName()).thenReturn(cluster.getName());
    when(jmx.isConnectionAlive()).thenReturn(true);
    when(jmx.getRangeToEndpointMap(anyString())).thenReturn(RepairRunnerTest.threeNodeClusterWithIps());
    when(jmx.getEndpointToHostId()).thenReturn(NODES_MAP);
    when(jmx.getTokens()).thenReturn(TOKENS);
    EndpointSnitchInfoMBean endpointSnitchInfoMBean = mock(EndpointSnitchInfoMBean.class);
    when(endpointSnitchInfoMBean.getDatacenter()).thenReturn("dc1");
    try {
      when(endpointSnitchInfoMBean.getDatacenter(anyString())).thenReturn("dc1");
    } catch (UnknownHostException ex) {
      throw new AssertionError(ex);
    }
    JmxProxyTest.mockGetEndpointSnitchInfoMBean(jmx, endpointSnitchInfoMBean);
    ClusterFacade clusterFacade = mock(ClusterFacade.class);
    when(clusterFacade.connect(any(Cluster.class), any())).thenReturn(jmx);
    when(clusterFacade.nodeIsAccessibleThroughJmx(any(), any())).thenReturn(true);
    when(clusterFacade.tokenRangeToEndpoint(any(), anyString(), any()))
        .thenReturn(Lists.newArrayList(NODES));
    when(clusterFacade.getRangeToEndpointMap(any(), anyString()))
        .thenReturn((Map)ImmutableMap.of(
            Lists.newArrayList("0", "100"), Lists.newArrayList(NODES),
            Lists.newArrayList("100", "200"), Lists.newArrayList(NODES)));

    context.repairManager = RepairManager.create(
        context,
        clusterFacade,
        Executors.newScheduledThreadPool(1),
        500,
        TimeUnit.MILLISECONDS,
        1,
        TimeUnit.MILLISECONDS);
    AtomicInteger repairNumberCounter = new AtomicInteger(1);

    when(jmx.triggerRepair(any(), any(), any(), any(), any(), anyBoolean(), any(), any(), any(), anyInt()))
        .then(
            (invocation) -> {
              final int repairNumber = repairNumberCounter.getAndIncrement();

              new Thread() {
                @Override
                public void run() {
                  ((RepairStatusHandler)invocation.getArgument(7))
                      .handle(
                          repairNumber,
                          Optional.of(ActiveRepairService.Status.STARTED),
                          Optional.empty(),
                          null,
                          jmx);
                  ((RepairStatusHandler)invocation.getArgument(7))
                      .handle(
                          repairNumber,
                          Optional.of(ActiveRepairService.Status.SESSION_SUCCESS),
                          Optional.empty(),
                          null,
                          jmx);
                  ((RepairStatusHandler)invocation.getArgument(7))
                      .handle(
                          repairNumber,
                          Optional.of(ActiveRepairService.Status.FINISHED),
                          Optional.empty(),
                          null,
                          jmx);
                }
              }.start();
              return repairNumber;
            });
    context.jmxConnectionFactory = new JmxConnectionFactory(context, new NoopCrypotograph()) {
          @Override
          protected JmxProxy connectImpl(Node host) throws ReaperException {
            return jmx;
          }
        };
    ClusterFacade clusterProxy = ClusterFacade.create(context);
    ClusterFacade clusterProxySpy = Mockito.spy(clusterProxy);
    Mockito.doReturn(Collections.singletonList("")).when(clusterProxySpy).tokenRangeToEndpoint(any(), any(), any());

    assertEquals(RepairRun.RunState.NOT_STARTED, storage.getRepairRun(RUN_ID).get().getRunState());
    context.repairManager.resumeRunningRepairRuns();
    assertEquals(RepairRun.RunState.NOT_STARTED, storage.getRepairRun(RUN_ID).get().getRunState());

    storage.updateRepairRun(
        run.with().runState(RepairRun.RunState.RUNNING).startTime(DateTime.now()).build(RUN_ID));

    context.repairManager.resumeRunningRepairRuns();
    Thread.sleep(1000);
    assertEquals(RepairRun.RunState.DONE, storage.getRepairRun(RUN_ID).get().getRunState());
  }

  @Test
  public void getPossibleParallelRepairsTest() throws Exception {
    Map<List<String>, List<String>> map = RepairRunnerTest.threeNodeCluster();
    Map<String, String> endpointsThreeNodes = RepairRunnerTest.threeNodeClusterEndpoint();
    assertEquals(1, RepairRunner.getPossibleParallelRepairsCount(map, endpointsThreeNodes, DatacenterAvailability.ALL));

    map = RepairRunnerTest.sixNodeCluster();
    Map<String, String> endpointsSixNodes = RepairRunnerTest.sixNodeClusterEndpoint();
    assertEquals(2, RepairRunner.getPossibleParallelRepairsCount(map, endpointsSixNodes, DatacenterAvailability.ALL));
    assertEquals(1,
        RepairRunner.getPossibleParallelRepairsCount(map, endpointsSixNodes, DatacenterAvailability.SIDECAR));
  }

  @Test
  public void getParallelSegmentsTest() throws ReaperException {
    List<BigInteger> tokens = Lists
        .transform(Lists.newArrayList("0", "50", "100", "150", "200", "250"), (string) -> new BigInteger(string));

    SegmentGenerator generator = new SegmentGenerator(new BigInteger("0"), new BigInteger("299"));

    List<Segment> segments = generator.generateSegments(
            32,
            tokens,
            Boolean.FALSE,
            RepairRunService.buildReplicasToRangeMap(RepairRunnerTest.sixNodeCluster()),
            "2.2.10");

    Map<List<String>, List<String>> map = RepairRunnerTest.sixNodeCluster();
    Map<String, String> endpointsSixNodes = RepairRunnerTest.sixNodeClusterEndpoint();

    List<RingRange> ranges = RepairRunner.getParallelRanges(
            RepairRunner.getPossibleParallelRepairsCount(map, endpointsSixNodes, DatacenterAvailability.ALL),
            Lists.newArrayList(
                Collections2.transform(segments, segment -> segment.getBaseRange())));

    assertEquals(2, ranges.size());
    assertEquals("0", ranges.get(0).getStart().toString());
    assertEquals("150", ranges.get(0).getEnd().toString());
    assertEquals("150", ranges.get(1).getStart().toString());
    assertEquals("0", ranges.get(1).getEnd().toString());
  }

  @Test
  public void getParallelSegmentsTest2() throws ReaperException {
    List<BigInteger> tokens = Lists.transform(
        Lists.newArrayList("0", "25", "50", "75", "100", "125", "150", "175", "200", "225", "250"),
        (string) -> new BigInteger(string));

    SegmentGenerator generator = new SegmentGenerator(new BigInteger("0"), new BigInteger("299"));

    Map<List<String>, List<String>> map = RepairRunnerTest.sixNodeCluster();
    Map<String, String> endpointsSixNodes = RepairRunnerTest.sixNodeClusterEndpoint();

    List<Segment> segments = generator.generateSegments(
            32, tokens, Boolean.FALSE, RepairRunService.buildReplicasToRangeMap(map), "2.2.10");

    List<RingRange> ranges = RepairRunner.getParallelRanges(
            RepairRunner.getPossibleParallelRepairsCount(map, endpointsSixNodes, DatacenterAvailability.ALL),
            Lists.newArrayList(
                Collections2.transform(segments, segment -> segment.getBaseRange())));

    assertEquals(2, ranges.size());
    assertEquals("0", ranges.get(0).getStart().toString());
    assertEquals("150", ranges.get(0).getEnd().toString());
    assertEquals("150", ranges.get(1).getStart().toString());
    assertEquals("0", ranges.get(1).getEnd().toString());
  }

  @Test
  public void getNoSegmentCoalescingTest() throws ReaperException {
    List<BigInteger> tokens = Lists.transform(
        Lists.newArrayList("0", "25", "50", "75", "100", "125", "150", "175", "200", "225", "250"),
        (string) -> new BigInteger(string));

    SegmentGenerator generator = new SegmentGenerator(new BigInteger("0"), new BigInteger("299"));

    Map<List<String>, List<String>> map = RepairRunnerTest.sixNodeCluster();
    List<Segment> segments = generator.generateSegments(
            6, tokens, Boolean.FALSE, RepairRunService.buildReplicasToRangeMap(map), "2.1.17");

    assertEquals(11, segments.size());
  }

  @Test
  public void getSegmentCoalescingTest() throws ReaperException {
    List<BigInteger> tokens = Lists.transform(
        Lists.newArrayList("0", "25", "50", "75", "100", "125", "150", "175", "200", "225", "250"),
        (string) -> new BigInteger(string));

    SegmentGenerator generator = new SegmentGenerator(new BigInteger("0"), new BigInteger("299"));

    Map<List<String>, List<String>> map = RepairRunnerTest.sixNodeCluster();

    List<Segment> segments = generator.generateSegments(
            6, tokens, Boolean.FALSE, RepairRunService.buildReplicasToRangeMap(map), "2.2.17");

    assertEquals(6, segments.size());
  }

  public static Map<List<String>, List<String>> threeNodeCluster() {
    Map<List<String>, List<String>> map = new HashMap<List<String>, List<String>>();
    map = addRangeToMap(map, "0", "50", "a1", "a2", "a3");
    map = addRangeToMap(map, "50", "100", "a2", "a3", "a1");
    map = addRangeToMap(map, "100", "0", "a3", "a1", "a2");
    return map;
  }

  public static Map<List<String>, List<String>> threeNodeClusterWithIps() {
    Map<List<String>, List<String>> map = new HashMap<List<String>, List<String>>();
    map = addRangeToMap(map, "0", "100", "127.0.0.1", "127.0.0.2", "127.0.0.3");
    map = addRangeToMap(map, "100", "200", "127.0.0.2", "127.0.0.3", "127.0.0.1");
    map = addRangeToMap(map, "200", "0", "127.0.0.3", "127.0.0.1", "127.0.0.2");
    return map;
  }

  public static Map<List<String>, List<String>> sixNodeCluster() {
    Map<List<String>, List<String>> map = new HashMap<List<String>, List<String>>();
    map = addRangeToMap(map, "0", "50", "a1", "a2", "a3");
    map = addRangeToMap(map, "50", "100", "a2", "a3", "a4");
    map = addRangeToMap(map, "100", "150", "a3", "a4", "a5");
    map = addRangeToMap(map, "150", "200", "a4", "a5", "a6");
    map = addRangeToMap(map, "200", "250", "a5", "a6", "a1");
    map = addRangeToMap(map, "250", "0", "a6", "a1", "a2");
    return map;
  }

  public static Map<String, String> threeNodeClusterEndpoint() {
    Map<String, String> map = new HashMap<String, String>();
    map.put("host1", "hostId1");
    map.put("host2", "hostId2");
    map.put("host3", "hostId3");
    return map;
  }

  public static Map<String, String> sixNodeClusterEndpoint() {
    Map<String, String> map = new HashMap<String, String>();
    map.put("host1", "hostId1");
    map.put("host2", "hostId2");
    map.put("host3", "hostId3");
    map.put("host4", "hostId4");
    map.put("host5", "hostId5");
    map.put("host6", "hostId6");
    return map;
  }

  private static Map<List<String>, List<String>> addRangeToMap(
      Map<List<String>, List<String>> map,
      String start,
      String end,
      String... hosts) {

    List<String> range = Lists.newArrayList(start, end);
    List<String> endPoints = Lists.newArrayList(hosts);
    map.put(range, endPoints);
    return map;
  }

}
