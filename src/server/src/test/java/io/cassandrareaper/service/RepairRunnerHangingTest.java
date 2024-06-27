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
import io.cassandrareaper.management.RepairStatusHandler;
import io.cassandrareaper.management.jmx.CassandraManagementProxyTest;
import io.cassandrareaper.management.jmx.JmxCassandraManagementProxy;
import io.cassandrareaper.management.jmx.JmxManagementConnectionFactory;
import io.cassandrareaper.storage.IStorageDao;
import io.cassandrareaper.storage.MemoryStorageFacade;

import java.io.IOException;
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
import javax.management.JMException;
import javax.management.MalformedObjectNameException;
import javax.management.ReflectionException;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.progress.ProgressEventType;
import org.apache.commons.lang3.RandomStringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class RepairRunnerHangingTest {

  private static final Logger LOG = LoggerFactory.getLogger(RepairRunnerHangingTest.class);
  private static final Set<String> TABLES = ImmutableSet.of("table1");
  private static final List<BigInteger> THREE_TOKENS = Lists.newArrayList(
      BigInteger.valueOf(0L),
      BigInteger.valueOf(100L),
      BigInteger.valueOf(200L));

  private final Cluster cluster = Cluster.builder()
      .withName("test_" + RandomStringUtils.randomAlphabetic(12))
      .withSeedHosts(ImmutableSet.of("127.0.0.1"))
      .withState(Cluster.State.ACTIVE)
      .build();
  private Map<String, String> replicas = ImmutableMap.of(
      "127.0.0.1", "dc1"
  );
  private Map<UUID, RepairRunner> currentRunners;

  @Before
  public void setUp() throws Exception {
    SegmentRunner.SEGMENT_RUNNERS.clear();
    currentRunners = new HashMap<>();
    // Create 3 runners for cluster1 and 1 for cluster2
    for (int i = 0; i < 3; i++) {
      RepairRunner runner = mock(RepairRunner.class);
      when(runner.getClusterName()).thenReturn("cluster1");
      when(runner.isRunning()).thenReturn(true);
      currentRunners.put(UUID.randomUUID(), runner);
    }

    RepairRunner runner = mock(RepairRunner.class);
    when(runner.getClusterName()).thenReturn("cluster2");
    when(runner.isRunning()).thenReturn(true);
    currentRunners.put(UUID.randomUUID(), runner);
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

  public static Map<List<String>, List<String>> scyllaThreeNodeClusterWithIps() {
    Map<List<String>, List<String>> map = new HashMap<List<String>, List<String>>();
    map = addRangeToMap(map, "", "0", "127.0.0.3", "127.0.0.1", "127.0.0.2");
    map = addRangeToMap(map, "0", "100", "127.0.0.1", "127.0.0.2", "127.0.0.3");
    map = addRangeToMap(map, "100", "200", "127.0.0.2", "127.0.0.3", "127.0.0.1");
    map = addRangeToMap(map, "200", "", "127.0.0.3", "127.0.0.1", "127.0.0.2");
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

  private Map<String, String> endpointToHostIDMap() {
    Map<String, String> endpointToHostIDMap = new HashMap<String, String>();
    endpointToHostIDMap.put("127.0.0.1", UUID.randomUUID().toString());
    endpointToHostIDMap.put("127.0.0.2", UUID.randomUUID().toString());
    endpointToHostIDMap.put("127.0.0.3", UUID.randomUUID().toString());

    return endpointToHostIDMap;
  }

  private RepairRun addNewRepairRun(
      final Map<String, String> nodeMap,
      final double intensity,
      final IStorageDao storage,
      UUID cf,
      UUID hostID
  ) {
    return storage.getRepairRunDao().addRepairRun(
        RepairRun.builder(cluster.getName(), cf)
            .intensity(intensity)
            .segmentCount(1)
            .repairParallelism(RepairParallelism.PARALLEL)
            .tables(TABLES),
        Lists.newArrayList(
            RepairSegment.builder(
                    Segment.builder()
                        .withTokenRange(new RingRange(BigInteger.ZERO, new BigInteger("100")))
                        .withReplicas(nodeMap)
                        .build(), cf)
                .withState(RepairSegment.State.RUNNING)
                .withStartTime(DateTime.now())
                .withCoordinatorHost("reaper")
                .withHostID(hostID),
            RepairSegment.builder(
                    Segment.builder()
                        .withTokenRange(new RingRange(new BigInteger("100"), new BigInteger("200")))
                        .withReplicas(nodeMap)
                        .build(), cf)
                .withHostID(hostID)
        )
    );
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

  @After
  public void tearDown() {
    DateTimeUtils.setCurrentMillisSystem();
  }

  @Test
  public void testHangingRepair() throws InterruptedException, ReaperException, JMException, IOException {
    final Set<String> cfNames = Sets.newHashSet("reaper");
    final Set<String> nodeSet = Sets.newHashSet("127.0.0.1");
    final Set<String> datacenters = Collections.emptySet();
    final Set<String> blacklistedTables = Collections.emptySet();
    final long timeRun = 41L;
    final double intensity = 0.5f;
    final int repairThreadCount = 1;
    final int segmentTimeout = 1;
    final IStorageDao storage = new MemoryStorageFacade();
    storage.getClusterDao().addCluster(cluster);
    RepairUnit cf = storage.getRepairUnitDao().addRepairUnit(
        RepairUnit.builder()
            .clusterName(cluster.getName())
            .keyspaceName("reaper")
            .columnFamilies(cfNames)
            .incrementalRepair(false)
            .subrangeIncrementalRepair(false)
            .nodes(nodeSet)
            .datacenters(datacenters)
            .blacklistedTables(blacklistedTables)
            .repairThreadCount(repairThreadCount)
            .timeout(segmentTimeout));
    DateTimeUtils.setCurrentMillisFixed(timeRun);
    RepairRun run = storage.getRepairRunDao().addRepairRun(
        RepairRun.builder(cluster.getName(), cf.getId())
            .intensity(intensity)
            .segmentCount(1)
            .repairParallelism(RepairParallelism.PARALLEL)
            .tables(TABLES),
        Collections.singleton(
            RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(new RingRange(BigInteger.ZERO, new BigInteger("100")))
                    .withReplicas(replicas)
                    .build(),
                cf.getId())));
    final UUID runId = run.getId();
    final UUID segmentId = storage.getRepairSegmentDao().getNextFreeSegments(run.getId()).get(0).getId();
    assertEquals(storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState(),
        RepairSegment.State.NOT_STARTED);
    AppContext context = new AppContext();
    context.storage = storage;
    context.config = new ReaperApplicationConfiguration();
    final Semaphore mutex = new Semaphore(0);
    final JmxCassandraManagementProxy jmx = CassandraManagementProxyTest.mockJmxProxyImpl();
    Map<String, String> endpointToHostIDMap = endpointToHostIDMap();
    when(jmx.getEndpointToHostId()).thenReturn(endpointToHostIDMap);
    when(jmx.getClusterName()).thenReturn(cluster.getName());
    when(jmx.getRangeToEndpointMap(anyString())).thenReturn(RepairRunnerHangingTest.sixNodeCluster());
    EndpointSnitchInfoMBean endpointSnitchInfoMBean = mock(EndpointSnitchInfoMBean.class);
    when(endpointSnitchInfoMBean.getDatacenter()).thenReturn("dc1");
    try {
      when(endpointSnitchInfoMBean.getDatacenter(anyString())).thenReturn("dc1");
    } catch (UnknownHostException ex) {
      throw new AssertionError(ex);
    }
    final AtomicInteger repairAttempts = new AtomicInteger(1);
    when(jmx.triggerRepair(any(), any(), any(), any(), any(), any(), any(), anyInt()))
        .then(
            (invocation) -> {
              assertEquals(RepairSegment.State.STARTED,
                  storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState());
              final int repairNumber = repairAttempts.getAndIncrement();
              switch (repairNumber) {
                case 1:
                  new Thread() {
                    @Override
                    public void run() {
                      ((RepairStatusHandler) invocation.getArgument(5))
                          .handle(repairNumber,
                              Optional.of(ActiveRepairService.Status.STARTED),
                              Optional.empty(), null, jmx);
                      assertEquals(
                          RepairSegment.State.RUNNING,
                          storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState());
                    }
                  }.start();
                  break;
                case 2:
                  new Thread() {
                    @Override
                    public void run() {
                      ((RepairStatusHandler) invocation.getArgument(5))
                          .handle(
                              repairNumber,
                              Optional.of(ActiveRepairService.Status.STARTED),
                              Optional.empty(), null, jmx);
                      assertEquals(
                          RepairSegment.State.RUNNING,
                          storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState());
                      ((RepairStatusHandler) invocation.getArgument(5))
                          .handle(
                              repairNumber,
                              Optional.of(ActiveRepairService.Status.SESSION_SUCCESS),
                              Optional.empty(), null, jmx);
                      assertEquals(
                          RepairSegment.State.DONE,
                          storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState());
                      ((RepairStatusHandler) invocation.getArgument(5))
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
    when(clusterFacade.nodeIsDirectlyAccessible(any(), any())).thenReturn(true);
    when(clusterFacade.tokenRangeToEndpoint(any(), anyString(), any())).thenReturn(Lists.newArrayList(nodeSet));
    when(clusterFacade.getEndpointToHostId(any())).thenReturn(endpointToHostIDMap);
    when(clusterFacade.listActiveCompactions(any())).thenReturn(CompactionStats.builder().withActiveCompactions(
        Collections.emptyList()).withPendingCompactions(Optional.of(0)).build());
    when(clusterFacade.getRangeToEndpointMap(any(), anyString()))
        .thenReturn((Map) ImmutableMap.of(Lists.newArrayList("0", "100"), Lists.newArrayList(nodeSet)));
    context.repairManager = RepairManager.create(
        context,
        clusterFacade,
        Executors.newScheduledThreadPool(1),
        1,
        TimeUnit.MILLISECONDS,
        1,
        context.storage.getRepairRunDao());
    context.managementConnectionFactory = new JmxManagementConnectionFactory(context, new NoopCrypotograph()) {
      @Override
      protected JmxCassandraManagementProxy connectImpl(Node host) throws ReaperException {
        return jmx;
      }
    };
    context.repairManager.startRepairRun(run);
    await().with().atMost(2, TimeUnit.MINUTES).until(() -> {
      try {
        mutex.acquire();
        Thread.sleep(1000);
        return true;
      } catch (InterruptedException ex) {
        throw new IllegalStateException(ex);
      }
    });
    assertEquals(RepairRun.RunState.DONE, storage.getRepairRunDao().getRepairRun(runId).get().getRunState());
  }

  @Test
  public void testHangingRepairNewApi() throws InterruptedException, ReaperException, MalformedObjectNameException,
      ReflectionException, IOException {
    final String ksName = "reaper";
    final Set<String> cfNames = Sets.newHashSet("reaper");
    final boolean incrementalRepair = false;
    final Set<String> nodeSet = Sets.newHashSet("127.0.0.1");
    final Set<String> datacenters = Collections.emptySet();
    final Set<String> blacklistedTables = Collections.emptySet();
    final long timeRun = 41L;
    final double intensity = 0.5f;
    final int repairThreadCount = 1;
    final int segmentTimeout = 1;
    final IStorageDao storage = new MemoryStorageFacade();
    storage.getClusterDao().addCluster(cluster);
    DateTimeUtils.setCurrentMillisFixed(timeRun);
    RepairUnit cf = storage.getRepairUnitDao().addRepairUnit(
        RepairUnit.builder()
            .clusterName(cluster.getName())
            .keyspaceName(ksName)
            .columnFamilies(cfNames)
            .incrementalRepair(incrementalRepair)
            .subrangeIncrementalRepair(incrementalRepair)
            .nodes(nodeSet)
            .datacenters(datacenters)
            .blacklistedTables(blacklistedTables)
            .repairThreadCount(repairThreadCount)
            .timeout(segmentTimeout));
    RepairRun run = storage.getRepairRunDao().addRepairRun(
        RepairRun.builder(cluster.getName(), cf.getId())
            .intensity(intensity).segmentCount(1)
            .repairParallelism(RepairParallelism.PARALLEL)
            .tables(TABLES),
        Collections.singleton(
            RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(new RingRange(BigInteger.ZERO, new BigInteger("100")))
                    .withReplicas(replicas)
                    .build(),
                cf.getId())));
    final UUID runId = run.getId();
    final UUID segmentId = storage.getRepairSegmentDao().getNextFreeSegments(run.getId()).get(0).getId();
    assertEquals(storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState(),
        RepairSegment.State.NOT_STARTED);
    AppContext context = new AppContext();
    context.storage = storage;
    context.config = new ReaperApplicationConfiguration();
    final Semaphore mutex = new Semaphore(0);
    final JmxCassandraManagementProxy jmx = CassandraManagementProxyTest.mockJmxProxyImpl();
    when(jmx.getClusterName()).thenReturn(cluster.getName());
    when(jmx.getRangeToEndpointMap(anyString())).thenReturn(RepairRunnerHangingTest.sixNodeCluster());
    EndpointSnitchInfoMBean endpointSnitchInfoMBean = mock(EndpointSnitchInfoMBean.class);
    when(endpointSnitchInfoMBean.getDatacenter()).thenReturn("dc1");
    try {
      when(endpointSnitchInfoMBean.getDatacenter(anyString())).thenReturn("dc1");
    } catch (UnknownHostException ex) {
      throw new AssertionError(ex);
    }
    final AtomicInteger repairAttempts = new AtomicInteger(1);
    when(jmx.triggerRepair(any(), any(), any(), any(), any(), any(), any(), anyInt()))
        .then(
            (invocation) -> {
              assertEquals(
                  RepairSegment.State.STARTED,
                  storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState());
              final int repairNumber = repairAttempts.getAndIncrement();
              switch (repairNumber) {
                case 1:
                  new Thread() {
                    @Override
                    public void run() {
                      ((RepairStatusHandler) invocation.getArgument(5))
                          .handle(
                              repairNumber, Optional.empty(),
                              Optional.of(ProgressEventType.START), null, jmx);
                      assertEquals(
                          RepairSegment.State.RUNNING,
                          storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState());
                    }
                  }.start();
                  break;
                case 2:
                  new Thread() {
                    @Override
                    public void run() {
                      ((RepairStatusHandler) invocation.getArgument(5))
                          .handle(
                              repairNumber, Optional.empty(),
                              Optional.of(ProgressEventType.START), null, jmx);
                      assertEquals(
                          RepairSegment.State.RUNNING,
                          storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState());
                      ((RepairStatusHandler) invocation.getArgument(5))
                          .handle(
                              repairNumber, Optional.empty(),
                              Optional.of(ProgressEventType.SUCCESS), null, jmx);
                      assertEquals(
                          RepairSegment.State.DONE,
                          storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState());
                      ((RepairStatusHandler) invocation.getArgument(5))
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
    when(clusterFacade.nodeIsDirectlyAccessible(any(), any())).thenReturn(true);
    when(clusterFacade.tokenRangeToEndpoint(any(), anyString(), any()))
        .thenReturn(Lists.newArrayList(nodeSet));
    when(clusterFacade.getRangeToEndpointMap(any(), anyString()))
        .thenReturn((Map) ImmutableMap.of(Lists.newArrayList("0", "100"), Lists.newArrayList(nodeSet)));
    when(clusterFacade.listActiveCompactions(any())).thenReturn(CompactionStats.builder().withActiveCompactions(
        Collections.emptyList()).withPendingCompactions(Optional.of(0)).build());
    context.repairManager
        = RepairManager.create(
        context,
        clusterFacade,
        Executors.newScheduledThreadPool(1),
        1,
        TimeUnit.MILLISECONDS,
        1,
        context.storage.getRepairRunDao());
    context.managementConnectionFactory = new JmxManagementConnectionFactory(context, new NoopCrypotograph()) {
      @Override
      protected JmxCassandraManagementProxy connectImpl(Node host) throws ReaperException {
        return jmx;
      }
    };
    context.repairManager.startRepairRun(run);
    await().with().atMost(2, TimeUnit.MINUTES).until(() -> {
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
    assertEquals(RepairRun.RunState.DONE, storage.getRepairRunDao().getRepairRun(runId).get().getRunState());
  }

  @Test
  public void testDontFailRepairAfterTopologyChangeIncrementalRepair() throws InterruptedException, ReaperException,
      MalformedObjectNameException, ReflectionException, IOException {
    final String ksName = "reaper";
    final Set<String> cfNames = Sets.newHashSet("reaper");
    final boolean incrementalRepair = true;
    final Set<String> nodeSet = Sets.newHashSet("127.0.0.1", "127.0.0.2", "127.0.0.3");
    final List<String> nodeSetAfterTopologyChange = Lists.newArrayList("127.0.0.3", "127.0.0.2", "127.0.0.4");
    final Map<String, String> nodeMap = ImmutableMap.of("127.0.0.1", "dc1", "127.0.0.2", "dc1", "127.0.0.3", "dc1");
    final Map<String, String> nodeMapAfterTopologyChange = ImmutableMap.of(
        "127.0.0.3", "dc1", "127.0.0.2", "dc1", "127.0.0.4", "dc1");
    final Set<String> datacenters = Collections.emptySet();
    final Set<String> blacklistedTables = Collections.emptySet();
    final double intensity = 0.5f;
    final int repairThreadCount = 1;
    final int segmentTimeout = 30;
    final List<BigInteger> tokens = THREE_TOKENS;
    final IStorageDao storage = new MemoryStorageFacade();
    AppContext context = new AppContext();
    context.storage = storage;
    context.config = new ReaperApplicationConfiguration();
    storage.getClusterDao().addCluster(cluster);
    UUID cf = storage.getRepairUnitDao().addRepairUnit(
            RepairUnit.builder()
                .clusterName(cluster.getName())
                .keyspaceName(ksName)
                .columnFamilies(cfNames)
                .incrementalRepair(incrementalRepair)
                .subrangeIncrementalRepair(false)
                .nodes(nodeSet)
                .datacenters(datacenters)
                .blacklistedTables(blacklistedTables)
                .repairThreadCount(repairThreadCount)
                .timeout(segmentTimeout))
        .getId();
    Map<String, String> endpointToHostIDMap = endpointToHostIDMap();
    RepairRun run = addNewRepairRun(nodeMap,
        intensity,
        storage,
        cf,
        UUID.fromString(endpointToHostIDMap.get("127.0.0.1")));
    final UUID runId = run.getId();
    final UUID segmentId = storage.getRepairSegmentDao().getNextFreeSegments(run.getId()).get(0).getId();
    assertEquals(storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState(),
        RepairSegment.State.NOT_STARTED);
    final JmxCassandraManagementProxy jmx = CassandraManagementProxyTest.mockJmxProxyImpl();
    when(jmx.getClusterName()).thenReturn(cluster.getName());
    when(jmx.getRangeToEndpointMap(anyString())).thenReturn(RepairRunnerTest.threeNodeClusterWithIps());
    when(jmx.getEndpointToHostId()).thenReturn(endpointToHostIDMap);
    when(jmx.getTokens()).thenReturn(tokens);
    EndpointSnitchInfoMBean endpointSnitchInfoMBean = mock(EndpointSnitchInfoMBean.class);
    when(endpointSnitchInfoMBean.getDatacenter()).thenReturn("dc1");
    try {
      when(endpointSnitchInfoMBean.getDatacenter(anyString())).thenReturn("dc1");
    } catch (UnknownHostException ex) {
      throw new AssertionError(ex);
    }

    ClusterFacade clusterFacade = mock(ClusterFacade.class);
    when(clusterFacade.connect(any(Cluster.class), any())).thenReturn(jmx);
    when(clusterFacade.nodeIsDirectlyAccessible(any(), any())).thenReturn(true);
    when(clusterFacade.tokenRangeToEndpoint(any(), anyString(), any()))
        .thenReturn(Lists.newArrayList(nodeSet));
    when(clusterFacade.getRangeToEndpointMap(any(), anyString()))
        .thenReturn((Map) ImmutableMap.of(
            Lists.newArrayList("0", "100"), Lists.newArrayList(nodeSet),
            Lists.newArrayList("100", "200"), Lists.newArrayList(nodeSet)));
    when(clusterFacade.getEndpointToHostId(any())).thenReturn(endpointToHostIDMap);
    when(clusterFacade.listActiveCompactions(any())).thenReturn(
        CompactionStats.builder()
            .withActiveCompactions(Collections.emptyList())
            .withPendingCompactions(Optional.of(0))
            .build());
    context.repairManager = RepairManager.create(
        context,
        clusterFacade,
        Executors.newScheduledThreadPool(10),
        1,
        TimeUnit.MILLISECONDS,
        1,
        context.storage.getRepairRunDao());
    AtomicInteger repairNumberCounter = new AtomicInteger(1);
    when(jmx.triggerRepair(any(), any(), any(), any(), any(), any(), any(), anyInt()))
        .then(
            (invocation) -> {
              final int repairNumber = repairNumberCounter.getAndIncrement();
              new Thread() {
                @Override
                public void run() {
                  ((RepairStatusHandler) invocation.getArgument(5))
                      .handle(
                          repairNumber,
                          Optional.of(ActiveRepairService.Status.STARTED),
                          Optional.empty(),
                          null,
                          jmx);
                  ((RepairStatusHandler) invocation.getArgument(5))
                      .handle(
                          repairNumber,
                          Optional.of(ActiveRepairService.Status.SESSION_SUCCESS),
                          Optional.empty(),
                          null,
                          jmx);
                  ((RepairStatusHandler) invocation.getArgument(5))
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
    context.managementConnectionFactory = new JmxManagementConnectionFactory(context, new NoopCrypotograph()) {
      @Override
      protected JmxCassandraManagementProxy connectImpl(Node host) throws ReaperException {
        return jmx;
      }
    };
    ClusterFacade clusterProxy = ClusterFacade.create(context);
    ClusterFacade clusterProxySpy = Mockito.spy(clusterProxy);
    Mockito.doReturn(nodeSetAfterTopologyChange).when(clusterProxySpy).tokenRangeToEndpoint(any(), any(), any());
    assertEquals(RepairRun.RunState.NOT_STARTED, storage.getRepairRunDao().getRepairRun(runId).get().getRunState());
    storage.getRepairRunDao().updateRepairRun(
        run.with().runState(RepairRun.RunState.RUNNING).startTime(DateTime.now()).build(runId));
    // We'll now change the list of replicas for any segment, making the stored ones obsolete
    when(clusterFacade.getRangeToEndpointMap(any(), anyString()))
        .thenReturn((Map) ImmutableMap.of(
            Lists.newArrayList("0", "100"), Lists.newArrayList(nodeSetAfterTopologyChange),
            Lists.newArrayList("100", "200"), Lists.newArrayList(nodeSetAfterTopologyChange)));
    String hostIdToChange = endpointToHostIDMap.get("127.0.0.1");
    endpointToHostIDMap.remove("127.0.0.1");
    endpointToHostIDMap.put("127.0.0.4", hostIdToChange);
    when(clusterFacade.getEndpointToHostId(any())).thenReturn(endpointToHostIDMap);
    when(clusterFacade.tokenRangeToEndpoint(any(), anyString(), any()))
        .thenReturn(Lists.newArrayList(nodeSetAfterTopologyChange));
    context.repairManager.resumeRunningRepairRuns();

    // The repair run should succeed despite the topology change.
    await().with().atMost(60, TimeUnit.SECONDS).until(() -> {
      return RepairRun.RunState.DONE == storage.getRepairRunDao().getRepairRun(runId).get().getRunState();
    });
  }
}