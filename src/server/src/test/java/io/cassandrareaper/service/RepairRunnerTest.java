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
import io.cassandrareaper.core.RepairSchedule;
import io.cassandrareaper.core.RepairSegment;
import io.cassandrareaper.core.RepairType;
import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.core.Segment;
import io.cassandrareaper.crypto.NoopCrypotograph;
import io.cassandrareaper.management.ClusterFacade;
import io.cassandrareaper.management.RepairStatusHandler;
import io.cassandrareaper.management.jmx.CassandraManagementProxyTest;
import io.cassandrareaper.management.jmx.JmxCassandraManagementProxy;
import io.cassandrareaper.management.jmx.JmxManagementConnectionFactory;
import io.cassandrareaper.storage.IDistributedStorage;
import io.cassandrareaper.storage.IStorageDao;
import io.cassandrareaper.storage.MemoryStorageFacade;
import io.cassandrareaper.storage.cassandra.CassandraStorageFacade;
import io.cassandrareaper.storage.cluster.IClusterDao;
import io.cassandrareaper.storage.repairrun.IRepairRunDao;
import io.cassandrareaper.storage.repairschedule.IRepairScheduleDao;
import io.cassandrareaper.storage.repairsegment.IRepairSegmentDao;
import io.cassandrareaper.storage.repairunit.IRepairUnitDao;

import java.io.IOException;
import java.math.BigInteger;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.management.MalformedObjectNameException;
import javax.management.ReflectionException;

import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.awaitility.Duration;
import org.awaitility.core.ConditionTimeoutException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public final class RepairRunnerTest {
  private static final long LEAD_TIME = 1L;
  private static final Set<String> TABLES = ImmutableSet.of("table1");
  private static final Duration POLL_INTERVAL = Duration.TWO_SECONDS;
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
  public void testResumeRepair() throws InterruptedException, ReaperException, MalformedObjectNameException,
      ReflectionException, IOException {
    final String ksName = "reaper";
    final Set<String> cfNames = Sets.newHashSet("reaper");
    final boolean incrementalRepair = false;
    final Set<String> nodeSet = Sets.newHashSet("127.0.0.1");
    final Map<String, String> nodeMap = ImmutableMap.of(
        "127.0.0.1", "dc1"
    );
    final Set<String> datacenters = Collections.emptySet();
    final Set<String> blacklistedTables = Collections.emptySet();
    final long timeRun = 41L;
    final double intensity = 0.5f;
    final int repairThreadCount = 1;
    final int segmentTimeout = 30;
    final List<BigInteger> tokens = THREE_TOKENS;
    final IStorageDao storage = new MemoryStorageFacade(LEAD_TIME);
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
                .subrangeIncrementalRepair(incrementalRepair)
                .nodes(nodeSet)
                .datacenters(datacenters)
                .blacklistedTables(blacklistedTables)
                .repairThreadCount(repairThreadCount)
                .timeout(segmentTimeout))
        .getId();
    DateTimeUtils.setCurrentMillisFixed(timeRun);
    RepairRun run = generateRepairRunForPendingCompactions(intensity, storage, cf);
    final UUID runId = run.getId();
    final UUID segmentId = storage.getRepairSegmentDao().getNextFreeSegments(run.getId()).get(0).getId();
    assertEquals(storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState(),
        RepairSegment.State.NOT_STARTED);
    final JmxCassandraManagementProxy jmx = CassandraManagementProxyTest.mockJmxProxyImpl();
    when(jmx.getClusterName()).thenReturn(cluster.getName());
    when(jmx.getRangeToEndpointMap(anyString())).thenReturn(RepairRunnerTest.threeNodeClusterWithIps());
    when(jmx.getEndpointToHostId()).thenReturn(nodeMap);
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
    when(clusterFacade.listActiveCompactions(any())).thenReturn(CompactionStats.builder().withActiveCompactions(
        Collections.emptyList()).withPendingCompactions(Optional.of(0)).build());
    when(clusterFacade.getEndpointToHostId(any())).thenReturn(nodeMap);

    context.repairManager = RepairManager.create(
        context,
        clusterFacade,
        Executors.newScheduledThreadPool(1),
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
    Mockito.doReturn(Collections.singletonList("")).when(clusterProxySpy).tokenRangeToEndpoint(any(), any(), any());

    assertEquals(RepairRun.RunState.NOT_STARTED, storage.getRepairRunDao().getRepairRun(runId).get().getRunState());
    context.repairManager.resumeRunningRepairRuns();
    assertEquals(RepairRun.RunState.NOT_STARTED, storage.getRepairRunDao().getRepairRun(runId).get().getRunState());

    storage.getRepairRunDao().updateRepairRun(
        run.with().runState(RepairRun.RunState.RUNNING).startTime(DateTime.now()).build(runId));

    context.repairManager.resumeRunningRepairRuns();

    await().with().pollInterval(POLL_INTERVAL).atMost(30, SECONDS).until(() -> {
      return RepairRun.RunState.DONE == storage.getRepairRunDao().getRepairRun(runId).get().getRunState();
    });
  }

  @Test(expected = ConditionTimeoutException.class)
  public void testTooManyPendingCompactions()
      throws InterruptedException, ReaperException, MalformedObjectNameException,
      ReflectionException, IOException {
    final String ksName = "reaper";
    final Set<String> cfNames = Sets.newHashSet("reaper");
    final boolean incrementalRepair = false;
    final Set<String> nodeSet = Sets.newHashSet("127.0.0.1");
    final Map<String, String> nodeMap = ImmutableMap.of("127.0.0.1", "dc1");
    final Set<String> datacenters = Collections.emptySet();
    final Set<String> blacklistedTables = Collections.emptySet();
    final long timeRun = 41L;
    final double intensity = 0.5f;
    final int repairThreadCount = 1;
    final int segmentTimeout = 30;
    final List<BigInteger> tokens = THREE_TOKENS;
    final IStorageDao storage = new MemoryStorageFacade(LEAD_TIME);
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
                .subrangeIncrementalRepair(incrementalRepair)
                .nodes(nodeSet)
                .datacenters(datacenters)
                .blacklistedTables(blacklistedTables)
                .repairThreadCount(repairThreadCount)
                .timeout(segmentTimeout))
        .getId();
    DateTimeUtils.setCurrentMillisFixed(timeRun);
    RepairRun run = generateRepairRunForPendingCompactions(intensity, storage, cf);
    final UUID runId = run.getId();
    final UUID segmentId = storage.getRepairSegmentDao().getNextFreeSegments(run.getId()).get(0).getId();
    assertEquals(storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState(),
        RepairSegment.State.NOT_STARTED);
    final JmxCassandraManagementProxy jmx = CassandraManagementProxyTest.mockJmxProxyImpl();
    when(jmx.getClusterName()).thenReturn(cluster.getName());
    when(jmx.getRangeToEndpointMap(anyString())).thenReturn(RepairRunnerTest.threeNodeClusterWithIps());
    when(jmx.getEndpointToHostId()).thenReturn(nodeMap);
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
    when(clusterFacade.listActiveCompactions(any())).thenReturn(CompactionStats.builder().withActiveCompactions(
        Collections.emptyList()).withPendingCompactions(Optional.of(100)).build());

    context.repairManager = RepairManager.create(
        context,
        clusterFacade,
        Executors.newScheduledThreadPool(1),
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
    Mockito.doReturn(Collections.singletonList("")).when(clusterProxySpy).tokenRangeToEndpoint(any(), any(), any());

    assertEquals(RepairRun.RunState.NOT_STARTED, storage.getRepairRunDao().getRepairRun(runId).get().getRunState());
    context.repairManager.resumeRunningRepairRuns();
    assertEquals(RepairRun.RunState.NOT_STARTED, storage.getRepairRunDao().getRepairRun(runId).get().getRunState());

    storage.getRepairRunDao().updateRepairRun(
        run.with().runState(RepairRun.RunState.RUNNING).startTime(DateTime.now()).build(runId));

    context.repairManager.resumeRunningRepairRuns();
    // Make sure it's still in RUNNING state as segments can't be processed due to pending compactions
    await().with().pollInterval(POLL_INTERVAL).atMost(30, SECONDS).until(() -> {
      if (RepairRun.RunState.RUNNING != storage.getRepairRunDao().getRepairRun(runId).get().getRunState()) {
        return true;
      }
      return false;
    });
    // If we get here then there's a problem...
    // An exception should be thrown by Awaitility as we should not reach a status different than RUNNING
    assertEquals(RepairRun.RunState.RUNNING, storage.getRepairRunDao().getRepairRun(runId).get().getRunState());
  }

  private RepairRun generateRepairRunForPendingCompactions(final double intensity, final IStorageDao storage, UUID cf) {
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
                        .withReplicas(replicas)
                        .build(),
                    cf)
                .withState(RepairSegment.State.RUNNING)
                .withStartTime(DateTime.now())
                .withCoordinatorHost("reaper"),
            RepairSegment.builder(
                Segment.builder()
                    .withTokenRange(new RingRange(new BigInteger("100"), new BigInteger("200")))
                    .withReplicas(replicas)
                    .build(),
                cf)));
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

  @Test
  public void testDontFailRepairAfterTopologyChange() throws InterruptedException, ReaperException,
      MalformedObjectNameException, ReflectionException, IOException {
    final String ksName = "reaper";
    final Set<String> cfNames = Sets.newHashSet("reaper");
    final boolean incrementalRepair = false;
    final Set<String> nodeSet = Sets.newHashSet("127.0.0.1", "127.0.0.2", "127.0.0.3");
    final List<String> nodeSetAfterTopologyChange = Lists.newArrayList("127.0.0.1", "127.0.0.2", "127.0.0.4");
    final Map<String, String> nodeMap = ImmutableMap.of("127.0.0.1", "dc1", "127.0.0.2", "dc1", "127.0.0.3", "dc1");
    final Map<String, String> nodeMapAfterTopologyChange = ImmutableMap.of(
        "127.0.0.1", "dc1", "127.0.0.2", "dc1", "127.0.0.4", "dc1");
    final Set<String> datacenters = Collections.emptySet();
    final Set<String> blacklistedTables = Collections.emptySet();
    final double intensity = 0.5f;
    final int repairThreadCount = 1;
    final int segmentTimeout = 30;
    final List<BigInteger> tokens = THREE_TOKENS;
    final IStorageDao storage = new MemoryStorageFacade(LEAD_TIME);
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
                .subrangeIncrementalRepair(incrementalRepair)
                .nodes(nodeSet)
                .datacenters(datacenters)
                .blacklistedTables(blacklistedTables)
                .repairThreadCount(repairThreadCount)
                .timeout(segmentTimeout))
        .getId();
    final Map<String, String> endpointToHostIDMap = endpointToHostIDMap();
    RepairRun run = addNewRepairRun(nodeMap,
        intensity,
        storage,
        cf,
        null,
        1);
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
    when(clusterFacade.getEndpointToHostId(any())).thenReturn(nodeMap);
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
    when(clusterFacade.getEndpointToHostId(any())).thenReturn(nodeMapAfterTopologyChange);
    when(clusterFacade.tokenRangeToEndpoint(any(), anyString(), any()))
        .thenReturn(Lists.newArrayList(nodeSetAfterTopologyChange));
    context.repairManager.resumeRunningRepairRuns();

    // The repair run should succeed despite the topology change.
    await().with().atMost(60, TimeUnit.SECONDS).until(() -> {
      return RepairRun.RunState.DONE == storage.getRepairRunDao().getRepairRun(runId).get().getRunState();
    });
  }

  @Test
  public void testSubrangeIncrementalRepair() throws InterruptedException, ReaperException,
      MalformedObjectNameException, ReflectionException, IOException {
    final String ksName = "reaper";
    final Set<String> cfNames = Sets.newHashSet("reaper");
    final boolean incrementalRepair = true;
    final boolean subrangeIncrementalRepair = true;
    final Set<String> nodeSet = Sets.newHashSet("127.0.0.1", "127.0.0.2", "127.0.0.3");
    final Map<String, String> nodeMap = ImmutableMap.of("127.0.0.1", "dc1", "127.0.0.2", "dc1", "127.0.0.3", "dc1");
    final Set<String> datacenters = Collections.emptySet();
    final Set<String> blacklistedTables = Collections.emptySet();
    final double intensity = 0.5f;
    final int repairThreadCount = 1;
    final int segmentTimeout = 30;
    final List<BigInteger> tokens = THREE_TOKENS;
    final IStorageDao storage = new MemoryStorageFacade(LEAD_TIME);
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
                .subrangeIncrementalRepair(subrangeIncrementalRepair)
                .nodes(nodeSet)
                .datacenters(datacenters)
                .blacklistedTables(blacklistedTables)
                .repairThreadCount(repairThreadCount)
                .timeout(segmentTimeout))
        .getId();
    final Map<String, String> endpointToHostIDMap = endpointToHostIDMap();
    RepairRun run = addNewRepairRun(nodeMap,
        intensity,
        storage,
        cf,
        null,
        30);
    final UUID runId = run.getId();
    assertTrue(storage.getRepairRunDao().getRepairRun(runId).get().getSegmentCount() > 3);
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
    when(clusterFacade.getEndpointToHostId(any())).thenReturn(nodeMap);
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
    // triggerRepair is only configured for incremental runs and will fail if full repair is requested
    when(jmx.triggerRepair(any(), any(), any(), eq(RepairType.INCREMENTAL), any(), any(), any(), anyInt()))
        .thenThrow(new RuntimeException());
    when(jmx.triggerRepair(any(), any(), any(), eq(RepairType.SUBRANGE_FULL), any(), any(), any(), anyInt()))
        .thenThrow(new RuntimeException());
    when(jmx.triggerRepair(any(), any(), any(), eq(RepairType.SUBRANGE_INCREMENTAL), any(), any(), any(), anyInt()))
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
    assertEquals(RepairRun.RunState.NOT_STARTED, storage.getRepairRunDao().getRepairRun(runId).get().getRunState());
    storage.getRepairRunDao().updateRepairRun(
        run.with().runState(RepairRun.RunState.RUNNING).startTime(DateTime.now()).build(runId));
    context.repairManager.resumeRunningRepairRuns();

    await().with().atMost(20, TimeUnit.SECONDS).until(() -> {
      return RepairRun.RunState.DONE == storage.getRepairRunDao().getRepairRun(runId).get().getRunState();
    });
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
      UUID hostID,
      int segmentCount
  ) {
    return storage.getRepairRunDao().addRepairRun(
        RepairRun.builder(cluster.getName(), cf)
            .intensity(intensity)
            .segmentCount(segmentCount)
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

  @Test
  public void isItOkToRepairTest() {
    assertFalse(RepairRunner.okToRepairSegment(false, true, DatacenterAvailability.ALL));
    assertFalse(RepairRunner.okToRepairSegment(false, false, DatacenterAvailability.ALL));
    assertTrue(RepairRunner.okToRepairSegment(true, true, DatacenterAvailability.ALL));

    assertTrue(RepairRunner.okToRepairSegment(false, true, DatacenterAvailability.LOCAL));
    assertFalse(RepairRunner.okToRepairSegment(false, false, DatacenterAvailability.LOCAL));
    assertTrue(RepairRunner.okToRepairSegment(true, true, DatacenterAvailability.LOCAL));

    assertFalse(RepairRunner.okToRepairSegment(false, true, DatacenterAvailability.EACH));
    assertFalse(RepairRunner.okToRepairSegment(false, false, DatacenterAvailability.EACH));
    assertTrue(RepairRunner.okToRepairSegment(true, true, DatacenterAvailability.EACH));
  }

  @Test
  public void getNodeMetricsInLocalDcAvailabilityForRemoteDcNodeTest() throws Exception {
    final String ksName = "reaper";
    final Set<String> cfNames = Sets.newHashSet("reaper");
    final boolean incrementalRepair = false;
    final boolean subrangeIncremental = false;
    final Set<String> nodeSet = Sets.newHashSet("127.0.0.1", "127.0.0.2", "127.0.0.3");
    final List<String> nodeSetAfterTopologyChange = Lists.newArrayList("127.0.0.1", "127.0.0.2", "127.0.0.4");
    final Map<String, String> nodeMap = ImmutableMap.of(
        "127.0.0.1", "dc1", "127.0.0.2", "dc1", "127.0.0.3", "dc1"
    );
    final Set<String> datacenters = Collections.emptySet();
    final Set<String> blacklistedTables = Collections.emptySet();
    final double intensity = 0.5f;
    final int repairThreadCount = 1;
    final int segmentTimeout = 30;
    final List<BigInteger> tokens = THREE_TOKENS;
    final AppContext context = new AppContext();
    context.storage = Mockito.mock(CassandraStorageFacade.class);
    RepairUnit repairUnit = RepairUnit.builder()
        .clusterName(cluster.getName())
        .keyspaceName(ksName)
        .columnFamilies(cfNames)
        .incrementalRepair(incrementalRepair)
        .subrangeIncrementalRepair(subrangeIncremental)
        .nodes(nodeSet)
        .datacenters(datacenters)
        .blacklistedTables(blacklistedTables)
        .repairThreadCount(repairThreadCount)
        .timeout(segmentTimeout)
        .build(UUID.randomUUID());

    RepairRun run = RepairRun.builder(cluster.getName(), repairUnit.getId())
        .intensity(intensity)
        .segmentCount(1)
        .repairParallelism(RepairParallelism.PARALLEL)
        .tables(TABLES).build(UUID.randomUUID());

    IRepairRunDao mockedRepairRunDao = mock(IRepairRunDao.class);
    Mockito.when(mockedRepairRunDao.getRepairRun(any())).thenReturn(Optional.of(run));

    Mockito.when(((IDistributedStorage) context.storage).countRunningReapers()).thenReturn(1);
    Mockito.when(((CassandraStorageFacade) context.storage).getRepairRunDao()).thenReturn(mockedRepairRunDao);

    IRepairUnitDao mockedRepairUnitDao = mock(IRepairUnitDao.class);
    Mockito.when(((CassandraStorageFacade) context.storage).getRepairUnitDao()).thenReturn(mockedRepairUnitDao);
    Mockito.when(mockedRepairUnitDao.getRepairUnit(any(UUID.class))).thenReturn(repairUnit);
    IClusterDao mockedClusterDao = Mockito.mock(IClusterDao.class);
    Mockito.when(context.storage.getClusterDao()).thenReturn(mockedClusterDao);
    Mockito.when(mockedClusterDao.getCluster(any())).thenReturn(cluster);
    JmxManagementConnectionFactory jmxManagementConnectionFactory = mock(JmxManagementConnectionFactory.class);
    JmxCassandraManagementProxy jmx = mock(JmxCassandraManagementProxy.class);
    when(jmxManagementConnectionFactory.connectAny(any(Collection.class))).thenReturn(jmx);
    when(jmx.getClusterName()).thenReturn(cluster.getName());
    when(jmx.isConnectionAlive()).thenReturn(true);
    when(jmx.getRangeToEndpointMap(anyString())).thenReturn(RepairRunnerTest.threeNodeClusterWithIps());
    when(jmx.getEndpointToHostId()).thenReturn(nodeMap);
    when(jmx.getTokens()).thenReturn(tokens);
    when(jmx.isRepairRunning()).thenReturn(true);
    when(jmx.getPendingCompactions()).thenReturn(3);
    context.managementConnectionFactory = jmxManagementConnectionFactory;
    context.config = new ReaperApplicationConfiguration();
    context.config.setDatacenterAvailability(DatacenterAvailability.LOCAL);

    ClusterFacade clusterFacade = mock(ClusterFacade.class);
    when(clusterFacade.connect(any(Cluster.class), any())).thenReturn(jmx);
    when(clusterFacade.listActiveCompactions(any())).thenReturn(null);
    when(clusterFacade.getRangeToEndpointMap(any(), anyString()))
        .thenReturn((Map) ImmutableMap.of(Lists.newArrayList("0", "100"), Lists.newArrayList(nodeSet)));

    RepairRunner repairRunner = RepairRunner.create(
        context,
        UUID.randomUUID(),
        clusterFacade,
        context.storage.getRepairRunDao());

    Pair<String, Callable<Optional<CompactionStats>>> result = repairRunner.getNodeMetrics("node-some", "dc1", "dc2");
    assertFalse(result.getRight().call().isPresent());
    verify(jmxManagementConnectionFactory, times(0)).connectAny(any(Collection.class));
  }

  @Test
  public void getNodeMetricsInLocalDcAvailabilityForLocalDcNodeTest() throws Exception {
    final String ksName = "reaper";
    final Set<String> cfNames = Sets.newHashSet("reaper");
    final boolean incrementalRepair = false;
    final Set<String> nodeSet = Sets.newHashSet("127.0.0.1", "127.0.0.2", "127.0.0.3");
    final Map<String, String> nodeMap = ImmutableMap.of(
        "127.0.0.1", "dc1", "127.0.0.2", "dc1", "127.0.0.3", "dc1"
    );
    final Set<String> datacenters = Collections.emptySet();
    final Set<String> blacklistedTables = Collections.emptySet();
    final long timeRun = 41L;
    final double intensity = 0.5f;
    final int repairThreadCount = 1;
    final int segmentTimeout = 30;
    final List<BigInteger> tokens = THREE_TOKENS;
    final IStorageDao storage = new MemoryStorageFacade(LEAD_TIME);
    AppContext context = new AppContext();
    context.storage = storage;
    context.config = new ReaperApplicationConfiguration();
    storage.getClusterDao().addCluster(cluster);
    final UUID cf = storage.getRepairUnitDao().addRepairUnit(
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
                .timeout(segmentTimeout))
        .getId();
    DateTimeUtils.setCurrentMillisFixed(timeRun);
    final Map<String, String> endpointToHostIDMap = endpointToHostIDMap();
    RepairRun run = addNewRepairRun(nodeMap,
        intensity,
        storage,
        cf,
        null,
        1
    );
    final UUID runId = run.getId();
    final UUID segmentId = storage.getRepairSegmentDao().getNextFreeSegments(run.getId()).get(0).getId();
    assertEquals(storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState(),
        RepairSegment.State.NOT_STARTED);
    final JmxCassandraManagementProxy proxy = CassandraManagementProxyTest.mockJmxProxyImpl();
    when(proxy.getClusterName()).thenReturn(cluster.getName());
    when(proxy.getRangeToEndpointMap(anyString())).thenReturn(RepairRunnerTest.threeNodeClusterWithIps());
    when(proxy.getEndpointToHostId()).thenReturn(endpointToHostIDMap);
    when(proxy.getTokens()).thenReturn(tokens);
    when(proxy.isRepairRunning()).thenReturn(true);
    when(proxy.getPendingCompactions()).thenReturn(3);

    EndpointSnitchInfoMBean endpointSnitchInfoMBeanMock = mock(EndpointSnitchInfoMBean.class);
    when(endpointSnitchInfoMBeanMock.getDatacenter(any())).thenReturn("dc1");

    JmxManagementConnectionFactory jmxManagementConnectionFactory = mock(JmxManagementConnectionFactory.class);
    when(jmxManagementConnectionFactory.connectAny(any(Collection.class))).thenReturn(proxy);
    when(jmxManagementConnectionFactory.getAccessibleDatacenters()).thenReturn(Sets.newHashSet("dc1"));
    context.managementConnectionFactory = jmxManagementConnectionFactory;
    context.config = new ReaperApplicationConfiguration();
    context.config.setDatacenterAvailability(DatacenterAvailability.LOCAL);

    ClusterFacade clusterFacade = mock(ClusterFacade.class);
    when(clusterFacade.connect(any(Cluster.class), any())).thenReturn(proxy);
    when(clusterFacade.nodeIsDirectlyAccessible(any(), any())).thenReturn(true);
    when(clusterFacade.listActiveCompactions(any())).thenReturn(CompactionStats.builder().withActiveCompactions(
        Collections.emptyList()).withPendingCompactions(Optional.of(3)).build());
    when(clusterFacade.getRangeToEndpointMap(any(), anyString()))
        .thenReturn((Map) ImmutableMap.of(Lists.newArrayList("0", "100"), Lists.newArrayList(nodeSet)));

    RepairRunner repairRunner = RepairRunner.create(
        context,
        runId,
        clusterFacade,
        context.storage.getRepairRunDao());

    Pair<String, Callable<Optional<CompactionStats>>> result = repairRunner.getNodeMetrics("node-some", "dc1", "dc1");
    Optional<CompactionStats> optional = result.getRight().call();
    assertTrue(optional.isPresent());
    CompactionStats metrics = optional.get();
    assertEquals(3, metrics.getPendingCompactions().get().intValue());
  }

  @Test
  public void adaptiveRepairReduceSegmentsTest() throws Exception {
    final String ksName = "reaper";
    final Set<String> cfNames = Sets.newHashSet("reaper");
    final boolean incrementalRepair = false;
    final boolean subrangeIncremental = false;
    final Set<String> nodeSet = Sets.newHashSet("127.0.0.1", "127.0.0.2", "127.0.0.3");
    final Map<String, String> nodeMap = ImmutableMap.of(
        "127.0.0.1", "dc1", "127.0.0.2", "dc1", "127.0.0.3", "dc1"
    );
    final Set<String> datacenters = Collections.emptySet();
    final Set<String> blacklistedTables = Collections.emptySet();
    final double intensity = 0.5f;
    final int repairThreadCount = 1;
    final int segmentTimeout = 30;
    final List<BigInteger> tokens = THREE_TOKENS;
    final AppContext context = new AppContext();
    context.storage = Mockito.mock(CassandraStorageFacade.class);
    RepairUnit repairUnit = RepairUnit.builder()
        .clusterName(cluster.getName())
        .keyspaceName(ksName)
        .columnFamilies(cfNames)
        .incrementalRepair(incrementalRepair)
        .subrangeIncrementalRepair(subrangeIncremental)
        .nodes(nodeSet)
        .datacenters(datacenters)
        .blacklistedTables(blacklistedTables)
        .repairThreadCount(repairThreadCount)
        .timeout(segmentTimeout)
        .build(UUID.randomUUID());

    RepairRun run = RepairRun.builder(cluster.getName(), repairUnit.getId())
        .intensity(intensity)
        .segmentCount(64)
        .repairParallelism(RepairParallelism.PARALLEL)
        .adaptiveSchedule(true)
        .tables(TABLES).build(UUID.randomUUID());

    RepairSchedule repairSchedule = RepairSchedule.builder(repairUnit.getId())
        .adaptive(true)
        .daysBetween(1)
        .intensity(1)
        .owner("reaper")
        .segmentCountPerNode(64)
        .state(RepairSchedule.State.ACTIVE)
        .nextActivation(DateTime.now())
        .repairParallelism(RepairParallelism.DATACENTER_AWARE)
        .build(UUID.randomUUID());
    Collection<RepairSchedule> schedules = Lists.newArrayList(repairSchedule);

    IRepairRunDao mockedRepairRun = mock(IRepairRunDao.class);
    Mockito.when(mockedRepairRun.getRepairRun(any())).thenReturn(Optional.of(run));
    IRepairSegmentDao mockedRepairSegmentDao = mock(IRepairSegmentDao.class);
    List<RepairSegment> segments = generateRepairSegments(10, 0, 1, repairUnit.getId());
    Mockito.when(mockedRepairSegmentDao.getSegmentsWithState(any(), any())).thenReturn(segments);
    Mockito.when(context.storage.getRepairSegmentDao()).thenReturn(mockedRepairSegmentDao);


    Mockito.when(((IDistributedStorage) context.storage).countRunningReapers()).thenReturn(1);
    Mockito.when(((CassandraStorageFacade) context.storage).getRepairRunDao()).thenReturn(mockedRepairRun);
    IRepairUnitDao mockedRepairUnitDao = mock(IRepairUnitDao.class);
    Mockito.when(((CassandraStorageFacade) context.storage).getRepairUnitDao()).thenReturn(mockedRepairUnitDao);
    Mockito.when(mockedRepairUnitDao.getRepairUnit(any(UUID.class))).thenReturn(repairUnit);

    IRepairScheduleDao mockedRepairScheduleDao = Mockito.mock(IRepairScheduleDao.class);
    Mockito.when(context.storage.getRepairScheduleDao()).thenReturn(mockedRepairScheduleDao);
    Mockito.when(mockedRepairScheduleDao.getRepairSchedulesForClusterAndKeyspace(any(), any()))
        .thenReturn(schedules);
    IClusterDao mockedClusterDao = Mockito.mock(IClusterDao.class);
    Mockito.when(context.storage.getClusterDao()).thenReturn(mockedClusterDao);
    Mockito.when(mockedClusterDao.getCluster(any())).thenReturn(cluster);
    JmxManagementConnectionFactory jmxManagementConnectionFactory = mock(JmxManagementConnectionFactory.class);
    JmxCassandraManagementProxy jmx = mock(JmxCassandraManagementProxy.class);
    when(jmxManagementConnectionFactory.connectAny(any(Collection.class))).thenReturn(jmx);
    when(jmx.getClusterName()).thenReturn(cluster.getName());
    when(jmx.isConnectionAlive()).thenReturn(true);
    when(jmx.getRangeToEndpointMap(anyString())).thenReturn(RepairRunnerTest.threeNodeClusterWithIps());
    when(jmx.getEndpointToHostId()).thenReturn(nodeMap);
    when(jmx.getTokens()).thenReturn(tokens);
    when(jmx.isRepairRunning()).thenReturn(true);
    when(jmx.getPendingCompactions()).thenReturn(3);
    context.managementConnectionFactory = jmxManagementConnectionFactory;
    context.config = new ReaperApplicationConfiguration();
    context.config.setDatacenterAvailability(DatacenterAvailability.LOCAL);

    ClusterFacade clusterFacade = mock(ClusterFacade.class);

    RepairRunner repairRunner = RepairRunner.create(
        context,
        UUID.randomUUID(),
        clusterFacade,
        context.storage.getRepairRunDao());

    RepairRunner repairRunnerSpy = Mockito.spy(repairRunner);

    repairRunnerSpy.maybeAdaptRepairSchedule();
    verify(repairRunnerSpy, times(1)).tuneAdaptiveRepair(anyDouble(), anyInt());
    verify(repairRunnerSpy, times(1)).reduceSegmentsPerNodeToScheduleForUnit();
  }

  @Test
  public void adaptiveRepairAddSegmentsTest() throws Exception {
    final String ksName = "reaper";
    final Set<String> cfNames = Sets.newHashSet("reaper");
    final boolean incrementalRepair = false;
    final boolean subrangeIncremental = false;
    final Set<String> nodeSet = Sets.newHashSet("127.0.0.1", "127.0.0.2", "127.0.0.3");
    final Map<String, String> nodeMap = ImmutableMap.of(
        "127.0.0.1", "dc1", "127.0.0.2", "dc1", "127.0.0.3", "dc1"
    );
    final Set<String> datacenters = Collections.emptySet();
    final Set<String> blacklistedTables = Collections.emptySet();
    final double intensity = 0.5f;
    final int repairThreadCount = 1;
    final int segmentTimeout = 30;
    final List<BigInteger> tokens = THREE_TOKENS;
    final AppContext context = new AppContext();
    context.storage = Mockito.mock(CassandraStorageFacade.class);
    RepairUnit repairUnit = RepairUnit.builder()
        .clusterName(cluster.getName())
        .keyspaceName(ksName)
        .columnFamilies(cfNames)
        .incrementalRepair(incrementalRepair)
        .subrangeIncrementalRepair(subrangeIncremental)
        .nodes(nodeSet)
        .datacenters(datacenters)
        .blacklistedTables(blacklistedTables)
        .repairThreadCount(repairThreadCount)
        .timeout(segmentTimeout)
        .build(UUID.randomUUID());

    RepairRun run = RepairRun.builder(cluster.getName(), repairUnit.getId())
        .intensity(intensity)
        .segmentCount(100)
        .repairParallelism(RepairParallelism.PARALLEL)
        .adaptiveSchedule(true)
        .tables(TABLES).build(UUID.randomUUID());

    RepairSchedule repairSchedule = RepairSchedule.builder(repairUnit.getId())
        .adaptive(true)
        .daysBetween(1)
        .intensity(1)
        .owner("reaper")
        .segmentCountPerNode(64)
        .state(RepairSchedule.State.ACTIVE)
        .nextActivation(DateTime.now())
        .repairParallelism(RepairParallelism.DATACENTER_AWARE)
        .build(UUID.randomUUID());
    Collection<RepairSchedule> schedules = Lists.newArrayList(repairSchedule);
    IRepairRunDao mockedRepairRunDao = mock(IRepairRunDao.class);
    Mockito.when(((CassandraStorageFacade) context.storage).getRepairRunDao()).thenReturn(mockedRepairRunDao);
    Mockito.when(mockedRepairRunDao.getRepairRun(any())).thenReturn(Optional.of(run));

    IRepairSegmentDao mockedRepairSegmentDao = mock(IRepairSegmentDao.class);
    List<RepairSegment> segments = generateRepairSegments(100, 35, 10, repairUnit.getId());
    Mockito.when(mockedRepairSegmentDao.getSegmentsWithState(any(), any())).thenReturn(segments);
    Mockito.when(context.storage.getRepairSegmentDao()).thenReturn(mockedRepairSegmentDao);

    Mockito.when(((IDistributedStorage) context.storage).countRunningReapers()).thenReturn(1);
    IRepairUnitDao mockedRepairUnitDao = mock(IRepairUnitDao.class);
    Mockito.when(((CassandraStorageFacade) context.storage).getRepairUnitDao()).thenReturn(mockedRepairUnitDao);
    Mockito.when(mockedRepairUnitDao.getRepairUnit(any(UUID.class))).thenReturn(repairUnit);

    IRepairScheduleDao mockedRepairScheduleDao = Mockito.mock(IRepairScheduleDao.class);
    Mockito.when(context.storage.getRepairScheduleDao()).thenReturn(mockedRepairScheduleDao);
    Mockito.when(mockedRepairScheduleDao.getRepairSchedulesForClusterAndKeyspace(any(), any()))
        .thenReturn(schedules);
    IClusterDao mockedClusterDao = Mockito.mock(IClusterDao.class);
    Mockito.when(context.storage.getClusterDao()).thenReturn(mockedClusterDao);
    Mockito.when(mockedClusterDao.getCluster(any())).thenReturn(cluster);
    JmxManagementConnectionFactory jmxManagementConnectionFactory = mock(JmxManagementConnectionFactory.class);
    JmxCassandraManagementProxy jmx = mock(JmxCassandraManagementProxy.class);
    when(jmxManagementConnectionFactory.connectAny(any(Collection.class))).thenReturn(jmx);
    when(jmx.getClusterName()).thenReturn(cluster.getName());
    when(jmx.isConnectionAlive()).thenReturn(true);
    when(jmx.getRangeToEndpointMap(anyString())).thenReturn(RepairRunnerTest.threeNodeClusterWithIps());
    when(jmx.getEndpointToHostId()).thenReturn(nodeMap);
    when(jmx.getTokens()).thenReturn(tokens);
    when(jmx.isRepairRunning()).thenReturn(true);
    when(jmx.getPendingCompactions()).thenReturn(3);
    context.managementConnectionFactory = jmxManagementConnectionFactory;
    context.config = new ReaperApplicationConfiguration();
    context.config.setDatacenterAvailability(DatacenterAvailability.LOCAL);

    ClusterFacade clusterFacade = mock(ClusterFacade.class);

    RepairRunner repairRunner = RepairRunner.create(
        context,
        UUID.randomUUID(),
        clusterFacade,
        context.storage.getRepairRunDao());

    RepairRunner repairRunnerSpy = Mockito.spy(repairRunner);

    repairRunnerSpy.maybeAdaptRepairSchedule();
    verify(repairRunnerSpy, times(1)).tuneAdaptiveRepair(anyDouble(), anyInt());
    verify(repairRunnerSpy, times(1)).addSegmentsPerNodeToScheduleForUnit();
  }

  @Test
  public void adaptiveRepairRaiseTimeoutTest() throws Exception {
    final String ksName = "reaper";
    final Set<String> cfNames = Sets.newHashSet("reaper");
    final boolean incrementalRepair = false;
    final boolean subrangeIncremental = false;
    final Set<String> nodeSet = Sets.newHashSet("127.0.0.1", "127.0.0.2", "127.0.0.3");
    final Map<String, String> nodeMap = ImmutableMap.of(
        "127.0.0.1", "dc1", "127.0.0.2", "dc1", "127.0.0.3", "dc1"
    );
    final Set<String> datacenters = Collections.emptySet();
    final Set<String> blacklistedTables = Collections.emptySet();
    final double intensity = 0.5f;
    final int repairThreadCount = 1;
    final int segmentTimeout = 30;
    final int maxDuration = 10;
    final List<BigInteger> tokens = THREE_TOKENS;
    final AppContext context = new AppContext();
    context.storage = Mockito.mock(CassandraStorageFacade.class);
    RepairUnit repairUnit = RepairUnit.builder()
        .clusterName(cluster.getName())
        .keyspaceName(ksName)
        .columnFamilies(cfNames)
        .incrementalRepair(incrementalRepair)
        .subrangeIncrementalRepair(subrangeIncremental)
        .nodes(nodeSet)
        .datacenters(datacenters)
        .blacklistedTables(blacklistedTables)
        .repairThreadCount(repairThreadCount)
        .timeout(segmentTimeout)
        .build(UUID.randomUUID());

    RepairRun run = RepairRun.builder(cluster.getName(), repairUnit.getId())
        .intensity(intensity)
        .segmentCount(100)
        .repairParallelism(RepairParallelism.PARALLEL)
        .adaptiveSchedule(true)
        .tables(TABLES).build(UUID.randomUUID());

    RepairSchedule repairSchedule = RepairSchedule.builder(repairUnit.getId())
        .adaptive(true)
        .daysBetween(1)
        .intensity(1)
        .owner("reaper")
        .segmentCountPerNode(64)
        .state(RepairSchedule.State.ACTIVE)
        .nextActivation(DateTime.now())
        .repairParallelism(RepairParallelism.DATACENTER_AWARE)
        .build(UUID.randomUUID());
    Collection<RepairSchedule> schedules = Lists.newArrayList(repairSchedule);

    IRepairRunDao mockedRepairRunDao = mock(IRepairRunDao.class);
    Mockito.when(context.storage.getRepairRunDao()).thenReturn(mockedRepairRunDao);
    Mockito.when(mockedRepairRunDao.getRepairRun(any())).thenReturn(Optional.of(run));

    IRepairSegmentDao mockedRepairSegmentDao = mock(IRepairSegmentDao.class);
    List<RepairSegment> segments = generateRepairSegments(100, 10, maxDuration, repairUnit.getId());
    Mockito.when(mockedRepairSegmentDao.getSegmentsWithState(any(), any())).thenReturn(segments);
    Mockito.when(context.storage.getRepairSegmentDao()).thenReturn(mockedRepairSegmentDao);

    Mockito.when(((IDistributedStorage) context.storage).countRunningReapers()).thenReturn(1);
    IRepairUnitDao mockedRepairUnitDao = mock(IRepairUnitDao.class);
    Mockito.when(((CassandraStorageFacade) context.storage).getRepairUnitDao()).thenReturn(mockedRepairUnitDao);
    Mockito.when(mockedRepairUnitDao.getRepairUnit(any(UUID.class))).thenReturn(repairUnit);

    IRepairScheduleDao mockedRepairScheduleDao = Mockito.mock(IRepairScheduleDao.class);
    Mockito.when(context.storage.getRepairScheduleDao()).thenReturn(mockedRepairScheduleDao);
    Mockito.when(mockedRepairScheduleDao.getRepairSchedulesForClusterAndKeyspace(any(), any()))
        .thenReturn(schedules);
    IClusterDao mockedClusterDao = Mockito.mock(IClusterDao.class);
    Mockito.when(context.storage.getClusterDao()).thenReturn(mockedClusterDao);
    Mockito.when(mockedClusterDao.getCluster(any())).thenReturn(cluster);


    Mockito.when(mockedClusterDao.getCluster(any())).thenReturn(cluster);
    JmxManagementConnectionFactory jmxManagementConnectionFactory = mock(JmxManagementConnectionFactory.class);
    JmxCassandraManagementProxy jmx = mock(JmxCassandraManagementProxy.class);
    when(jmxManagementConnectionFactory.connectAny(any(Collection.class))).thenReturn(jmx);
    when(jmx.getClusterName()).thenReturn(cluster.getName());
    when(jmx.isConnectionAlive()).thenReturn(true);
    when(jmx.getRangeToEndpointMap(anyString())).thenReturn(RepairRunnerTest.threeNodeClusterWithIps());
    when(jmx.getEndpointToHostId()).thenReturn(nodeMap);
    when(jmx.getTokens()).thenReturn(tokens);
    when(jmx.isRepairRunning()).thenReturn(true);
    when(jmx.getPendingCompactions()).thenReturn(3);
    context.managementConnectionFactory = jmxManagementConnectionFactory;
    context.config = new ReaperApplicationConfiguration();
    context.config.setDatacenterAvailability(DatacenterAvailability.LOCAL);

    ClusterFacade clusterFacade = mock(ClusterFacade.class);

    RepairRunner repairRunner = RepairRunner.create(
        context,
        UUID.randomUUID(),
        clusterFacade,
        context.storage.getRepairRunDao());

    RepairRunner repairRunnerSpy = Mockito.spy(repairRunner);

    repairRunnerSpy.maybeAdaptRepairSchedule();
    verify(repairRunnerSpy, times(1)).tuneAdaptiveRepair(anyDouble(), anyInt());
    verify(repairRunnerSpy, times(1)).raiseTimeoutOfUnit();
  }

  @Test
  public void notAdaptiveScheduleTest() throws Exception {
    final String ksName = "reaper";
    final Set<String> cfNames = Sets.newHashSet("reaper");
    final boolean incrementalRepair = false;
    final boolean subrangeIncremental = false;
    final Set<String> nodeSet = Sets.newHashSet("127.0.0.1", "127.0.0.2", "127.0.0.3");
    final Map<String, String> nodeMap = ImmutableMap.of(
        "127.0.0.1", "dc1", "127.0.0.2", "dc1", "127.0.0.3", "dc1"
    );
    final Set<String> datacenters = Collections.emptySet();
    final Set<String> blacklistedTables = Collections.emptySet();
    final double intensity = 0.5f;
    final int repairThreadCount = 1;
    final int segmentTimeout = 30;
    final int maxDuration = 10;
    final List<BigInteger> tokens = THREE_TOKENS;
    final AppContext context = new AppContext();
    context.storage = Mockito.mock(CassandraStorageFacade.class);
    RepairUnit repairUnit = RepairUnit.builder()
        .clusterName(cluster.getName())
        .keyspaceName(ksName)
        .columnFamilies(cfNames)
        .incrementalRepair(incrementalRepair)
        .subrangeIncrementalRepair(subrangeIncremental)
        .nodes(nodeSet)
        .datacenters(datacenters)
        .blacklistedTables(blacklistedTables)
        .repairThreadCount(repairThreadCount)
        .timeout(segmentTimeout)
        .build(UUID.randomUUID());

    RepairRun run = RepairRun.builder(cluster.getName(), repairUnit.getId())
        .intensity(intensity)
        .segmentCount(100)
        .repairParallelism(RepairParallelism.PARALLEL)
        .adaptiveSchedule(false)
        .tables(TABLES).build(UUID.randomUUID());

    IRepairRunDao mockedRepairRunDao = mock(IRepairRunDao.class);
    Mockito.when(mockedRepairRunDao.getRepairRun(any())).thenReturn(Optional.of(run));
    Mockito.when(((CassandraStorageFacade) context.storage).getRepairRunDao()).thenReturn(mockedRepairRunDao);

    IRepairSegmentDao mockedRepairSegmentDao = mock(IRepairSegmentDao.class);
    List<RepairSegment> segments = generateRepairSegments(100, 30, maxDuration, repairUnit.getId());
    Mockito.when(mockedRepairSegmentDao.getSegmentsWithState(any(), any())).thenReturn(segments);
    Mockito.when(context.storage.getRepairSegmentDao()).thenReturn(mockedRepairSegmentDao);

    Mockito.when(((IDistributedStorage) context.storage).countRunningReapers()).thenReturn(1);
    IRepairUnitDao mockedRepairUnitDao = mock(IRepairUnitDao.class);
    Mockito.when(((CassandraStorageFacade) context.storage).getRepairUnitDao()).thenReturn(mockedRepairUnitDao);
    Mockito.when(mockedRepairUnitDao.getRepairUnit(any(UUID.class))).thenReturn(repairUnit);

    IClusterDao mockedClusterDao = Mockito.mock(IClusterDao.class);
    Mockito.when(context.storage.getClusterDao()).thenReturn(mockedClusterDao);
    Mockito.when(mockedClusterDao.getCluster(any())).thenReturn(cluster);
    JmxManagementConnectionFactory jmxManagementConnectionFactory = mock(JmxManagementConnectionFactory.class);
    JmxCassandraManagementProxy jmx = mock(JmxCassandraManagementProxy.class);
    when(jmxManagementConnectionFactory.connectAny(any(Collection.class))).thenReturn(jmx);
    when(jmx.getClusterName()).thenReturn(cluster.getName());
    when(jmx.isConnectionAlive()).thenReturn(true);
    when(jmx.getRangeToEndpointMap(anyString())).thenReturn(RepairRunnerTest.threeNodeClusterWithIps());
    when(jmx.getEndpointToHostId()).thenReturn(nodeMap);
    when(jmx.getTokens()).thenReturn(tokens);
    when(jmx.isRepairRunning()).thenReturn(true);
    when(jmx.getPendingCompactions()).thenReturn(3);
    context.managementConnectionFactory = jmxManagementConnectionFactory;
    context.config = new ReaperApplicationConfiguration();
    context.config.setDatacenterAvailability(DatacenterAvailability.LOCAL);

    ClusterFacade clusterFacade = mock(ClusterFacade.class);

    RepairRunner repairRunner = RepairRunner.create(
        context,
        UUID.randomUUID(),
        clusterFacade,
        context.storage.getRepairRunDao());

    RepairRunner repairRunnerSpy = Mockito.spy(repairRunner);

    repairRunnerSpy.maybeAdaptRepairSchedule();
    verify(repairRunnerSpy, Mockito.never()).tuneAdaptiveRepair(anyDouble(), anyInt());
  }

  private List<RepairSegment> generateRepairSegments(
      int nbSegments,
      int extendedSegments,
      int segmentDuration,
      UUID repairUnitId) {
    List<RepairSegment> segments = Lists.newArrayList();
    for (int i = 0; i < nbSegments; i++) {
      int failCount = extendedSegments > i ? 1 : 0;
      DateTime startTime = DateTime.now();
      RepairSegment segment = RepairSegment.builder(mock(Segment.class), repairUnitId)
          .withStartTime(startTime)
          .withEndTime(startTime.plusMinutes(segmentDuration))
          .withFailCount(failCount)
          .withId(UUID.randomUUID())
          .withRunId(repairUnitId)
          .withState(RepairSegment.State.DONE)
          .build();
      segments.add(segment);
    }
    return segments;
  }

  @Test
  public void isAllowedToRunTest() throws ReaperException {
    final String ksName = "reaper";
    final Set<String> cfNames = Sets.newHashSet("reaper");
    final boolean incrementalRepair = false;
    final boolean subrangeIncremental = false;
    final Set<String> nodeSet = Sets.newHashSet("127.0.0.1", "127.0.0.2", "127.0.0.3");
    final Map<String, String> nodeMap = ImmutableMap.of(
        "127.0.0.1", "dc1", "127.0.0.2", "dc1", "127.0.0.3", "dc1"
    );
    final Set<String> datacenters = Collections.emptySet();
    final Set<String> blacklistedTables = Collections.emptySet();
    final double intensity = 0.5f;
    final int repairThreadCount = 1;
    final int segmentTimeout = 30;
    final int maxDuration = 10;
    final AppContext context = new AppContext();
    context.storage = Mockito.mock(CassandraStorageFacade.class);
    context.config = new ReaperApplicationConfiguration();
    context.config.setMaxParallelRepairs(3);
    IClusterDao mockedClusterDao = Mockito.mock(IClusterDao.class);
    Mockito.when(context.storage.getClusterDao()).thenReturn(mockedClusterDao);
    Mockito.when(mockedClusterDao.getCluster(any())).thenReturn(cluster);

    RepairUnit repairUnit = RepairUnit.builder()
        .clusterName(cluster.getName())
        .keyspaceName(ksName)
        .columnFamilies(cfNames)
        .incrementalRepair(incrementalRepair)
        .subrangeIncrementalRepair(subrangeIncremental)
        .nodes(nodeSet)
        .datacenters(datacenters)
        .blacklistedTables(blacklistedTables)
        .repairThreadCount(repairThreadCount)
        .timeout(segmentTimeout)
        .build(UUID.randomUUID());

    // The allowed run time UUID will be the first one generated
    UUID allowedRun = UUIDs.timeBased();
    RepairRun run = RepairRun.builder(cluster.getName(), repairUnit.getId())
        .intensity(intensity)
        .segmentCount(100)
        .repairParallelism(RepairParallelism.PARALLEL)
        .adaptiveSchedule(false)
        .tables(TABLES).build(allowedRun);

    IRepairRunDao mockedRepairRunDao = mock(IRepairRunDao.class);
    Mockito.when(mockedRepairRunDao.getRepairRun(any())).thenReturn(Optional.of(run));
    Mockito.when(((CassandraStorageFacade) context.storage).getRepairRunDao()).thenReturn(mockedRepairRunDao);

    IRepairSegmentDao mockedRepairSegmentDao = mock(IRepairSegmentDao.class);
    List<RepairSegment> segments = generateRepairSegments(100, 30, maxDuration, repairUnit.getId());
    Mockito.when(mockedRepairSegmentDao.getSegmentsWithState(any(), any())).thenReturn(segments);
    Mockito.when(context.storage.getRepairSegmentDao()).thenReturn(mockedRepairSegmentDao);

    Mockito.when(((IDistributedStorage) context.storage).countRunningReapers()).thenReturn(1);
    IRepairUnitDao mockedRepairUnitDao = mock(IRepairUnitDao.class);
    Mockito.when(((CassandraStorageFacade) context.storage).getRepairUnitDao()).thenReturn(mockedRepairUnitDao);
    Mockito.when(mockedRepairUnitDao.getRepairUnit(any(UUID.class))).thenReturn(repairUnit);


    List<UUID> runningRepairs = Lists.newArrayList();
    for (int i = 0; i < 3; i++) {
      runningRepairs.add(UUIDs.timeBased());
    }
    runningRepairs.add(allowedRun);

    ClusterFacade clusterFacade = mock(ClusterFacade.class);
    RepairRunner repairRunner = RepairRunner.create(
        context,
        UUID.randomUUID(),
        clusterFacade,
        context.storage.getRepairRunDao());

    assertTrue(repairRunner.isAllowedToRun(runningRepairs, allowedRun));
  }

  @Test
  public void isNotAllowedToRunTest() throws ReaperException {
    final String ksName = "reaper";
    final Set<String> cfNames = Sets.newHashSet("reaper");
    final boolean incrementalRepair = false;
    final boolean subrangeIncremental = false;
    final Set<String> nodeSet = Sets.newHashSet("127.0.0.1", "127.0.0.2", "127.0.0.3");
    final Map<String, String> nodeMap = ImmutableMap.of(
        "127.0.0.1", "dc1", "127.0.0.2", "dc1", "127.0.0.3", "dc1"
    );
    final Set<String> datacenters = Collections.emptySet();
    final Set<String> blacklistedTables = Collections.emptySet();
    final double intensity = 0.5f;
    final int repairThreadCount = 1;
    final int segmentTimeout = 30;
    final int maxDuration = 10;
    final AppContext context = new AppContext();
    context.storage = Mockito.mock(CassandraStorageFacade.class);
    context.config = new ReaperApplicationConfiguration();
    context.config.setMaxParallelRepairs(3);
    IClusterDao mockedClusterDao = Mockito.mock(IClusterDao.class);
    Mockito.when(context.storage.getClusterDao()).thenReturn(mockedClusterDao);
    Mockito.when(mockedClusterDao.getCluster(any())).thenReturn(cluster);

    RepairUnit repairUnit = RepairUnit.builder()
        .clusterName(cluster.getName())
        .keyspaceName(ksName)
        .columnFamilies(cfNames)
        .incrementalRepair(incrementalRepair)
        .subrangeIncrementalRepair(subrangeIncremental)
        .nodes(nodeSet)
        .datacenters(datacenters)
        .blacklistedTables(blacklistedTables)
        .repairThreadCount(repairThreadCount)
        .timeout(segmentTimeout)
        .build(UUID.randomUUID());

    IRepairSegmentDao mockedRepairSegmentDao = mock(IRepairSegmentDao.class);
    List<RepairSegment> segments = generateRepairSegments(100, 30, maxDuration, repairUnit.getId());
    Mockito.when(mockedRepairSegmentDao.getSegmentsWithState(any(), any())).thenReturn(segments);
    Mockito.when(context.storage.getRepairSegmentDao()).thenReturn(mockedRepairSegmentDao);

    Mockito.when(((IDistributedStorage) context.storage).countRunningReapers()).thenReturn(1);
    IRepairUnitDao mockedRepairUnitDao = mock(IRepairUnitDao.class);
    Mockito.when(((CassandraStorageFacade) context.storage).getRepairUnitDao()).thenReturn(mockedRepairUnitDao);
    Mockito.when(mockedRepairUnitDao.getRepairUnit(any(UUID.class))).thenReturn(repairUnit);


    List<UUID> runningRepairs = Lists.newArrayList();
    for (int i = 0; i < 3; i++) {
      runningRepairs.add(UUIDs.timeBased());
    }

    UUID unallowedRun = UUIDs.timeBased();
    RepairRun run = RepairRun.builder(cluster.getName(), repairUnit.getId())
        .intensity(intensity)
        .segmentCount(100)
        .repairParallelism(RepairParallelism.PARALLEL)
        .adaptiveSchedule(false)
        .tables(TABLES).build(unallowedRun);

    IRepairRunDao mockedRepairRunDao = mock(IRepairRunDao.class);
    Mockito.when(mockedRepairRunDao.getRepairRun(any())).thenReturn(Optional.of(run));
    Mockito.when(((CassandraStorageFacade) context.storage).getRepairRunDao()).thenReturn(mockedRepairRunDao);
    runningRepairs.add(unallowedRun);

    ClusterFacade clusterFacade = mock(ClusterFacade.class);
    RepairRunner repairRunner = RepairRunner.create(
        context,
        UUID.randomUUID(),
        clusterFacade,
        context.storage.getRepairRunDao());

    assertFalse(repairRunner.isAllowedToRun(runningRepairs, unallowedRun));
  }

  @Test
  public void testGetRunningRepairRunIds() {
    String currentClusterName = "cluster1";
    List<UUID> actualIds = RepairRunner.getRunningRepairRunIds(currentRunners, currentClusterName);
    assertEquals("Expected 3 running repair run IDs", 3, actualIds.size());

    String otherCurrentClusterName = "cluster2";
    actualIds = RepairRunner.getRunningRepairRunIds(currentRunners, otherCurrentClusterName);
    assertEquals("Expected 1 running repair run IDs", 1, actualIds.size());
  }

  @Test
  public void testGetRunningRepairRunIdsEmpty() {
    String currentClusterName = "cluster3";
    List<UUID> actualIds = RepairRunner.getRunningRepairRunIds(currentRunners, currentClusterName);
    assertEquals("Expected no running repair run ID", 0, actualIds.size());
  }
}