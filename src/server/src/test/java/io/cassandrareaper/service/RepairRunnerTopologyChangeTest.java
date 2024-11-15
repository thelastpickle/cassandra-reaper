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

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.MalformedObjectNameException;
import javax.management.ReflectionException;

import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.utils.progress.ProgressEventType;
import org.apache.commons.lang3.RandomStringUtils;
import org.joda.time.DateTime;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.cassandrareaper.AppContext;
import io.cassandrareaper.ReaperApplicationConfiguration;
import io.cassandrareaper.ReaperApplicationConfiguration.AutoSchedulingConfiguration;
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.CompactionStats;
import io.cassandrareaper.core.Node;
import io.cassandrareaper.core.RepairRun;
import io.cassandrareaper.core.RepairSchedule;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.management.MalformedObjectNameException;
import javax.management.ReflectionException;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.commons.lang3.RandomStringUtils;
import org.joda.time.DateTime;
import org.junit.Test;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class RepairRunnerTopologyChangeTest {

  private static final List<BigInteger> THREE_TOKENS = Lists.newArrayList(
          BigInteger.valueOf(0L),
          BigInteger.valueOf(100L),
          BigInteger.valueOf(200L));

  private static final Set<String> TABLES = ImmutableSet.of("table1");

  private final Cluster cluster = Cluster.builder()
          .withName("test_" + RandomStringUtils.randomAlphabetic(12))
          .withSeedHosts(ImmutableSet.of("127.0.0.1"))
          .withState(Cluster.State.ACTIVE)
          .build();

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

  private static Map<List<String>, List<String>> fourNodeClusterAfterBootstrap() {
    Map<List<String>, List<String>> map = new HashMap<List<String>, List<String>>();
    map = addRangeToMap(map, "0", "50", "127.0.0.1", "127.0.0.4", "127.0.0.2");
    map = addRangeToMap(map, "50", "100", "127.0.0.4", "127.0.0.2", "127.0.0.3");
    map = addRangeToMap(map, "100", "200", "127.0.0.2", "127.0.0.3", "127.0.0.1");
    map = addRangeToMap(map, "200", "0", "127.0.0.3", "127.0.0.1", "127.0.0.4");
    return map;
  }

  private static Map<List<String>, List<String>> twoNodeClusterAfterBootstrap() {
    Map<List<String>, List<String>> map = new HashMap<List<String>, List<String>>();
    map = addRangeToMap(map, "0", "200", "127.0.0.1", "127.0.0.3");
    map = addRangeToMap(map, "200", "0", "127.0.0.3", "127.0.0.1");
    return map;
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
                    .segmentCount(64)
                    .repairParallelism(RepairParallelism.PARALLEL)
                    .tables(TABLES),
            Lists.newArrayList(
                RepairSegment.builder(
                        Segment.builder()
                            .withTokenRange(new RingRange(BigInteger.ZERO, new BigInteger("100")))
                            .withReplicas(nodeMap)
                            .build(),
                        cf)
                    .withHostID(hostID),
                RepairSegment.builder(
                        Segment.builder()
                            .withTokenRange(
                                new RingRange(new BigInteger("100"), new BigInteger("200")))
                            .withReplicas(nodeMap)
                            .build(),
                        cf)
                    .withHostID(hostID)));
  }

  private Map<String, String> endpointToHostIDMap() {
    Map<String, String> endpointToHostIDMap = new HashMap<String, String>();
    endpointToHostIDMap.put("127.0.0.1", UUID.randomUUID().toString());
    endpointToHostIDMap.put("127.0.0.2", UUID.randomUUID().toString());
    endpointToHostIDMap.put("127.0.0.3", UUID.randomUUID().toString());

    return endpointToHostIDMap;
  }

  @Test
  public void testFailRepairAfterAdditiveChangeInTopology() throws ReaperException, IOException {
    final String ksName = "reaper";
    final Set<String> cfNames = Sets.newHashSet("reaper");
    final boolean incrementalRepair = false;
    final Set<String> nodeSet = Sets.newHashSet("127.0.0.1", "127.0.0.2", "127.0.0.3");
    final Map<String, String> nodeMap = ImmutableMap.of(
            "127.0.0.1", "dc1", "127.0.0.2", "dc1", "127.0.0.3", "dc1");
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
    context.config.setAutoScheduling(new AutoSchedulingConfiguration());
    context.config.setScheduleRetryOnError(true);
    context.config.setScheduleRetryDelay(java.time.Duration.parse("PT1H"));
    context.schedulingManager = mock(SchedulingManager.class);
    doNothing().when(context.schedulingManager).maybeRegisterRepairRunCompleted(any());
    storage.getClusterDao().addCluster(cluster);
    UUID cf = storage.getRepairUnitDao().addRepairUnit(
                    RepairUnit.builder().clusterName(cluster.getName())
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
    DateTime initialActivationDate = DateTime.now();
    DateTime nextActivationDate = initialActivationDate.plusHours(2);
    storage.getRepairScheduleDao().addRepairSchedule(RepairSchedule.builder(cf).daysBetween(1).intensity(1)
            .segmentCountPerNode(64)
            .nextActivation(nextActivationDate)
            .repairParallelism(RepairParallelism.DATACENTER_AWARE)
    );
    final Map<String, String> endpointToHostIDMap = endpointToHostIDMap();
    RepairRun run = addNewRepairRun(nodeMap, intensity, storage, cf, null);
    assertEquals(64, run.getSegmentCount());
    final UUID runId = run.getId();
    final UUID segmentId = storage.getRepairSegmentDao().getNextFreeSegments(run.getId()).get(0).getId();
    assertEquals(storage.getRepairSegmentDao().getRepairSegment(runId, segmentId).get().getState(),
            RepairSegment.State.NOT_STARTED);
    final JmxCassandraManagementProxy jmx = CassandraManagementProxyTest.mockJmxProxyImpl();
    when(jmx.getClusterName()).thenReturn(cluster.getName());
    when(jmx.isConnectionAlive()).thenReturn(true);
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
    context.repairManager = RepairManager.create(
            context,
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
              final RepairStatusHandler handler = invocation.getArgument(5);
              // Execute in a separate thread to simulate async behavior
              new Thread(
                      () -> {
                        try {
                          handler.handle(
                              repairNumber, Optional.of(ProgressEventType.START), null, jmx);
                          Thread.sleep(100); // Add small delay between notifications
                          handler.handle(
                              repairNumber, Optional.of(ProgressEventType.SUCCESS), null, jmx);
                          Thread.sleep(100); // Add small delay between notifications
                          handler.handle(
                              repairNumber, Optional.of(ProgressEventType.COMPLETE), null, jmx);
                        } catch (InterruptedException e) {
                          Thread.currentThread().interrupt();
                        }
                      })
                  .start();
              return repairNumber;
            });
    context.managementConnectionFactory =
        new JmxManagementConnectionFactory(context, new NoopCrypotograph()) {
          @Override
          protected JmxCassandraManagementProxy connectImpl(Node host) throws ReaperException {
            return jmx;
          }
        };
    assertEquals(
        RepairRun.RunState.NOT_STARTED,
        storage.getRepairRunDao().getRepairRun(runId).get().getRunState());
    storage
        .getRepairRunDao()
        .updateRepairRun(
            run.with().runState(RepairRun.RunState.RUNNING).startTime(DateTime.now()).build(runId));
    when(jmx.getRangeToEndpointMap(anyString())).thenReturn(fourNodeClusterAfterBootstrap());
    context.repairManager.resumeRunningRepairRuns();
    // The repair run should fail due to the token ranges for each node becoming smaller, resulting in
    // the new ranges not completely enclosing every previously calculated segment.
    await().with().atMost(20, TimeUnit.SECONDS).untilAsserted(() -> {
      RepairRun repairRun = storage.getRepairRunDao().getRepairRun(runId).get();
      assertEquals("Repair run " + runId + " did not reach ERROR state within timeout",
          RepairRun.RunState.ERROR, repairRun.getRunState());
    });
    RepairSchedule updatedRepairSchedule = storage.getRepairScheduleDao()
            .getRepairSchedulesForClusterAndKeyspace(cluster.getName(), ksName)
            .iterator().next();
    // Ensure that repair schedule has been updated to activate again in one hour with a delta of a minute, given
    // test execution takes a few seconds. One hour is the default schedule retry delay.
    assertTrue(updatedRepairSchedule.getNextActivation().isAfter(initialActivationDate.plusHours(1)));
    assertTrue(updatedRepairSchedule.getNextActivation().isBefore(initialActivationDate.plusHours(1).plusMinutes(1)));
  }

  @Test
  public void testAfterAdditiveChangeInTopologyNoErrorWhenRepairScheduleDoesNotExist()
          throws ReaperException, IOException {
    final String ksName = "reaper";
    final Set<String> cfNames = Sets.newHashSet("reaper");
    final boolean incrementalRepair = false;
    final Set<String> nodeSet = Sets.newHashSet("127.0.0.1", "127.0.0.2", "127.0.0.3");
    final Map<String, String> nodeMap = ImmutableMap.of(
            "127.0.0.1", "dc1", "127.0.0.2", "dc1", "127.0.0.3", "dc1");
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
    context.config.setAutoScheduling(new AutoSchedulingConfiguration());
    context.config.setScheduleRetryOnError(true);
    context.config.setScheduleRetryDelay(java.time.Duration.parse("PT1H"));
    context.schedulingManager = mock(SchedulingManager.class);
    doNothing().when(context.schedulingManager).maybeRegisterRepairRunCompleted(any());
    storage.getClusterDao().addCluster(cluster);
    UUID cf = storage.getRepairUnitDao().addRepairUnit(
                    RepairUnit.builder().clusterName(cluster.getName())
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
    RepairRun run = addNewRepairRun(nodeMap, intensity, storage, cf, null);
    final UUID runId = run.getId();
    final UUID segmentId = storage.getRepairSegmentDao().getNextFreeSegments(run.getId()).get(0).getId();
    assertEquals(storage.getRepairSegmentDao()
            .getRepairSegment(runId, segmentId).get().getState(), RepairSegment.State.NOT_STARTED);
    final JmxCassandraManagementProxy jmx = CassandraManagementProxyTest.mockJmxProxyImpl();
    when(jmx.getClusterName()).thenReturn(cluster.getName());
    when(jmx.isConnectionAlive()).thenReturn(true);
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
    context.repairManager = RepairManager.create(
            context,
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
              final RepairStatusHandler handler = invocation.getArgument(5);
              // Execute in a separate thread to simulate async behavior
              new Thread(
                      () -> {
                        try {
                          handler.handle(
                              repairNumber, Optional.of(ProgressEventType.START), null, jmx);
                          Thread.sleep(100); // Add small delay between notifications
                          handler.handle(
                              repairNumber, Optional.of(ProgressEventType.SUCCESS), null, jmx);
                          Thread.sleep(100); // Add small delay between notifications
                          handler.handle(
                              repairNumber, Optional.of(ProgressEventType.COMPLETE), null, jmx);
                        } catch (InterruptedException e) {
                          Thread.currentThread().interrupt();
                        }
                      })
                  .start();
              return repairNumber;
            });
    context.managementConnectionFactory =
        new JmxManagementConnectionFactory(context, new NoopCrypotograph()) {
          @Override
          protected JmxCassandraManagementProxy connectImpl(Node host) throws ReaperException {
            return jmx;
          }
        };
    assertEquals(
        RepairRun.RunState.NOT_STARTED,
        storage.getRepairRunDao().getRepairRun(runId).get().getRunState());
    storage
        .getRepairRunDao()
        .updateRepairRun(
            run.with().runState(RepairRun.RunState.RUNNING).startTime(DateTime.now()).build(runId));
    when(jmx.getRangeToEndpointMap(anyString())).thenReturn(fourNodeClusterAfterBootstrap());
    context.repairManager.resumeRunningRepairRuns();
    await().with().atMost(20, TimeUnit.SECONDS).untilAsserted(() -> {
      RepairRun repairRun = storage.getRepairRunDao().getRepairRun(runId).get();
      assertEquals("Repair run " + runId + " did not reach ERROR state within timeout",
          RepairRun.RunState.ERROR, repairRun.getRunState());
    });
  }

  @Test
  public void testAfterAdditiveChangeInTopologyNoRescheduleWhenScheduleRetryOnErrorIsFalse()
          throws ReaperException, IOException {
    final String ksName = "reaper";
    final Set<String> cfNames = Sets.newHashSet("reaper");
    final boolean incrementalRepair = false;
    final Set<String> nodeSet = Sets.newHashSet("127.0.0.1", "127.0.0.2", "127.0.0.3");
    final Map<String, String> nodeMap = ImmutableMap.of(
            "127.0.0.1", "dc1", "127.0.0.2", "dc1", "127.0.0.3", "dc1");
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
    context.config.setAutoScheduling(new AutoSchedulingConfiguration());
    context.config.setScheduleRetryOnError(false);
    context.config.setScheduleRetryDelay(java.time.Duration.parse("PT1H"));
    context.schedulingManager = mock(SchedulingManager.class);
    doNothing().when(context.schedulingManager).maybeRegisterRepairRunCompleted(any());
    storage.getClusterDao().addCluster(cluster);
    UUID cf = storage.getRepairUnitDao().addRepairUnit(
                    RepairUnit.builder().clusterName(cluster.getName())
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
    DateTime initialActivationDate = DateTime.now();
    DateTime nextActivationDate = initialActivationDate.plusHours(2);
    storage.getRepairScheduleDao().addRepairSchedule(RepairSchedule.builder(cf)
            .daysBetween(1).intensity(1).segmentCountPerNode(64)
            .nextActivation(nextActivationDate)
            .repairParallelism(RepairParallelism.DATACENTER_AWARE)
    );
    final Map<String, String> endpointToHostIDMap = endpointToHostIDMap();
    RepairRun run = addNewRepairRun(nodeMap, intensity, storage, cf, null);
    final UUID runId = run.getId();
    final UUID segmentId = storage.getRepairSegmentDao().getNextFreeSegments(run.getId()).get(0).getId();
    assertEquals(storage.getRepairSegmentDao()
            .getRepairSegment(runId, segmentId).get().getState(), RepairSegment.State.NOT_STARTED);
    final JmxCassandraManagementProxy jmx = CassandraManagementProxyTest.mockJmxProxyImpl();
    when(jmx.getClusterName()).thenReturn(cluster.getName());
    when(jmx.isConnectionAlive()).thenReturn(true);
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
    context.repairManager = RepairManager.create(
            context,
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
              final RepairStatusHandler handler = invocation.getArgument(5);
              // Execute in a separate thread to simulate async behavior
              new Thread(
                      () -> {
                        try {
                          handler.handle(
                              repairNumber, Optional.of(ProgressEventType.START), null, jmx);
                          Thread.sleep(100); // Add small delay between notifications
                          handler.handle(
                              repairNumber, Optional.of(ProgressEventType.SUCCESS), null, jmx);
                          Thread.sleep(100); // Add small delay between notifications
                          handler.handle(
                              repairNumber, Optional.of(ProgressEventType.COMPLETE), null, jmx);
                        } catch (InterruptedException e) {
                          Thread.currentThread().interrupt();
                        }
                      })
                  .start();
              return repairNumber;
            });
    context.managementConnectionFactory =
        new JmxManagementConnectionFactory(context, new NoopCrypotograph()) {
          @Override
          protected JmxCassandraManagementProxy connectImpl(Node host) throws ReaperException {
            return jmx;
          }
        };
    assertEquals(
        RepairRun.RunState.NOT_STARTED,
        storage.getRepairRunDao().getRepairRun(runId).get().getRunState());
    storage
        .getRepairRunDao()
        .updateRepairRun(
            run.with().runState(RepairRun.RunState.RUNNING).startTime(DateTime.now()).build(runId));
    when(jmx.getRangeToEndpointMap(anyString())).thenReturn(fourNodeClusterAfterBootstrap());
    context.repairManager.resumeRunningRepairRuns();
    await().with().atMost(20, TimeUnit.SECONDS).untilAsserted(() -> {
      RepairRun repairRun = storage.getRepairRunDao().getRepairRun(runId).get();
      assertEquals("Repair run " + runId + " did not reach ERROR state within timeout",
          RepairRun.RunState.ERROR, repairRun.getRunState());
    });
    RepairSchedule updatedRepairSchedule = storage.getRepairScheduleDao()
            .getRepairSchedulesForClusterAndKeyspace(cluster.getName(), ksName)
            .iterator().next();
    // Ensure that repair schedule has not been updated
    assertEquals(updatedRepairSchedule.getNextActivation(), nextActivationDate);
  }

  // TODO: fix this test, it is very flaky and fails randomly
  @Test
  @Ignore
  public void
      testAfterAdditiveChangeInTopologyNoRescheduleWhenScheduleRetryDelayIsLaterThanNextActivation()
          throws ReaperException, IOException {
    final String ksName = "reaper";
    final Set<String> cfNames = Sets.newHashSet("reaper");
    final boolean incrementalRepair = false;
    final Set<String> nodeSet = Sets.newHashSet("127.0.0.1", "127.0.0.2", "127.0.0.3");
    final Map<String, String> nodeMap = ImmutableMap.of(
            "127.0.0.1", "dc1", "127.0.0.2", "dc1", "127.0.0.3", "dc1");
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
    context.config.setAutoScheduling(new AutoSchedulingConfiguration());
    context.config.setScheduleRetryOnError(true);
    context.config.setScheduleRetryDelay(java.time.Duration.parse("PT3H"));
    context.schedulingManager = mock(SchedulingManager.class);
    doNothing().when(context.schedulingManager).maybeRegisterRepairRunCompleted(any());
    storage.getClusterDao().addCluster(cluster);
    UUID cf = storage.getRepairUnitDao().addRepairUnit(
                    RepairUnit.builder().clusterName(cluster.getName())
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
    DateTime initialActivationDate = DateTime.now();
    DateTime nextActivationDate = initialActivationDate.plusHours(2);
    storage.getRepairScheduleDao().addRepairSchedule(RepairSchedule.builder(cf).daysBetween(1).intensity(1)
            .segmentCountPerNode(64)
            .nextActivation(nextActivationDate)
            .repairParallelism(RepairParallelism.DATACENTER_AWARE)
    );
    final Map<String, String> endpointToHostIDMap = endpointToHostIDMap();
    RepairRun run = addNewRepairRun(nodeMap, intensity, storage, cf, null);
    final UUID runId = run.getId();
    final UUID segmentId = storage.getRepairSegmentDao().getNextFreeSegments(run.getId()).get(0).getId();
    assertEquals(storage.getRepairSegmentDao()
            .getRepairSegment(runId, segmentId).get().getState(), RepairSegment.State.NOT_STARTED);
    final JmxCassandraManagementProxy jmx = CassandraManagementProxyTest.mockJmxProxyImpl();
    when(jmx.getClusterName()).thenReturn(cluster.getName());
    when(jmx.isConnectionAlive()).thenReturn(true);
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
    context.repairManager = RepairManager.create(
            context,
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
              final RepairStatusHandler handler = invocation.getArgument(5);
              // Execute in a separate thread to simulate async behavior
              new Thread(
                      () -> {
                        try {
                          handler.handle(
                              repairNumber, Optional.of(ProgressEventType.START), null, jmx);
                          Thread.sleep(100); // Add small delay between notifications
                          handler.handle(
                              repairNumber, Optional.of(ProgressEventType.SUCCESS), null, jmx);
                          Thread.sleep(100); // Add small delay between notifications
                          handler.handle(
                              repairNumber, Optional.of(ProgressEventType.COMPLETE), null, jmx);
                        } catch (InterruptedException e) {
                          Thread.currentThread().interrupt();
                        }
                      })
                  .start();
              return repairNumber;
            });
    context.managementConnectionFactory =
        new JmxManagementConnectionFactory(context, new NoopCrypotograph()) {
          @Override
          protected JmxCassandraManagementProxy connectImpl(Node host) throws ReaperException {
            return jmx;
          }
        };
    assertEquals(
        RepairRun.RunState.NOT_STARTED,
        storage.getRepairRunDao().getRepairRun(runId).get().getRunState());
    storage
        .getRepairRunDao()
        .updateRepairRun(
            run.with().runState(RepairRun.RunState.RUNNING).startTime(DateTime.now()).build(runId));
    when(jmx.getRangeToEndpointMap(anyString())).thenReturn(fourNodeClusterAfterBootstrap());
    context.repairManager.resumeRunningRepairRuns();
    await().with().atMost(20, TimeUnit.SECONDS).untilAsserted(() -> {
      RepairRun repairRun = storage.getRepairRunDao().getRepairRun(runId).get();
      assertEquals("Repair run " + runId + " did not reach ERROR state within timeout",
          RepairRun.RunState.ERROR, repairRun.getRunState());
    });
    RepairSchedule updatedRepairSchedule = storage.getRepairScheduleDao()
            .getRepairSchedulesForClusterAndKeyspace(cluster.getName(), ksName)
            .iterator().next();
    // Ensure that repair schedule has not been updated
    assertEquals(updatedRepairSchedule.getNextActivation(), nextActivationDate);
  }

  // TODO: fix this test, it is very flaky and fails randomly
  @Test
  @Ignore
  public void testSuccessAfterSubtractiveChangeInTopology()
      throws InterruptedException,
          ReaperException,
          MalformedObjectNameException,
          ReflectionException,
          IOException {
    final String ksName = "testSuccessAfterSubtractiveChangeInTopology".toLowerCase();
    final Set<String> cfNames = Sets.newHashSet("testSuccessAfterSubtractiveChangeInTopology".toLowerCase());
    final boolean incrementalRepair = false;
    final Set<String> nodeSet = Sets.newHashSet("127.0.0.1", "127.0.0.2", "127.0.0.3");
    final Map<String, String> nodeMap = ImmutableMap.of("127.0.0.1", "dc1",
            "127.0.0.2", "dc1", "127.0.0.3", "dc1");
    final Set<String> datacenters = Collections.emptySet();
    final Set<String> blacklistedTables = Collections.emptySet();
    final double intensity = 0.5f;
    final int repairThreadCount = 1;
    final int segmentTimeout = 30;
    final IStorageDao storage = new MemoryStorageFacade();
    AppContext context = new AppContext();
    context.storage = storage;
    context.config = new ReaperApplicationConfiguration();
    context.schedulingManager = mock(SchedulingManager.class);
    doNothing().when(context.schedulingManager).maybeRegisterRepairRunCompleted(any());
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
    RepairRun run = addNewRepairRun(nodeMap, intensity, storage, cf, null);
    final UUID runId = run.getId();
    final UUID segmentId = storage.getRepairSegmentDao().getNextFreeSegments(run.getId()).get(0).getId();
    assertEquals(storage.getRepairSegmentDao()
            .getRepairSegment(runId, segmentId).get().getState(), RepairSegment.State.NOT_STARTED);
    final JmxCassandraManagementProxy jmx = CassandraManagementProxyTest.mockJmxProxyImpl();
    when(jmx.getClusterName()).thenReturn(cluster.getName());
    when(jmx.isConnectionAlive()).thenReturn(true);
    when(jmx.getRangeToEndpointMap(anyString())).thenReturn(RepairRunnerTest.threeNodeClusterWithIps());

    when(jmx.getEndpointToHostId()).thenReturn(endpointToHostIDMap);
    when(jmx.getTokens()).thenReturn(THREE_TOKENS);
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
        .thenAnswer((invocation) -> {
          final int repairNumber = repairNumberCounter.getAndIncrement();
          RepairStatusHandler handler = invocation.getArgument(5);

              // Exécuter dans un thread séparé pour simuler le comportement asynchrone
              Executors.newSingleThreadExecutor()
                  .submit(
                      () -> {
                        try {
                          handler.handle(
                              repairNumber, Optional.of(ProgressEventType.START), null, jmx);
                          Thread.sleep(100); // Add small delay between
                          // notifications
                          handler.handle(
                              repairNumber, Optional.of(ProgressEventType.SUCCESS), null, jmx);
                          Thread.sleep(100); // Add small delay between
                          // notifications
                          handler.handle(
                              repairNumber, Optional.of(ProgressEventType.COMPLETE), null, jmx);
                        } catch (InterruptedException e) {
                          Thread.currentThread().interrupt();
                        } catch (RuntimeException e) {
                          throw new RuntimeException("Error in mock handler execution", e);
                        }
                      });

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
    when(jmx.getRangeToEndpointMap(anyString())).thenReturn(twoNodeClusterAfterBootstrap());
    context.repairManager.resumeRunningRepairRuns();
    // The repair run should succeed despite the topology change. Although token ranges change,
    // they will become larger and still entirely enclose each previously calculated segment.
    await()
        .with()
        .atMost(20, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              RepairRun repairRun = storage.getRepairRunDao().getRepairRun(runId).get();
              assertEquals(
                  "Repair run " + runId + " did not reach DONE state within timeout",
                  RepairRun.RunState.DONE,
                  repairRun.getRunState());
            });
  }

}