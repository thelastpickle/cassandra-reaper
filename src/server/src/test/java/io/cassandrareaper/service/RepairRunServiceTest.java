/*
 * Copyright 2017-2017 Spotify AB
 * Copyright 2017-2018 The Last Pickle Ltd
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.cassandrareaper.AppContext;
import io.cassandrareaper.ReaperApplicationConfiguration;
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.Node;
import io.cassandrareaper.core.RepairSegment;
import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.core.Segment;
import io.cassandrareaper.core.Table;
import io.cassandrareaper.crypto.NoopCrypotograph;
import io.cassandrareaper.management.ClusterFacade;
import io.cassandrareaper.management.ICassandraManagementProxy;
import io.cassandrareaper.management.jmx.CassandraManagementProxyTest;
import io.cassandrareaper.management.jmx.JmxCassandraManagementProxy;
import io.cassandrareaper.management.jmx.JmxManagementConnectionFactory;
import io.cassandrareaper.storage.IStorageDao;
import io.cassandrareaper.storage.MemoryStorageFacade;
import io.cassandrareaper.storage.cluster.IClusterDao;
import io.cassandrareaper.storage.repairrun.IRepairRunDao;
import java.math.BigInteger;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.commons.lang3.RandomStringUtils;
import org.joda.time.DateTimeUtils;
import org.junit.After;
import org.junit.Test;
import org.mockito.Mockito;

public final class RepairRunServiceTest {

  @After
  public void tearDown() {
    DateTimeUtils.setCurrentMillisSystem();
  }

  @Test
  public void buildEndpointToRangeMapTest() {
    Map<List<String>, List<String>> rangeToEndpoint = Maps.newHashMap();
    rangeToEndpoint.put(Arrays.asList("1", "2"), Arrays.asList("node1", "node2", "node3"));
    rangeToEndpoint.put(Arrays.asList("2", "4"), Arrays.asList("node1", "node2", "node3"));
    rangeToEndpoint.put(Arrays.asList("4", "6"), Arrays.asList("node1"));
    rangeToEndpoint.put(Arrays.asList("6", "8"), Arrays.asList("node1", "node2"));

    Map<String, List<RingRange>> endpointToRangeMap =
        RepairRunService.buildEndpointToRangeMap(rangeToEndpoint);

    assertEquals(endpointToRangeMap.entrySet().size(), 3);
    assertEquals(endpointToRangeMap.get("node1").size(), 4);
    assertEquals(endpointToRangeMap.get("node2").size(), 3);
    assertEquals(endpointToRangeMap.get("node3").size(), 2);
  }

  @Test
  public void filterSegmentsByNodesTest() throws ReaperException {
    Map<List<String>, List<String>> rangeToEndpoint = Maps.newHashMap();
    rangeToEndpoint.put(Arrays.asList("1", "2"), Arrays.asList("node1", "node2", "node3"));
    rangeToEndpoint.put(Arrays.asList("2", "4"), Arrays.asList("node1", "node2", "node3"));
    rangeToEndpoint.put(Arrays.asList("4", "6"), Arrays.asList("node1"));
    rangeToEndpoint.put(Arrays.asList("6", "8"), Arrays.asList("node1", "node2"));

    List<Segment> segments =
        Arrays.asList(
            Segment.builder().withTokenRange(new RingRange("1", "2")).build(),
            Segment.builder().withTokenRange(new RingRange("2", "3")).build(),
            Segment.builder().withTokenRange(new RingRange("3", "4")).build(),
            Segment.builder().withTokenRange(new RingRange("4", "5")).build(),
            Segment.builder().withTokenRange(new RingRange("5", "6")).build(),
            Segment.builder().withTokenRange(new RingRange("6", "8")).build());

    final RepairUnit repairUnit1 = mock(RepairUnit.class);
    when(repairUnit1.getNodes()).thenReturn(new HashSet<String>(Arrays.asList("node3", "node2")));

    final RepairUnit repairUnit2 = mock(RepairUnit.class);
    when(repairUnit2.getNodes()).thenReturn(new HashSet<String>(Arrays.asList("node1")));

    final RepairUnit repairUnit3 = mock(RepairUnit.class);
    when(repairUnit3.getNodes()).thenReturn(new HashSet<String>(Arrays.asList("node3")));

    List<Segment> filtered =
        RepairRunService.filterSegmentsByNodes(
            segments, repairUnit1, RepairRunService.buildEndpointToRangeMap(rangeToEndpoint));

    assertEquals(filtered.size(), 4);

    filtered =
        RepairRunService.filterSegmentsByNodes(
            segments, repairUnit2, RepairRunService.buildEndpointToRangeMap(rangeToEndpoint));
    assertEquals(filtered.size(), 6);

    filtered =
        RepairRunService.filterSegmentsByNodes(
            segments, repairUnit3, RepairRunService.buildEndpointToRangeMap(rangeToEndpoint));
    assertEquals(filtered.size(), 3);

    final RepairUnit repairUnitWithNoNodes = mock(RepairUnit.class);
    when(repairUnitWithNoNodes.getNodes()).thenReturn(new HashSet<String>());

    filtered =
        RepairRunService.filterSegmentsByNodes(
            segments,
            repairUnitWithNoNodes,
            RepairRunService.buildEndpointToRangeMap(rangeToEndpoint));
    assertEquals(filtered.size(), 6);
  }

  @Test
  public void computeGlobalSegmentCountSubdivisionOkTest() {

    Map<List<String>, List<String>> rangeToEndpoint = Maps.newHashMap();
    for (int i = 0; i < 10; i++) {
      rangeToEndpoint.put(
          Arrays.asList(i + "", (i + 1) + ""), Arrays.asList("node1", "node2", "node3"));
    }

    Map<String, List<RingRange>> endpointToRange = Maps.newHashMap();
    endpointToRange.put("node1", Lists.newArrayList());
    endpointToRange.put("node2", Lists.newArrayList());
    endpointToRange.put("node3", Lists.newArrayList());

    assertEquals(30, RepairRunService.computeGlobalSegmentCount(10, endpointToRange));
  }

  @Test
  public void computeGlobalSegmentCountSubdivisionNotOkTest() {

    Map<List<String>, List<String>> rangeToEndpoint = Maps.newHashMap();
    for (int i = 0; i < 60; i++) {
      rangeToEndpoint.put(
          Arrays.asList(i + "", (i + 1) + ""), Arrays.asList("node1", "node2", "node3"));
    }

    Map<String, List<RingRange>> endpointToRange = Maps.newHashMap();
    endpointToRange.put("node1", Lists.newArrayList());
    endpointToRange.put("node2", Lists.newArrayList());
    endpointToRange.put("node3", Lists.newArrayList());

    assertEquals(30, RepairRunService.computeGlobalSegmentCount(10, endpointToRange));
  }

  @Test
  public void computeGlobalSegmentCountSingleTokenPerNodeTest() {

    Map<List<String>, List<String>> rangeToEndpoint = Maps.newHashMap();
    for (int i = 0; i < 3; i++) {
      rangeToEndpoint.put(
          Arrays.asList(i + "", (i + 1) + ""), Arrays.asList("node1", "node2", "node3"));
    }

    Map<String, List<RingRange>> endpointToRange = Maps.newHashMap();
    endpointToRange.put("node1", Lists.newArrayList());
    endpointToRange.put("node2", Lists.newArrayList());
    endpointToRange.put("node3", Lists.newArrayList());

    assertEquals(192, RepairRunService.computeGlobalSegmentCount(0, endpointToRange));
  }

  @Test
  public void computeGlobalSegmentCount256TokenPerNodeTest() {

    Map<List<String>, List<String>> rangeToEndpoint = Maps.newHashMap();
    for (int i = 0; i < 768; i++) {
      rangeToEndpoint.put(
          Arrays.asList(i + "", (i + 1) + ""), Arrays.asList("node1", "node2", "node3"));
    }

    Map<String, List<RingRange>> endpointToRange = Maps.newHashMap();
    endpointToRange.put("node1", Lists.newArrayList());
    endpointToRange.put("node2", Lists.newArrayList());
    endpointToRange.put("node3", Lists.newArrayList());

    assertEquals(192, RepairRunService.computeGlobalSegmentCount(0, endpointToRange));
  }

  @Test
  public void buildReplicasToRangeMapTest() {
    Map<List<String>, List<String>> rangeToEndpoint = Maps.newHashMap();
    rangeToEndpoint.put(Arrays.asList("1", "2"), Arrays.asList("node1", "node2", "node3"));
    rangeToEndpoint.put(Arrays.asList("2", "4"), Arrays.asList("node1", "node2", "node3"));
    rangeToEndpoint.put(Arrays.asList("4", "6"), Arrays.asList("node1"));
    rangeToEndpoint.put(Arrays.asList("6", "8"), Arrays.asList("node1", "node2"));
    rangeToEndpoint.put(Arrays.asList("9", "10"), Arrays.asList("node1", "node2"));
    rangeToEndpoint.put(Arrays.asList("11", "12"), Arrays.asList("node2", "node3", "node1"));

    Map<List<String>, List<RingRange>> replicasToRangeMap =
        RepairRunService.buildReplicasToRangeMap(rangeToEndpoint);

    assertEquals(replicasToRangeMap.entrySet().size(), 3);
    assertEquals(replicasToRangeMap.get(Arrays.asList("node1", "node2", "node3")).size(), 3);
    assertEquals(replicasToRangeMap.get(Arrays.asList("node1")).size(), 1);
    assertEquals(replicasToRangeMap.get(Arrays.asList("node1", "node2")).size(), 2);
  }

  @Test(expected = ReaperException.class)
  public void generateSegmentsFail1Test() throws ReaperException, UnknownHostException {
    Cluster cluster =
        Cluster.builder()
            .withName("test_" + RandomStringUtils.randomAlphabetic(12))
            .withSeedHosts(ImmutableSet.of("127.0.0.1", "127.0.0.2", "127.0.0.3"))
            .withState(Cluster.State.ACTIVE)
            .withPartitioner("Murmur3Partitioner")
            .build();
    final String KS_NAME = "reaper";
    final Set<String> CF_NAMES = Sets.newHashSet("reaper");
    final boolean INCREMENTAL_REPAIR = false;
    final Set<String> NODES = Sets.newHashSet("127.0.0.1", "127.0.0.2", "127.0.0.3");
    final Set<String> DATACENTERS = Collections.emptySet();
    final Set<String> BLACKLISTED_TABLES = Collections.emptySet();
    final long TIME_RUN = 41L;
    final int REPAIR_THREAD_COUNT = 1;
    final int segmentTimeout = 30;
    final List<BigInteger> TOKENS =
        Lists.newArrayList(
            BigInteger.valueOf(0L), BigInteger.valueOf(100L), BigInteger.valueOf(200L));
    final IStorageDao storage = new MemoryStorageFacade();

    storage.getClusterDao().addCluster(cluster);

    RepairUnit cf =
        storage
            .getRepairUnitDao()
            .addRepairUnit(
                RepairUnit.builder()
                    .clusterName(cluster.getName())
                    .keyspaceName(KS_NAME)
                    .columnFamilies(CF_NAMES)
                    .incrementalRepair(INCREMENTAL_REPAIR)
                    .subrangeIncrementalRepair(INCREMENTAL_REPAIR)
                    .nodes(NODES)
                    .datacenters(DATACENTERS)
                    .blacklistedTables(BLACKLISTED_TABLES)
                    .repairThreadCount(REPAIR_THREAD_COUNT)
                    .timeout(segmentTimeout));
    DateTimeUtils.setCurrentMillisFixed(TIME_RUN);

    AppContext context = new AppContext();
    context.storage = storage;
    context.config = new ReaperApplicationConfiguration();
    context.schedulingManager = mock(SchedulingManager.class);
    doNothing().when(context.schedulingManager).maybeRegisterRepairRunCompleted(any());
    final JmxCassandraManagementProxy jmx = CassandraManagementProxyTest.mockJmxProxyImpl();
    when(jmx.getClusterName()).thenReturn(cluster.getName());
    when(jmx.getRangeToEndpointMap(anyString())).thenReturn(RepairRunnerTest.sixNodeCluster());
    EndpointSnitchInfoMBean endpointSnitchInfoMBean = mock(EndpointSnitchInfoMBean.class);
    when(endpointSnitchInfoMBean.getDatacenter()).thenReturn("dc1");
    try {
      when(endpointSnitchInfoMBean.getDatacenter(anyString())).thenReturn("dc1");
    } catch (UnknownHostException ex) {
      throw new AssertionError(ex);
    }

    ClusterFacade clusterFacade = mock(ClusterFacade.class);
    when(clusterFacade.connect(any(Cluster.class))).thenReturn(jmx);
    when(clusterFacade.nodeIsDirectlyAccessible(any(), any())).thenReturn(true);
    when(clusterFacade.tokenRangeToEndpoint(any(), anyString(), any()))
        .thenReturn(Lists.newArrayList(NODES));
    when(clusterFacade.getRangeToEndpointMap(any(), anyString()))
        .thenThrow(new ReaperException("fail"));
    when(clusterFacade.getCassandraVersion(any())).thenReturn("3.11.6");
    when(clusterFacade.getTokens(any())).thenReturn(TOKENS);

    context.managementConnectionFactory =
        new JmxManagementConnectionFactory(context, new NoopCrypotograph()) {
          @Override
          protected JmxCassandraManagementProxy connectImpl(Node host) throws ReaperException {
            return jmx;
          }
        };

    RepairRunService repairRunService =
        RepairRunService.create(context, () -> clusterFacade, context.storage.getRepairRunDao());

    RepairUnit unit =
        RepairUnit.builder()
            .clusterName(cluster.getName())
            .keyspaceName("test")
            .blacklistedTables(Sets.newHashSet("table1"))
            .incrementalRepair(false)
            .subrangeIncrementalRepair(false)
            .repairThreadCount(4)
            .timeout(segmentTimeout)
            .build(Uuids.timeBased());
    List<Segment> segments = repairRunService.generateSegments(cluster, 10, unit);
    assertEquals(32, segments.size());
    assertEquals(3, segments.get(0).getReplicas().keySet().size());
    assertEquals("dc1", segments.get(0).getReplicas().get("127.0.0.1"));
  }

  @Test
  public void generateSegmentsTest() throws ReaperException, UnknownHostException {
    Cluster cluster =
        Cluster.builder()
            .withName("test_" + RandomStringUtils.randomAlphabetic(12))
            .withSeedHosts(ImmutableSet.of("127.0.0.1", "127.0.0.2", "127.0.0.3"))
            .withState(Cluster.State.ACTIVE)
            .withPartitioner("Murmur3Partitioner")
            .build();
    final String KS_NAME = "reaper";
    final Set<String> CF_NAMES = Sets.newHashSet("reaper");
    final boolean INCREMENTAL_REPAIR = false;
    final Set<String> NODES = Sets.newHashSet("127.0.0.1", "127.0.0.2", "127.0.0.3");
    final Set<String> DATACENTERS = Collections.emptySet();
    final Set<String> BLACKLISTED_TABLES = Collections.emptySet();
    final long TIME_RUN = 41L;
    final int REPAIR_THREAD_COUNT = 1;
    final int segmentTimeout = 30;
    final List<BigInteger> TOKENS =
        Lists.newArrayList(
            BigInteger.valueOf(0L), BigInteger.valueOf(100L), BigInteger.valueOf(200L));
    final IStorageDao storage = new MemoryStorageFacade();

    storage.getClusterDao().addCluster(cluster);

    RepairUnit cf =
        storage
            .getRepairUnitDao()
            .addRepairUnit(
                RepairUnit.builder()
                    .clusterName(cluster.getName())
                    .keyspaceName(KS_NAME)
                    .columnFamilies(CF_NAMES)
                    .incrementalRepair(INCREMENTAL_REPAIR)
                    .subrangeIncrementalRepair(INCREMENTAL_REPAIR)
                    .nodes(NODES)
                    .datacenters(DATACENTERS)
                    .blacklistedTables(BLACKLISTED_TABLES)
                    .repairThreadCount(REPAIR_THREAD_COUNT)
                    .timeout(segmentTimeout));
    DateTimeUtils.setCurrentMillisFixed(TIME_RUN);

    AppContext context = new AppContext();
    context.storage = storage;
    context.config = new ReaperApplicationConfiguration();
    context.schedulingManager = mock(SchedulingManager.class);
    doNothing().when(context.schedulingManager).maybeRegisterRepairRunCompleted(any());
    final Semaphore mutex = new Semaphore(0);
    final JmxCassandraManagementProxy jmx = CassandraManagementProxyTest.mockJmxProxyImpl();
    when(jmx.getClusterName()).thenReturn(cluster.getName());
    when(jmx.getRangeToEndpointMap(anyString())).thenReturn(RepairRunnerTest.sixNodeCluster());
    EndpointSnitchInfoMBean endpointSnitchInfoMBean = mock(EndpointSnitchInfoMBean.class);
    when(endpointSnitchInfoMBean.getDatacenter()).thenReturn("dc1");
    try {
      when(endpointSnitchInfoMBean.getDatacenter(anyString())).thenReturn("dc1");
    } catch (UnknownHostException ex) {
      throw new AssertionError(ex);
    }

    ClusterFacade clusterFacade = mock(ClusterFacade.class);
    when(clusterFacade.connect(any(Cluster.class))).thenReturn(jmx);
    when(clusterFacade.nodeIsDirectlyAccessible(any(), any())).thenReturn(true);
    when(clusterFacade.tokenRangeToEndpoint(any(), anyString(), any()))
        .thenReturn(Lists.newArrayList(NODES));
    when(clusterFacade.getRangeToEndpointMap(any(), anyString()))
        .thenReturn(
            (Map) ImmutableMap.of(Lists.newArrayList("0", "100"), Lists.newArrayList(NODES)));
    when(clusterFacade.getCassandraVersion(any())).thenReturn("3.11.6");
    when(clusterFacade.getTokens(any())).thenReturn(TOKENS);

    context.managementConnectionFactory =
        new JmxManagementConnectionFactory(context, new NoopCrypotograph()) {
          @Override
          protected JmxCassandraManagementProxy connectImpl(Node host) throws ReaperException {
            return jmx;
          }
        };

    RepairRunService repairRunService =
        RepairRunService.create(context, () -> clusterFacade, context.storage.getRepairRunDao());

    RepairUnit unit =
        RepairUnit.builder()
            .clusterName(cluster.getName())
            .keyspaceName("test")
            .blacklistedTables(Sets.newHashSet("table1"))
            .incrementalRepair(false)
            .subrangeIncrementalRepair(false)
            .repairThreadCount(4)
            .timeout(segmentTimeout)
            .build(Uuids.timeBased());
    List<Segment> segments = repairRunService.generateSegments(cluster, 10, unit);
    assertEquals(32, segments.size());
    assertEquals(3, segments.get(0).getReplicas().keySet().size());
    assertEquals("dc1", segments.get(0).getReplicas().get("127.0.0.1"));
  }

  @Test(expected = ReaperException.class)
  public void failRepairRunCreationTest() throws ReaperException, UnknownHostException {
    final String KS_NAME = "reaper";
    final Set<String> CF_NAMES = Sets.newHashSet("reaper");
    final boolean INCREMENTAL_REPAIR = false;
    final boolean SUBRANGE_INCREMENTAL_REPAIR = false;
    final Set<String> NODES = Sets.newHashSet("127.0.0.1", "127.0.0.2", "127.0.0.3");
    final Set<String> DATACENTERS = Collections.emptySet();
    final Set<String> BLACKLISTED_TABLES = Collections.emptySet();
    final int REPAIR_THREAD_COUNT = 1;
    final int segmentTimeout = 30;
    final List<BigInteger> TOKENS =
        Lists.newArrayList(
            BigInteger.valueOf(0L), BigInteger.valueOf(100L), BigInteger.valueOf(200L));
    final IStorageDao storage = mock(IStorageDao.class);
    IRepairRunDao mockedRepairRunDao = mock(IRepairRunDao.class);
    when(storage.getRepairRunDao()).thenReturn(mockedRepairRunDao);
    when(mockedRepairRunDao.addRepairRun(any(), any())).thenReturn(null);
    AppContext context = new AppContext();
    context.storage = storage;
    context.config = new ReaperApplicationConfiguration();
    context.schedulingManager = mock(SchedulingManager.class);
    doNothing().when(context.schedulingManager).maybeRegisterRepairRunCompleted(any());
    final ICassandraManagementProxy jmx = CassandraManagementProxyTest.mockJmxProxyImpl();
    Cluster cluster =
        Cluster.builder()
            .withName("test_" + RandomStringUtils.randomAlphabetic(12))
            .withSeedHosts(ImmutableSet.of("127.0.0.1", "127.0.0.2", "127.0.0.3"))
            .withState(Cluster.State.ACTIVE)
            .withPartitioner("Murmur3Partitioner")
            .build();
    when(jmx.getClusterName()).thenReturn(cluster.getName());
    when(jmx.getRangeToEndpointMap(anyString())).thenReturn(RepairRunnerTest.sixNodeCluster());

    ClusterFacade clusterFacade = mock(ClusterFacade.class);
    when(clusterFacade.connect(any(Cluster.class))).thenReturn(jmx);
    when(clusterFacade.nodeIsDirectlyAccessible(any(), any())).thenReturn(true);
    when(clusterFacade.tokenRangeToEndpoint(any(), anyString(), any()))
        .thenReturn(Lists.newArrayList(NODES));
    when(clusterFacade.getRangeToEndpointMap(any(), anyString()))
        .thenReturn(
            (Map) ImmutableMap.of(Lists.newArrayList("0", "100"), Lists.newArrayList(NODES)));
    when(clusterFacade.getCassandraVersion(any())).thenReturn("3.11.6");
    when(clusterFacade.getTokens(any())).thenReturn(TOKENS);

    RepairRunService repairRunService =
        RepairRunService.create(context, () -> clusterFacade, context.storage.getRepairRunDao());

    RepairUnit repairUnit =
        RepairUnit.builder()
            .clusterName(cluster.getName())
            .keyspaceName(KS_NAME)
            .columnFamilies(CF_NAMES)
            .incrementalRepair(INCREMENTAL_REPAIR)
            .subrangeIncrementalRepair(SUBRANGE_INCREMENTAL_REPAIR)
            .nodes(NODES)
            .datacenters(DATACENTERS)
            .blacklistedTables(BLACKLISTED_TABLES)
            .repairThreadCount(REPAIR_THREAD_COUNT)
            .timeout(segmentTimeout)
            .build(Uuids.timeBased());

    repairRunService.registerRepairRun(
        cluster,
        repairUnit,
        Optional.of("cause"),
        "owner",
        10,
        RepairParallelism.SEQUENTIAL,
        new Double(1),
        false);
  }

  @Test(expected = ReaperException.class)
  public void failIncrRepairRunCreationTest() throws ReaperException, UnknownHostException {
    final String KS_NAME = "reaper";
    final Set<String> CF_NAMES = Sets.newHashSet("reaper");
    final boolean INCREMENTAL_REPAIR = false;
    final boolean SUBRANGE_INCREMENTAL_REPAIR = false;
    final Set<String> NODES = Sets.newHashSet("127.0.0.1", "127.0.0.2", "127.0.0.3");
    final Set<String> DATACENTERS = Collections.emptySet();
    final Set<String> BLACKLISTED_TABLES = Collections.emptySet();
    final int REPAIR_THREAD_COUNT = 1;
    final int segmentTimeout = 30;
    final List<BigInteger> TOKENS =
        Lists.newArrayList(
            BigInteger.valueOf(0L), BigInteger.valueOf(100L), BigInteger.valueOf(200L));
    final IStorageDao storage = mock(IStorageDao.class);
    IRepairRunDao mockedRepairRunDao = mock(IRepairRunDao.class);
    when(storage.getRepairRunDao()).thenReturn(mockedRepairRunDao);
    when(mockedRepairRunDao.addRepairRun(any(), any())).thenReturn(null);
    AppContext context = new AppContext();
    context.storage = storage;
    context.config = new ReaperApplicationConfiguration();
    context.schedulingManager = mock(SchedulingManager.class);
    doNothing().when(context.schedulingManager).maybeRegisterRepairRunCompleted(any());
    final ICassandraManagementProxy jmx = CassandraManagementProxyTest.mockJmxProxyImpl();
    Cluster cluster =
        Cluster.builder()
            .withName("test_" + RandomStringUtils.randomAlphabetic(12))
            .withSeedHosts(ImmutableSet.of("127.0.0.1", "127.0.0.2", "127.0.0.3"))
            .withState(Cluster.State.ACTIVE)
            .withPartitioner("Murmur3Partitioner")
            .build();
    when(jmx.getClusterName()).thenReturn(cluster.getName());
    when(jmx.getRangeToEndpointMap(anyString())).thenReturn(RepairRunnerTest.sixNodeCluster());

    ClusterFacade clusterFacade = mock(ClusterFacade.class);
    when(clusterFacade.connect(any(Cluster.class))).thenReturn(jmx);
    when(clusterFacade.nodeIsDirectlyAccessible(any(), any())).thenReturn(true);
    when(clusterFacade.tokenRangeToEndpoint(any(), anyString(), any()))
        .thenReturn(Lists.newArrayList(NODES));
    when(clusterFacade.getRangeToEndpointMap(any(), anyString()))
        .thenReturn(
            (Map) ImmutableMap.of(Lists.newArrayList("0", "100"), Lists.newArrayList(NODES)));
    when(clusterFacade.getCassandraVersion(any())).thenReturn("3.11.6");
    when(clusterFacade.getTokens(any())).thenReturn(TOKENS);
    when(clusterFacade.getEndpointToHostId(any(Cluster.class))).thenReturn(Collections.emptyMap());
    Map<String, String> endpointToHostIDMap = new HashMap<String, String>();
    endpointToHostIDMap.put("127.0.0.1", UUID.randomUUID().toString());
    endpointToHostIDMap.put("127.0.0.2", UUID.randomUUID().toString());
    endpointToHostIDMap.put("127.0.0.3", UUID.randomUUID().toString());
    when(clusterFacade.getEndpointToHostId(any(Cluster.class))).thenReturn(endpointToHostIDMap);

    RepairRunService repairRunService =
        RepairRunService.create(context, () -> clusterFacade, context.storage.getRepairRunDao());

    RepairUnit repairUnit =
        RepairUnit.builder()
            .clusterName(cluster.getName())
            .keyspaceName(KS_NAME)
            .columnFamilies(CF_NAMES)
            .incrementalRepair(INCREMENTAL_REPAIR)
            .subrangeIncrementalRepair(SUBRANGE_INCREMENTAL_REPAIR)
            .nodes(NODES)
            .datacenters(DATACENTERS)
            .blacklistedTables(BLACKLISTED_TABLES)
            .repairThreadCount(REPAIR_THREAD_COUNT)
            .incrementalRepair(true)
            .timeout(segmentTimeout)
            .build(Uuids.timeBased());

    repairRunService.registerRepairRun(
        cluster,
        repairUnit,
        Optional.of("cause"),
        "owner",
        10,
        RepairParallelism.SEQUENTIAL,
        new Double(1),
        false);
  }

  @Test
  public void getDCsByNodeForRepairSegmentPath1Test() throws ReaperException, UnknownHostException {
    final String KS_NAME = "reaper";
    final Set<String> CF_NAMES = Sets.newHashSet("reaper");
    final boolean INCREMENTAL_REPAIR = false;
    final boolean SUBRANGE_INCREMENTAL_REPAIR = false;
    final Set<String> NODES = Sets.newHashSet("127.0.0.1", "127.0.0.2", "127.0.0.3");
    final Set<String> DATACENTERS = Sets.newHashSet("dc1");
    final Set<String> BLACKLISTED_TABLES = Collections.emptySet();
    final int REPAIR_THREAD_COUNT = 1;
    final int segmentTimeout = 30;
    final IStorageDao storage = mock(IStorageDao.class);
    IRepairRunDao mockedRepairRunDao = mock(IRepairRunDao.class);
    when(mockedRepairRunDao.addRepairRun(any(), any())).thenReturn(null);
    when(storage.getRepairRunDao()).thenReturn(mockedRepairRunDao);
    AppContext context = new AppContext();
    context.storage = storage;
    Mockito.when(context.storage.getClusterDao()).thenReturn(Mockito.mock(IClusterDao.class));
    context.config = new ReaperApplicationConfiguration();
    context.schedulingManager = mock(SchedulingManager.class);
    doNothing().when(context.schedulingManager).maybeRegisterRepairRunCompleted(any());
    final JmxCassandraManagementProxy jmx = CassandraManagementProxyTest.mockJmxProxyImpl();
    context.managementConnectionFactory =
        new JmxManagementConnectionFactory(context, new NoopCrypotograph()) {
          @Override
          public JmxCassandraManagementProxy connectImpl(Node host) throws ReaperException {
            return jmx;
          }
        };
    EndpointSnitchInfoMBean endpointSnitchInfoMBean = mock(EndpointSnitchInfoMBean.class);
    when(endpointSnitchInfoMBean.getDatacenter()).thenReturn("dc1");
    try {
      when(endpointSnitchInfoMBean.getDatacenter(anyString())).thenReturn("dc1");
    } catch (UnknownHostException ex) {
      throw new AssertionError(ex);
    }

    Cluster cluster =
        Cluster.builder()
            .withName("test_" + RandomStringUtils.randomAlphabetic(12))
            .withSeedHosts(ImmutableSet.of("127.0.0.1", "127.0.0.2", "127.0.0.3"))
            .withState(Cluster.State.ACTIVE)
            .withPartitioner("Murmur3Partitioner")
            .build();
    when(jmx.getClusterName()).thenReturn(cluster.getName());
    when(jmx.getRangeToEndpointMap(anyString())).thenReturn(RepairRunnerTest.sixNodeCluster());

    RepairRunService repairRunService =
        RepairRunService.create(context, context.storage.getRepairRunDao());

    RepairUnit repairUnit =
        RepairUnit.builder()
            .clusterName(cluster.getName())
            .keyspaceName(KS_NAME)
            .columnFamilies(CF_NAMES)
            .incrementalRepair(INCREMENTAL_REPAIR)
            .subrangeIncrementalRepair(SUBRANGE_INCREMENTAL_REPAIR)
            .nodes(NODES)
            .datacenters(DATACENTERS)
            .blacklistedTables(BLACKLISTED_TABLES)
            .repairThreadCount(REPAIR_THREAD_COUNT)
            .incrementalRepair(true)
            .timeout(segmentTimeout)
            .build(Uuids.timeBased());
    RingRange range1 = new RingRange("1", "2");
    Segment segment = Segment.builder().withBaseRange(range1).withTokenRange(range1).build();
    Map<String, String> dcByNode =
        repairRunService.getDCsByNodeForRepairSegment(cluster, segment, KS_NAME, repairUnit);
    assertFalse("Didn't get dc by node map", dcByNode.keySet().isEmpty());
  }

  @Test(expected = ReaperException.class)
  public void getDCsByNodeForRepairSegmentFailPathTest()
      throws ReaperException, UnknownHostException {
    final String KS_NAME = "reaper";
    final Set<String> CF_NAMES = Sets.newHashSet("reaper");
    final boolean INCREMENTAL_REPAIR = false;
    final boolean SUBRANGE_INCREMENTAL_REPAIR = false;
    final Set<String> NODES = Sets.newHashSet("127.0.0.1", "127.0.0.2", "127.0.0.3");
    final Set<String> DATACENTERS = Sets.newHashSet("dc1");
    final Set<String> BLACKLISTED_TABLES = Collections.emptySet();
    final int REPAIR_THREAD_COUNT = 1;
    final int segmentTimeout = 30;
    final IStorageDao storage = mock(IStorageDao.class);
    IRepairRunDao mockedRepairRunDao = mock(IRepairRunDao.class);
    when(mockedRepairRunDao.addRepairRun(any(), any())).thenReturn(null);
    when(storage.getRepairRunDao()).thenReturn(mockedRepairRunDao);
    AppContext context = new AppContext();
    context.storage = storage;
    context.config = new ReaperApplicationConfiguration();
    context.schedulingManager = mock(SchedulingManager.class);
    doNothing().when(context.schedulingManager).maybeRegisterRepairRunCompleted(any());
    Mockito.when(context.storage.getClusterDao()).thenReturn(Mockito.mock(IClusterDao.class));
    final JmxCassandraManagementProxy jmx = CassandraManagementProxyTest.mockJmxProxyImpl();
    context.managementConnectionFactory =
        new JmxManagementConnectionFactory(context, new NoopCrypotograph()) {
          @Override
          public JmxCassandraManagementProxy connectImpl(Node host) throws ReaperException {
            return jmx;
          }
        };
    EndpointSnitchInfoMBean endpointSnitchInfoMBean = mock(EndpointSnitchInfoMBean.class);
    when(endpointSnitchInfoMBean.getDatacenter()).thenThrow(new RuntimeException("fail"));
    try {
      when(endpointSnitchInfoMBean.getDatacenter(anyString()))
          .thenThrow(new RuntimeException("fail"));
    } catch (UnknownHostException ex) {
      throw new AssertionError(ex);
    }

    Cluster cluster =
        Cluster.builder()
            .withName("test_" + RandomStringUtils.randomAlphabetic(12))
            .withSeedHosts(ImmutableSet.of("127.0.0.1", "127.0.0.2", "127.0.0.3"))
            .withState(Cluster.State.ACTIVE)
            .withPartitioner("Murmur3Partitioner")
            .build();
    when(jmx.getClusterName()).thenReturn(cluster.getName());

    ClusterFacade clusterFacade = mock(ClusterFacade.class);
    when(clusterFacade.connect(any(Cluster.class))).thenReturn(jmx);
    when(clusterFacade.tokenRangeToEndpoint(any(), any(), any()))
        .thenThrow(new RuntimeException("fail"));
    RepairRunService repairRunService =
        RepairRunService.create(context, () -> clusterFacade, context.storage.getRepairRunDao());

    RepairUnit repairUnit =
        RepairUnit.builder()
            .clusterName(cluster.getName())
            .keyspaceName(KS_NAME)
            .columnFamilies(CF_NAMES)
            .incrementalRepair(INCREMENTAL_REPAIR)
            .subrangeIncrementalRepair(SUBRANGE_INCREMENTAL_REPAIR)
            .nodes(NODES)
            .datacenters(DATACENTERS)
            .blacklistedTables(BLACKLISTED_TABLES)
            .repairThreadCount(REPAIR_THREAD_COUNT)
            .incrementalRepair(true)
            .timeout(segmentTimeout)
            .build(Uuids.timeBased());
    RingRange range1 = new RingRange("1", "2");
    Segment segment = Segment.builder().withBaseRange(range1).withTokenRange(range1).build();
    repairRunService.getDCsByNodeForRepairSegment(cluster, segment, KS_NAME, repairUnit);
  }

  @Test
  public void createRepairSegmentsForIncrementalRepairTest() throws ReaperException {
    final String KS_NAME = "reaper";
    final Set<String> CF_NAMES = Sets.newHashSet("reaper");
    final boolean INCREMENTAL_REPAIR = false;
    final boolean SUBRANGE_INCREMENTAL_REPAIR = false;
    final Set<String> NODES = Sets.newHashSet("127.0.0.1", "127.0.0.2", "127.0.0.3");
    final Set<String> DATACENTERS = Sets.newHashSet("dc1");
    final Set<String> BLACKLISTED_TABLES = Collections.emptySet();
    final int REPAIR_THREAD_COUNT = 1;
    final int segmentTimeout = 30;
    final Cluster cluster =
        Cluster.builder()
            .withName("test_" + RandomStringUtils.randomAlphabetic(12))
            .withSeedHosts(ImmutableSet.of("127.0.0.1", "127.0.0.2", "127.0.0.3"))
            .withState(Cluster.State.ACTIVE)
            .withPartitioner("Murmur3Partitioner")
            .build();

    Map<String, RingRange> nodes = Maps.newHashMap();
    nodes.put("127.0.0.1", new RingRange("1", "2"));
    nodes.put("127.0.0.2", new RingRange("3", "4"));

    Map<String, String> endpointToHostIDMap = new HashMap<String, String>();
    endpointToHostIDMap.put("127.0.0.1", UUID.randomUUID().toString());
    endpointToHostIDMap.put("127.0.0.2", UUID.randomUUID().toString());
    endpointToHostIDMap.put("127.0.0.3", UUID.randomUUID().toString());
    ClusterFacade clusterFacade = mock(ClusterFacade.class);
    when(clusterFacade.getEndpointToHostId(any(Cluster.class))).thenReturn(endpointToHostIDMap);
    RepairUnit repairUnit =
        RepairUnit.builder()
            .clusterName(cluster.getName())
            .keyspaceName(KS_NAME)
            .columnFamilies(CF_NAMES)
            .incrementalRepair(INCREMENTAL_REPAIR)
            .subrangeIncrementalRepair(SUBRANGE_INCREMENTAL_REPAIR)
            .nodes(NODES)
            .datacenters(DATACENTERS)
            .blacklistedTables(BLACKLISTED_TABLES)
            .repairThreadCount(REPAIR_THREAD_COUNT)
            .incrementalRepair(true)
            .timeout(segmentTimeout)
            .build(Uuids.timeBased());
    List<RepairSegment.Builder> segmentBuilders =
        RepairRunService.createRepairSegmentsForIncrementalRepair(
            nodes, repairUnit, cluster, clusterFacade);
    assertEquals("Not enough segment builders were created", 2, segmentBuilders.size());
  }

  @Test(expected = IllegalArgumentException.class)
  public void getTableNamesBasedOnParamFailTest() throws ReaperException {
    final IStorageDao storage = mock(IStorageDao.class);
    AppContext context = new AppContext();
    context.storage = storage;
    context.config = new ReaperApplicationConfiguration();
    context.schedulingManager = mock(SchedulingManager.class);
    doNothing().when(context.schedulingManager).maybeRegisterRepairRunCompleted(any());
    Cluster cluster =
        Cluster.builder()
            .withName("test_" + RandomStringUtils.randomAlphabetic(12))
            .withSeedHosts(ImmutableSet.of("127.0.0.1", "127.0.0.2", "127.0.0.3"))
            .withState(Cluster.State.ACTIVE)
            .withPartitioner("Murmur3Partitioner")
            .build();

    ClusterFacade clusterFacade = mock(ClusterFacade.class);
    when(clusterFacade.getTablesForKeyspace(any(Cluster.class), any()))
        .thenReturn(Collections.emptySet());
    RepairRunService repairRunService =
        RepairRunService.create(context, () -> clusterFacade, context.storage.getRepairRunDao());
    repairRunService.getTableNamesBasedOnParam(cluster, "keyspace", Optional.empty());
  }

  @Test(expected = IllegalArgumentException.class)
  public void getTableNamesBasedOnParamNoMatchTest() throws ReaperException {
    final IStorageDao storage = mock(IStorageDao.class);
    AppContext context = new AppContext();
    context.storage = storage;
    context.config = new ReaperApplicationConfiguration();
    context.schedulingManager = mock(SchedulingManager.class);
    doNothing().when(context.schedulingManager).maybeRegisterRepairRunCompleted(any());
    Cluster cluster =
        Cluster.builder()
            .withName("test_" + RandomStringUtils.randomAlphabetic(12))
            .withSeedHosts(ImmutableSet.of("127.0.0.1", "127.0.0.2", "127.0.0.3"))
            .withState(Cluster.State.ACTIVE)
            .withPartitioner("Murmur3Partitioner")
            .build();

    ClusterFacade clusterFacade = mock(ClusterFacade.class);
    when(clusterFacade.getTablesForKeyspace(any(Cluster.class), any()))
        .thenReturn(Sets.newHashSet(Table.builder().withName("table1").build()));
    RepairRunService repairRunService =
        RepairRunService.create(context, () -> clusterFacade, context.storage.getRepairRunDao());
    repairRunService.getTableNamesBasedOnParam(cluster, "keyspace", Optional.of("table2"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void getNodesToRepairBasedOnParamEmptyFailTest() throws ReaperException {
    final IStorageDao storage = mock(IStorageDao.class);
    AppContext context = new AppContext();
    context.storage = storage;
    context.config = new ReaperApplicationConfiguration();
    context.schedulingManager = mock(SchedulingManager.class);
    doNothing().when(context.schedulingManager).maybeRegisterRepairRunCompleted(any());
    Cluster cluster =
        Cluster.builder()
            .withName("test_" + RandomStringUtils.randomAlphabetic(12))
            .withSeedHosts(ImmutableSet.of("127.0.0.1", "127.0.0.2", "127.0.0.3"))
            .withState(Cluster.State.ACTIVE)
            .withPartitioner("Murmur3Partitioner")
            .build();

    ClusterFacade clusterFacade = mock(ClusterFacade.class);
    when(clusterFacade.getEndpointToHostId(any(Cluster.class))).thenReturn(Collections.emptyMap());
    RepairRunService repairRunService =
        RepairRunService.create(context, () -> clusterFacade, context.storage.getRepairRunDao());
    repairRunService.getNodesToRepairBasedOnParam(cluster, Optional.empty());
  }

  @Test(expected = IllegalArgumentException.class)
  public void getNodesToRepairBasedOnParamNoMatchFailTest() throws ReaperException {
    final IStorageDao storage = mock(IStorageDao.class);
    AppContext context = new AppContext();
    context.storage = storage;
    context.config = new ReaperApplicationConfiguration();
    context.schedulingManager = mock(SchedulingManager.class);
    doNothing().when(context.schedulingManager).maybeRegisterRepairRunCompleted(any());
    Cluster cluster =
        Cluster.builder()
            .withName("test_" + RandomStringUtils.randomAlphabetic(12))
            .withSeedHosts(ImmutableSet.of("127.0.0.1", "127.0.0.2", "127.0.0.3"))
            .withState(Cluster.State.ACTIVE)
            .withPartitioner("Murmur3Partitioner")
            .build();

    Map<String, String> endpointToHostMap = Maps.newHashMap();
    endpointToHostMap.put("127.0.0.4", "whatever");
    ClusterFacade clusterFacade = mock(ClusterFacade.class);
    when(clusterFacade.getEndpointToHostId(any(Cluster.class))).thenReturn(endpointToHostMap);
    RepairRunService repairRunService =
        RepairRunService.create(context, () -> clusterFacade, context.storage.getRepairRunDao());
    repairRunService.getNodesToRepairBasedOnParam(cluster, Optional.of("127.0.0.1"));
  }

  @Test(expected = ReaperException.class)
  public void getClusterNodesFailTest() throws ReaperException {
    final IStorageDao storage = mock(IStorageDao.class);
    IRepairRunDao mockedRepairRunDao = mock(IRepairRunDao.class);
    when(storage.getRepairRunDao()).thenReturn(mockedRepairRunDao);
    AppContext context = new AppContext();
    context.storage = storage;
    Map<String, String> endpointToHostMap = Maps.newHashMap();
    endpointToHostMap.put("127.0.0.4", "whatever");
    ClusterFacade clusterFacade = mock(ClusterFacade.class);
    when(clusterFacade.getRangeToEndpointMap(any(Cluster.class), any()))
        .thenThrow(new ReaperException("fail"));
    RepairUnit repairUnit = mock(RepairUnit.class);
    when(repairUnit.getKeyspaceName()).thenReturn("keyspace");
    Cluster cluster =
        Cluster.builder()
            .withName("test_" + RandomStringUtils.randomAlphabetic(12))
            .withSeedHosts(ImmutableSet.of("127.0.0.1", "127.0.0.2", "127.0.0.3"))
            .withState(Cluster.State.ACTIVE)
            .withPartitioner("Murmur3Partitioner")
            .build();
    RepairRunService repairRunService =
        RepairRunService.create(context, () -> clusterFacade, context.storage.getRepairRunDao());
    repairRunService.getClusterNodes(cluster, repairUnit);
  }

  @Test
  public void getDatacentersToRepairBasedOnParamTest() throws ReaperException {
    final IStorageDao storage = mock(IStorageDao.class);
    Set<String> datacenters =
        RepairRunService.getDatacentersToRepairBasedOnParam(Optional.of("dc1,dc2"));
    assertEquals("Datacenters were not parsed correctly", 2, datacenters.size());
  }

  @Test(expected = ReaperException.class)
  public void generateSegmentsTestEmpty() throws ReaperException, UnknownHostException {
    Cluster cluster =
        Cluster.builder()
            .withName("test_" + RandomStringUtils.randomAlphabetic(12))
            .withSeedHosts(ImmutableSet.of("127.0.0.1", "127.0.0.2", "127.0.0.3"))
            .withState(Cluster.State.ACTIVE)
            .withPartitioner("Murmur3Partitioner")
            .build();
    final String KS_NAME = "reaper";
    final Set<String> CF_NAMES = Sets.newHashSet("reaper");
    final boolean INCREMENTAL_REPAIR = false;
    final boolean SUBRANGE_INCREMENTAL_REPAIR = false;
    final Set<String> NODES = Sets.newHashSet("127.0.0.1", "127.0.0.2", "127.0.0.3");
    final Set<String> DATACENTERS = Collections.emptySet();
    final Set<String> BLACKLISTED_TABLES = Collections.emptySet();
    final long TIME_RUN = 41L;
    final int REPAIR_THREAD_COUNT = 1;
    final int segmentTimeout = 30;
    final List<BigInteger> TOKENS =
        Lists.newArrayList(
            BigInteger.valueOf(0L), BigInteger.valueOf(100L), BigInteger.valueOf(200L));
    final IStorageDao storage = new MemoryStorageFacade();

    storage.getClusterDao().addCluster(cluster);

    RepairUnit cf =
        storage
            .getRepairUnitDao()
            .addRepairUnit(
                RepairUnit.builder()
                    .clusterName(cluster.getName())
                    .keyspaceName(KS_NAME)
                    .columnFamilies(CF_NAMES)
                    .incrementalRepair(INCREMENTAL_REPAIR)
                    .subrangeIncrementalRepair(SUBRANGE_INCREMENTAL_REPAIR)
                    .nodes(NODES)
                    .datacenters(DATACENTERS)
                    .blacklistedTables(BLACKLISTED_TABLES)
                    .repairThreadCount(REPAIR_THREAD_COUNT)
                    .timeout(segmentTimeout));
    DateTimeUtils.setCurrentMillisFixed(TIME_RUN);

    AppContext context = new AppContext();
    context.storage = storage;
    context.config = new ReaperApplicationConfiguration();
    context.schedulingManager = mock(SchedulingManager.class);
    doNothing().when(context.schedulingManager).maybeRegisterRepairRunCompleted(any());
    final Semaphore mutex = new Semaphore(0);
    final JmxCassandraManagementProxy jmx = CassandraManagementProxyTest.mockJmxProxyImpl();
    when(jmx.getClusterName()).thenReturn(cluster.getName());
    when(jmx.getRangeToEndpointMap(anyString())).thenReturn(RepairRunnerTest.sixNodeCluster());
    EndpointSnitchInfoMBean endpointSnitchInfoMBean = mock(EndpointSnitchInfoMBean.class);
    when(endpointSnitchInfoMBean.getDatacenter()).thenReturn("dc1");
    try {
      when(endpointSnitchInfoMBean.getDatacenter(anyString())).thenReturn("dc1");
    } catch (UnknownHostException ex) {
      throw new AssertionError(ex);
    }

    ClusterFacade clusterFacade = mock(ClusterFacade.class);
    when(clusterFacade.connect(any(Cluster.class))).thenReturn(jmx);
    when(clusterFacade.nodeIsDirectlyAccessible(any(), any())).thenReturn(true);
    when(clusterFacade.tokenRangeToEndpoint(any(), anyString(), any()))
        .thenReturn(Lists.newArrayList(NODES));
    when(clusterFacade.getRangeToEndpointMap(any(), anyString()))
        .thenReturn((Map) ImmutableMap.of(Lists.newArrayList("0", "100"), Collections.EMPTY_LIST));
    when(clusterFacade.getCassandraVersion(any())).thenReturn("3.11.6");
    when(clusterFacade.getTokens(any())).thenReturn(TOKENS);

    context.managementConnectionFactory =
        new JmxManagementConnectionFactory(context, new NoopCrypotograph()) {
          @Override
          protected JmxCassandraManagementProxy connectImpl(Node host) throws ReaperException {
            return jmx;
          }
        };

    RepairRunService repairRunService =
        RepairRunService.create(context, () -> clusterFacade, context.storage.getRepairRunDao());

    RepairUnit unit =
        RepairUnit.builder()
            .clusterName(cluster.getName())
            .keyspaceName("test")
            .blacklistedTables(Sets.newHashSet("table1"))
            .incrementalRepair(false)
            .subrangeIncrementalRepair(false)
            .repairThreadCount(4)
            .timeout(segmentTimeout)
            .build(Uuids.timeBased());
    List<Segment> segments = repairRunService.generateSegments(cluster, 0, unit);
  }
}
