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

package io.cassandrareaper.resources;

import io.cassandrareaper.AppContext;
import io.cassandrareaper.ReaperApplicationConfiguration;
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.RepairRun;
import io.cassandrareaper.core.RepairSegment;
import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.core.Table;
import io.cassandrareaper.jmx.JmxConnectionFactory;
import io.cassandrareaper.jmx.JmxProxy;
import io.cassandrareaper.jmx.JmxProxyTest;
import io.cassandrareaper.resources.view.RepairRunStatus;
import io.cassandrareaper.service.RepairManager;
import io.cassandrareaper.service.RepairRunnerTest;
import io.cassandrareaper.storage.MemoryStorageFacade;

import java.math.BigInteger;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.assertj.core.api.Assertions;
import org.assertj.core.util.Maps;
import org.joda.time.DateTimeUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class RepairRunResourceTest {

  private static final String PARTITIONER = "org.apache.cassandra.dht.RandomPartitioner";
  private static final String SEED_HOST = "127.0.0.1";
  private static final Boolean INCREMENTAL = false;
  private static final Set<String> TABLES = Sets.newHashSet("testTable");
  private static final Set<String> NODES = Collections.emptySet();
  private static final Set<String> DATACENTERS = Collections.emptySet();
  private static final Map<String, String> NODES_MAP = Maps.newHashMap("node1", "127.0.0.1");
  private static final Set<String> BLACKLISTED_TABLES = Collections.emptySet();
  private static final int REPAIR_THREAD_COUNT = 2;
  private static final String OWNER = "test";
  private static final int THREAD_CNT = 1;
  private static final int REPAIR_TIMEOUT_S = 60;
  private static final int RETRY_DELAY_S = 10;
  private static final long TIME_CREATE = 42L;
  private static final long TIME_START = 43L;
  private static final URI SAMPLE_URI = URI.create("http://reaper_host/repair_run/");
  private static final int SEGMENT_CNT = 6;
  private static final double REPAIR_INTENSITY = 0.5f;
  private static final RepairParallelism REPAIR_PARALLELISM = RepairParallelism.SEQUENTIAL;
  private static final String STCS = "SizeTieredCompactionStrategy";
  private static final int SEGMENT_TIMEOUT = 30;

  private static final List<BigInteger> TOKENS = Lists.newArrayList(
      BigInteger.valueOf(0L),
      BigInteger.valueOf(100L),
      BigInteger.valueOf(200L));


  private final String clustername = "testcluster_" + RandomStringUtils.randomAlphabetic(6);
  private final String keyspace = "testkeyspace_" + RandomStringUtils.randomAlphabetic(6);
  private AppContext context;
  private UriInfo uriInfo;
  private JmxProxy proxy;

  @Before
  public void setUp() throws Exception {
    context = new AppContext();
    context.config = new ReaperApplicationConfiguration();
    context.repairManager = RepairManager.create(
        context,
        Executors.newScheduledThreadPool(THREAD_CNT),
        RETRY_DELAY_S,
        TimeUnit.SECONDS,
        1);

    context.storage = new MemoryStorageFacade();

    Cluster cluster = Cluster.builder()
        .withName(clustername)
        .withPartitioner(PARTITIONER)
        .withSeedHosts(ImmutableSet.of(SEED_HOST))
        .withState(Cluster.State.ACTIVE)
        .build();

    context.storage.addCluster(cluster);

    context.config = new ReaperApplicationConfiguration();
    context.config.setSegmentCount(SEGMENT_CNT);
    context.config.setRepairIntensity(REPAIR_INTENSITY);
    context.config.setHangingRepairTimeoutMins(SEGMENT_TIMEOUT);

    uriInfo = mock(UriInfo.class);
    when(uriInfo.getBaseUriBuilder()).thenReturn(UriBuilder.fromUri(SAMPLE_URI));

    proxy = JmxProxyTest.mockJmxProxyImpl();
    when(proxy.getClusterName()).thenReturn(clustername);
    when(proxy.getCassandraVersion()).thenReturn("3.11.4");
    when(proxy.getPartitioner()).thenReturn(PARTITIONER);
    when(proxy.getTablesForKeyspace(keyspace))
        .thenReturn(TABLES.stream()
            .map(t -> Table.builder().withName(t).withCompactionStrategy(STCS).build())
            .collect(Collectors.toSet()));
    when(proxy.getEndpointToHostId()).thenReturn(NODES_MAP);
    when(proxy.getTokens()).thenReturn(TOKENS);
    when(proxy.isConnectionAlive()).thenReturn(Boolean.TRUE);
    when(proxy.getRangeToEndpointMap(anyString())).thenReturn(RepairRunnerTest.threeNodeClusterWithIps());
    when(proxy.triggerRepair(
            any(BigInteger.class),
            any(BigInteger.class),
            anyString(),
            any(RepairParallelism.class),
            anyCollection(),
            anyBoolean(),
            anyCollection(),
            any(),
            any(),
            any(Integer.class)))
        .thenReturn(1);

    EndpointSnitchInfoMBean endpointSnitchInfoMBean = mock(EndpointSnitchInfoMBean.class);
    when(endpointSnitchInfoMBean.getDatacenter()).thenReturn("dc1");
    try {
      when(endpointSnitchInfoMBean.getDatacenter(anyString())).thenReturn("dc1");
    } catch (UnknownHostException ex) {
      throw new AssertionError(ex);
    }
    JmxProxyTest.mockGetEndpointSnitchInfoMBean(proxy, endpointSnitchInfoMBean);

    context.jmxConnectionFactory = mock(JmxConnectionFactory.class);
    when(context.jmxConnectionFactory.connectAny(Mockito.anyCollection())).thenReturn(proxy);

    when(context.jmxConnectionFactory.connectAny(Mockito.anyCollection()))
        .thenReturn(proxy);

    RepairUnit.Builder repairUnitBuilder = RepairUnit.builder()
            .clusterName(clustername)
            .keyspaceName(keyspace)
            .columnFamilies(TABLES)
            .incrementalRepair(INCREMENTAL)
            .nodes(NODES)
            .datacenters(DATACENTERS)
            .blacklistedTables(BLACKLISTED_TABLES)
            .repairThreadCount(REPAIR_THREAD_COUNT)
            .timeout(SEGMENT_TIMEOUT);

    context.storage.addRepairUnit(repairUnitBuilder);
  }

  @After
  public void tearDown() {
    DateTimeUtils.setCurrentMillisSystem();
  }

  private Response addDefaultRepairRun(RepairRunResource resource) {
    return addRepairRun(resource,
        uriInfo,
        clustername,
        keyspace,
        TABLES,
        OWNER,
        "",
        SEGMENT_CNT,
        NODES,
        BLACKLISTED_TABLES,
        REPAIR_THREAD_COUNT);
  }

  private Response addRepairRun(
      RepairRunResource resource,
      UriInfo uriInfo,
      String clusterName,
      String keyspace,
      Set<String> columnFamilies,
      String owner,
      String cause,
      Integer segments,
      Set<String> nodes,
      Set<String> blacklistedTables,
      Integer repairThreadCount) {

    return resource.addRepairRun(
        uriInfo,
        Optional.ofNullable(clusterName),
        Optional.ofNullable(keyspace),
        columnFamilies.isEmpty() ? Optional.empty() : Optional.of(StringUtils.join(columnFamilies, ',')),
        Optional.ofNullable(owner),
        Optional.ofNullable(cause),
        Optional.ofNullable(segments),
        Optional.of(REPAIR_PARALLELISM.name()),
        Optional.<String>empty(),
        Optional.<String>empty(),
        nodes.isEmpty() ? Optional.empty() : Optional.of(StringUtils.join(nodes, ',')),
        Optional.<String>empty(),
        blacklistedTables.isEmpty() ? Optional.empty() : Optional.of(StringUtils.join(blacklistedTables, ',')),
        Optional.of(repairThreadCount),
        Optional.<String>empty(),
        Optional.of(30));
  }

  @Test
  public void testAddRepairRun() throws Exception {
    DateTimeUtils.setCurrentMillisFixed(TIME_CREATE);
    RepairRunResource resource = new RepairRunResource(context);
    Response response = addDefaultRepairRun(resource);

    assertEquals(201, response.getStatus());
    assertTrue(response.getEntity() instanceof RepairRunStatus);

    assertEquals(1, context.storage.getClusters().size());
    assertEquals(1, context.storage.getRepairRunsForCluster(clustername, Optional.of(2)).size());
    assertEquals(1, context.storage.getRepairRunsForClusterPrioritiseRunning(clustername, Optional.of(2)).size());
    assertEquals(1, context.storage.getRepairRunIdsForCluster(clustername, Optional.empty()).size());
    UUID runId = context.storage.getRepairRunIdsForCluster(clustername, Optional.empty()).iterator().next();
    RepairRun run = context.storage.getRepairRun(runId).get();
    final RepairUnit unit = context.storage.getRepairUnit(run.getRepairUnitId());
    assertEquals(RepairRun.RunState.NOT_STARTED, run.getRunState());
    assertEquals(TIME_CREATE, run.getCreationTime().getMillis());
    assertEquals(REPAIR_INTENSITY, run.getIntensity(), 0.0f);
    assertNull(run.getStartTime());
    assertNull(run.getEndTime());
    assertEquals(2, unit.getRepairThreadCount());

    // tokens [0, 100, 200], 6 requested segments per node and 3 nodes causes generating 20 RepairSegments
    assertEquals(
        20,
        context.storage.getSegmentAmountForRepairRunWithState(
            run.getId(), RepairSegment.State.NOT_STARTED));

    // adding another repair run should work as well
    response = addDefaultRepairRun(resource);

    assertEquals(201, response.getStatus());
    assertTrue(response.getEntity() instanceof RepairRunStatus);

    assertEquals(1, context.storage.getClusters().size());
    assertEquals(1, context.storage.getRepairRunsForCluster(clustername, Optional.of(1)).size());
    assertEquals(2, context.storage.getRepairRunsForCluster(clustername, Optional.of(2)).size());
    assertEquals(2, context.storage.getRepairRunsForCluster(clustername, Optional.of(3)).size());

    assertEquals(
        context.storage.getRepairRunsForCluster(clustername, Optional.of(3)).iterator().next().getId(),
        context.storage.getRepairRunsForCluster(clustername, Optional.of(1)).iterator().next().getId());

    assertEquals(
        context.storage.getRepairRunsForCluster(clustername, Optional.of(2)).iterator().next().getId(),
        context.storage.getRepairRunsForCluster(clustername, Optional.of(1)).iterator().next().getId());

  }

  @Test
  public void doesNotDisplayBlacklistedCompactionStrategies() throws Exception {
    context.config.setBlacklistTwcsTables(true);
    context.config.setHangingRepairTimeoutMins(30);
    when(proxy.getKeyspaces()).thenReturn(Lists.newArrayList(keyspace));

    when(proxy.getTablesForKeyspace(keyspace)).thenReturn(
            Sets.newHashSet(
                    Table.builder().withName("table1")
                            .withCompactionStrategy("org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy")
                            .build(),
                    Table.builder().withName("table2")
                            .withCompactionStrategy("org.apache.cassandra.db.compaction.DateTieredCompactionStrategy")
                            .build(),
                    Table.builder().withName("table3")
                            .withCompactionStrategy("org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy")
                            .build()));

    RepairRunResource resource = new RepairRunResource(context);

    Response response = addRepairRun(resource,
        uriInfo,
        clustername,
        keyspace,
        Collections.EMPTY_SET,
        OWNER,
        "",
        SEGMENT_CNT,
        NODES,
        BLACKLISTED_TABLES,
        REPAIR_THREAD_COUNT);

    assertTrue(response.getEntity().toString(), response.getEntity() instanceof RepairRunStatus);
    RepairRunStatus repairRunStatus = (RepairRunStatus) response.getEntity();

    Assertions.assertThat(repairRunStatus.getBlacklistedTables()).isEmpty();
    assertFalse("Tables shouldn't contain 'table1'", repairRunStatus.getColumnFamilies().contains("table1"));
    assertFalse("Tables shouldn't contain 'table2'", repairRunStatus.getColumnFamilies().contains("table2"));
    assertTrue("Tables shouldn't contain 'table3'", repairRunStatus.getColumnFamilies().contains("table3"));
  }

  @Test
  public void displaysExplicitBlacklistedCompactionStrategies() throws Exception {
    context.config.setBlacklistTwcsTables(true);
    when(proxy.getKeyspaces()).thenReturn(Lists.newArrayList(keyspace));

    when(proxy.getTablesForKeyspace(keyspace)).thenReturn(
            Sets.newHashSet(
                    Table.builder().withName("table1")
                            .withCompactionStrategy("org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy")
                            .build(),
                    Table.builder().withName("table2")
                            .withCompactionStrategy("org.apache.cassandra.db.compaction.DateTieredCompactionStrategy")
                            .build(),
                    Table.builder().withName("table3")
                            .withCompactionStrategy("org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy")
                            .build()));

    RepairRunResource resource = new RepairRunResource(context);

    Response response = addRepairRun(resource,
        uriInfo,
        clustername,
        keyspace,
        Sets.newHashSet("table1"),
        OWNER,
        "",
        SEGMENT_CNT,
        NODES,
        BLACKLISTED_TABLES,
        REPAIR_THREAD_COUNT);

    assertTrue(response.getEntity().toString(), response.getEntity() instanceof RepairRunStatus);
    RepairRunStatus repairRunStatus = (RepairRunStatus) response.getEntity();

    assertFalse("Blacklisted shouldn't contain 'table1'", repairRunStatus.getBlacklistedTables().contains("table1"));
    assertFalse("Blacklisted shouldn't contain 'table2'", repairRunStatus.getBlacklistedTables().contains("table2"));
    assertFalse("Blacklisted shouldn't contain 'table3'", repairRunStatus.getBlacklistedTables().contains("table3"));
    assertTrue("ColumnFamilies should contain 'table1'", repairRunStatus.getColumnFamilies().contains("table1"));
    assertFalse("ColumnFamilies shouldn't contain 'table2'", repairRunStatus.getColumnFamilies().contains("table2"));
    assertFalse("ColumnFamilies shouldn't contain 'table3'", repairRunStatus.getColumnFamilies().contains("table3"));
  }

  @Test
  public void testTriggerNotExistingRun() throws ReaperException {
    RepairRunResource resource = new RepairRunResource(context);
    Optional<String> newState = Optional.of(RepairRun.RunState.RUNNING.toString());
    Response response = resource.modifyRunState(uriInfo, UUIDs.timeBased(), newState);
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
    assertTrue(response.getEntity() instanceof String);
    assertTrue(response.getEntity().toString().contains("doesn't exist"));
  }

  @Test
  public void testTriggerAlreadyRunningRun() throws InterruptedException, ReaperException {
    DateTimeUtils.setCurrentMillisFixed(TIME_CREATE);

    RepairRunResource resource = new RepairRunResource(context);
    Response response = addDefaultRepairRun(resource);
    assertTrue(response.getEntity().toString(), response.getEntity() instanceof RepairRunStatus);
    RepairRunStatus repairRunStatus = (RepairRunStatus) response.getEntity();
    UUID runId = repairRunStatus.getId();

    DateTimeUtils.setCurrentMillisFixed(TIME_START);
    Optional<String> newState = Optional.of(RepairRun.RunState.RUNNING.toString());
    resource.modifyRunState(uriInfo, runId, newState);
    Thread.sleep(1000);
    response = resource.modifyRunState(uriInfo, runId, newState);
    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
  }

  @Test
  public void testTriggerNewRunAlreadyRunningRun() throws InterruptedException, ReaperException {
    DateTimeUtils.setCurrentMillisFixed(TIME_CREATE);
    RepairRunResource resource = new RepairRunResource(context);
    Response response = addDefaultRepairRun(resource);
    assertTrue(response.getEntity().toString(), response.getEntity() instanceof RepairRunStatus);
    RepairRunStatus repairRunStatus = (RepairRunStatus) response.getEntity();
    UUID runId = repairRunStatus.getId();

    DateTimeUtils.setCurrentMillisFixed(TIME_START);
    Optional<String> newState = Optional.of(RepairRun.RunState.RUNNING.toString());
    resource.modifyRunState(uriInfo, runId, newState);
    Thread.sleep(1000);
    response = resource.modifyRunState(uriInfo, runId, newState);
    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());

    // Adding a second run that we'll try to set to RUNNING status
    RepairRunResource newResource = new RepairRunResource(context);
    Response newResponse = addDefaultRepairRun(newResource);
    RepairRunStatus newRepairRunStatus = (RepairRunStatus) newResponse.getEntity();
    UUID newRunId = newRepairRunStatus.getId();

    DateTimeUtils.setCurrentMillisFixed(TIME_START);
    Optional<String> newRunState = Optional.of(RepairRun.RunState.RUNNING.toString());
    response = resource.modifyRunState(uriInfo, newRunId, newRunState);
    // We expect it to fail as we cannot have 2 running runs for the same repair unit at once
    assertEquals(Response.Status.CONFLICT.getStatusCode(), response.getStatus());

  }

  @Test
  public void testAddRunClusterNotInStorage() {
    context.storage = new MemoryStorageFacade();
    RepairRunResource resource = new RepairRunResource(context);
    Response response = addDefaultRepairRun(resource);
    assertEquals(404, response.getStatus());
    assertTrue(response.getEntity() instanceof String);
  }

  @Test
  public void testAddRunMissingArgument() {
    RepairRunResource resource = new RepairRunResource(context);

    Response response = addRepairRun(
            resource,
            uriInfo,
            clustername,
            null,
            TABLES,
            OWNER,
            null,
            SEGMENT_CNT,
            NODES,
            BLACKLISTED_TABLES,
            REPAIR_THREAD_COUNT);

    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    assertTrue(response.getEntity() instanceof String);
  }

  @Test
  public void testPauseNotRunningRun() throws InterruptedException, ReaperException {
    DateTimeUtils.setCurrentMillisFixed(TIME_CREATE);
    RepairRunResource resource = new RepairRunResource(context);
    Response response = addDefaultRepairRun(resource);
    assertTrue(response.getEntity().toString(), response.getEntity() instanceof RepairRunStatus);
    RepairRunStatus repairRunStatus = (RepairRunStatus) response.getEntity();
    UUID runId = repairRunStatus.getId();

    response = resource.modifyRunState(uriInfo, runId,
        Optional.of(RepairRun.RunState.PAUSED.toString()));
    Thread.sleep(200);

    assertEquals(409, response.getStatus());
    RepairRun repairRun = context.storage.getRepairRun(runId).get();
    // the run should be paused
    assertEquals(RepairRun.RunState.NOT_STARTED, repairRun.getRunState());
    // but the running segment should be untouched
    assertEquals(0,
        context.storage.getSegmentAmountForRepairRunWithState(runId,
            RepairSegment.State.RUNNING));
  }

  @Test
  public void testPauseNotExistingRun() throws InterruptedException, ReaperException {
    RepairRunResource resource = new RepairRunResource(context);
    Response response = resource.modifyRunState(uriInfo, UUIDs.timeBased(),
        Optional.of(RepairRun.RunState.PAUSED.toString()));
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
    assertEquals(0, context.storage.getRepairRunsWithState(RepairRun.RunState.RUNNING).size());
  }

  @Test
  public void testModifyIntensity() throws ReaperException {
    DateTimeUtils.setCurrentMillisFixed(TIME_CREATE);
    RepairRunResource resource = new RepairRunResource(context);
    Response response = addDefaultRepairRun(resource);
    assertTrue(response.getEntity().toString(), response.getEntity() instanceof RepairRunStatus);
    RepairRunStatus repairRunStatus = (RepairRunStatus) response.getEntity();
    UUID runId = repairRunStatus.getId();

    response = resource.modifyRunState(uriInfo, runId, Optional.of(RepairRun.RunState.RUNNING.toString()));
    assertEquals(200, response.getStatus());
    response = resource.modifyRunState(uriInfo, runId, Optional.of(RepairRun.RunState.PAUSED.toString()));
    assertEquals(200, response.getStatus());
    response = resource.modifyRunIntensity(uriInfo, runId, Optional.of("0.1"));
    assertFalse(response.hasEntity());
  }

  @Test
  public void testSplitStateParam() {
    Optional<String> stateParam = Optional.of("RUNNING");
    assertEquals(Sets.newHashSet("RUNNING"), RepairRunResource.splitStateParam(stateParam));
    stateParam = Optional.of("PAUSED,RUNNING");
    assertEquals(Sets.newHashSet("RUNNING", "PAUSED"), RepairRunResource.splitStateParam(stateParam));
    stateParam = Optional.of("NOT_EXISTING");
    assertEquals(null, RepairRunResource.splitStateParam(stateParam));
    stateParam = Optional.of("NOT_EXISTING,RUNNING");
    assertEquals(null, RepairRunResource.splitStateParam(stateParam));
    stateParam = Optional.of("RUNNING,PAUSED,");
    assertEquals(Sets.newHashSet("RUNNING", "PAUSED"), RepairRunResource.splitStateParam(stateParam));
    stateParam = Optional.of(",RUNNING,PAUSED,");
    assertEquals(Sets.newHashSet("RUNNING", "PAUSED"), RepairRunResource.splitStateParam(stateParam));
    stateParam = Optional.of("PAUSED ,RUNNING");
    assertEquals(Sets.newHashSet("RUNNING", "PAUSED"), RepairRunResource.splitStateParam(stateParam));
  }

  private Response addRepairRunWithForceParam(
      RepairRunResource resource,
      UriInfo uriInfo,
      String clusterName,
      String keyspace,
      Set<String> columnFamilies,
      String owner,
      String cause,
      Integer segments,
      Set<String> nodes,
      Set<String> blacklistedTables,
      Integer repairThreadCount,
      String force,
      Integer segmentTimeout) {

    return resource.addRepairRun(
        uriInfo,
        Optional.ofNullable(clusterName),
        Optional.ofNullable(keyspace),
        columnFamilies.isEmpty() ? Optional.empty() : Optional.of(StringUtils.join(columnFamilies, ',')),
        Optional.ofNullable(owner),
        Optional.ofNullable(cause),
        Optional.ofNullable(segments),
        Optional.of(REPAIR_PARALLELISM.name()),
        Optional.<String>empty(),
        Optional.<String>empty(),
        nodes.isEmpty() ? Optional.empty() : Optional.of(StringUtils.join(nodes, ',')),
        Optional.<String>empty(),
        blacklistedTables.isEmpty() ? Optional.empty() : Optional.of(StringUtils.join(blacklistedTables, ',')),
        Optional.of(repairThreadCount),
        Optional.of(force),
        Optional.of(segmentTimeout));
  }

  @Test
  public void testAddRunMalformedForceParam() {
    RepairRunResource resource = new RepairRunResource(context);

    Response response = addRepairRunWithForceParam(
            resource,
            uriInfo,
            clustername,
            keyspace,
            TABLES,
            OWNER,
            null,
            SEGMENT_CNT,
            NODES,
            BLACKLISTED_TABLES,
            REPAIR_THREAD_COUNT,
            "foo",
            30);

    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    assertTrue(response.getEntity() instanceof String);
    assertEquals("invalid query parameter \"force\", expecting [True,False]", response.getEntity());
  }
}
