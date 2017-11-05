/*
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
import io.cassandrareaper.jmx.JmxConnectionFactory;
import io.cassandrareaper.jmx.JmxProxy;
import io.cassandrareaper.jmx.RepairStatusHandler;
import io.cassandrareaper.resources.view.RepairRunStatus;
import io.cassandrareaper.service.RepairManager;
import io.cassandrareaper.service.RepairRunnerTest;
import io.cassandrareaper.service.RingRange;
import io.cassandrareaper.storage.MemoryStorage;

import java.math.BigInteger;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import com.datastax.driver.core.utils.UUIDs;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.cassandra.repair.RepairParallelism;
import org.assertj.core.util.Maps;
import org.joda.time.DateTimeUtils;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyCollectionOf;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class RepairRunResourceTest {

  private static final String CLUSTER_NAME = "testcluster";
  private static final String PARTITIONER = "org.apache.cassandra.dht.RandomPartitioner";
  private static final String SEED_HOST = "TestHost";
  private static final String KEYSPACE = "testKeyspace";
  private static final Boolean INCREMENTAL = false;
  private static final Set<String> TABLES = Sets.newHashSet("testTable");
  private static final Set<String> NODES = Collections.emptySet();
  private static final Set<String> DATACENTERS = Collections.emptySet();
  private static final Map<String, String> NODES_MAP = Maps.newHashMap("node1", "127.0.0.1");
  private static final Set<String> BLACKLISTED_TABLES = Collections.emptySet();
  private static final String OWNER = "test";
  private static final int THREAD_CNT = 1;
  private static final int REPAIR_TIMEOUT_S = 60;
  private static final int RETRY_DELAY_S = 10;
  private static final long TIME_CREATE = 42L;
  private static final long TIME_START = 43L;
  private static final URI SAMPLE_URI = URI.create("http://test");
  private static final int SEGMENT_CNT = 6;
  private static final double REPAIR_INTENSITY = 0.5f;
  private static final RepairParallelism REPAIR_PARALLELISM = RepairParallelism.SEQUENTIAL;

  private static final List<BigInteger> TOKENS = Lists.newArrayList(
      BigInteger.valueOf(0L),
      BigInteger.valueOf(100L),
      BigInteger.valueOf(200L));

  AppContext context;
  UriInfo uriInfo;

  @Before
  public void setUp() throws Exception {
    //SegmentRunner.SEGMENT_RUNNERS.clear();

    context = new AppContext();
    context.repairManager = RepairManager.create(context);
    context.storage = new MemoryStorage();
    Cluster cluster = new Cluster(CLUSTER_NAME, PARTITIONER, Sets.newHashSet(SEED_HOST));
    context.storage.addCluster(cluster);

    context.config = mock(ReaperApplicationConfiguration.class);
    when(context.config.getSegmentCount()).thenReturn(SEGMENT_CNT);
    when(context.config.getRepairIntensity()).thenReturn(REPAIR_INTENSITY);

    uriInfo = mock(UriInfo.class);
    when(uriInfo.getAbsolutePath()).thenReturn(SAMPLE_URI);
    when(uriInfo.getBaseUri()).thenReturn(SAMPLE_URI);

    final JmxProxy proxy = mock(JmxProxy.class);
    when(proxy.getClusterName()).thenReturn(CLUSTER_NAME);
    when(proxy.getPartitioner()).thenReturn(PARTITIONER);
    when(proxy.getTableNamesForKeyspace(KEYSPACE)).thenReturn(TABLES);
    when(proxy.getEndpointToHostId()).thenReturn(NODES_MAP);
    when(proxy.getTokens()).thenReturn(TOKENS);
    when(proxy.isConnectionAlive()).thenReturn(Boolean.TRUE);
    when(proxy.tokenRangeToEndpoint(anyString(), any(RingRange.class))).thenReturn(
        Collections.singletonList(""));
    when(proxy.getRangeToEndpointMap(anyString())).thenReturn(RepairRunnerTest.sixNodeCluster());
    when(proxy.triggerRepair(any(BigInteger.class), any(BigInteger.class), anyString(),
        any(RepairParallelism.class), anyCollectionOf(String.class), anyBoolean(), anyCollectionOf(String.class)))
        .thenReturn(1);

    context.jmxConnectionFactory = new JmxConnectionFactory() {
      @Override
      protected JmxProxy connect(Optional<RepairStatusHandler> handler, String host, int connectionTimeout)
          throws ReaperException {
        return proxy;
      }
    };

    RepairUnit.Builder repairUnitBuilder =
        new RepairUnit.Builder(
            CLUSTER_NAME, KEYSPACE, TABLES, INCREMENTAL, NODES, DATACENTERS, BLACKLISTED_TABLES);

    context.storage.addRepairUnit(repairUnitBuilder);
  }

  private Response addDefaultRepairRun(RepairRunResource resource) {
    return addRepairRun(
        resource,
        uriInfo,
        CLUSTER_NAME,
        KEYSPACE,
        TABLES,
        OWNER,
        "",
        SEGMENT_CNT,
        NODES,
        BLACKLISTED_TABLES);
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
      Set<String> blacklistedTables) {

    return resource.addRepairRun(
        uriInfo,
        Optional.fromNullable(clusterName),
        Optional.fromNullable(keyspace),
        columnFamilies == null
            ? Optional.<String>absent()
            : Optional.of(columnFamilies.iterator().next()),
        Optional.fromNullable(owner),
        Optional.fromNullable(cause),
        Optional.fromNullable(segments),
        Optional.of(REPAIR_PARALLELISM.name()),
        Optional.<String>absent(),
        Optional.<String>absent(),
        nodes == null || nodes.isEmpty()
            ? Optional.<String>absent()
            : Optional.of(nodes.iterator().next()),
        Optional.<String>absent(),
        Optional.<String>absent());
  }

  @Test
  public void testAddRepairRun() throws Exception {

    DateTimeUtils.setCurrentMillisFixed(TIME_CREATE);
    RepairRunResource resource = new RepairRunResource(context);
    Response response = addDefaultRepairRun(resource);

    assertEquals(201, response.getStatus());
    assertTrue(response.getEntity() instanceof RepairRunStatus);

    assertEquals(1, context.storage.getClusters().size());
    assertEquals(1, context.storage.getRepairRunsForCluster(CLUSTER_NAME).size());
    assertEquals(1, context.storage.getRepairRunIdsForCluster(CLUSTER_NAME).size());
    UUID runId = context.storage.getRepairRunIdsForCluster(CLUSTER_NAME).iterator().next();
    RepairRun run = context.storage.getRepairRun(runId).get();
    assertEquals(RepairRun.RunState.NOT_STARTED, run.getRunState());
    assertEquals(TIME_CREATE, run.getCreationTime().getMillis());
    assertEquals(REPAIR_INTENSITY, run.getIntensity(), 0.0f);
    assertNull(run.getStartTime());
    assertNull(run.getEndTime());

    // tokens [0, 100, 200], 6 requested segments per node and 6 nodes causes generating 38 RepairSegments
    assertEquals(
        38,
        context.storage.getSegmentAmountForRepairRunWithState(
            run.getId(), RepairSegment.State.NOT_STARTED));

    // adding another repair run should work as well
    response = addDefaultRepairRun(resource);

    assertEquals(201, response.getStatus());
    assertTrue(response.getEntity() instanceof RepairRunStatus);

    assertEquals(1, context.storage.getClusters().size());
    assertEquals(2, context.storage.getRepairRunsForCluster(CLUSTER_NAME).size());
  }

  @Test
  public void testTriggerNotExistingRun() throws ReaperException {
    RepairRunResource resource = new RepairRunResource(context);
    Optional<String> newState = Optional.of(RepairRun.RunState.RUNNING.toString());
    Response response = resource.modifyRunState(uriInfo, UUIDs.timeBased(), newState);
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
    assertTrue(response.getEntity() instanceof String);
    assertTrue(response.getEntity().toString().contains("not found"));
  }

  @Test
  public void testTriggerAlreadyRunningRun() throws InterruptedException, ReaperException {
    DateTimeUtils.setCurrentMillisFixed(TIME_CREATE);

    context.repairManager
        .initializeThreadPool(THREAD_CNT, REPAIR_TIMEOUT_S, TimeUnit.SECONDS, RETRY_DELAY_S, TimeUnit.SECONDS);

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
    assertEquals(Response.Status.NOT_MODIFIED.getStatusCode(), response.getStatus());
  }

  @Test
  public void testTriggerNewRunAlreadyRunningRun() throws InterruptedException, ReaperException {
    DateTimeUtils.setCurrentMillisFixed(TIME_CREATE);
    context.repairManager.initializeThreadPool(THREAD_CNT, REPAIR_TIMEOUT_S, TimeUnit.SECONDS,
        RETRY_DELAY_S, TimeUnit.SECONDS);
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
    assertEquals(Response.Status.NOT_MODIFIED.getStatusCode(), response.getStatus());

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
    context.storage = new MemoryStorage();
    RepairRunResource resource = new RepairRunResource(context);
    Response response = addDefaultRepairRun(resource);
    assertEquals(404, response.getStatus());
    assertTrue(response.getEntity() instanceof String);
  }

  @Test
  public void testAddRunMissingArgument() {
    RepairRunResource resource = new RepairRunResource(context);
    Response response =
        addRepairRun(
            resource,
            uriInfo,
            CLUSTER_NAME,
            null,
            TABLES,
            OWNER,
            null,
            SEGMENT_CNT,
            NODES,
            BLACKLISTED_TABLES);
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    assertTrue(response.getEntity() instanceof String);
  }

  @Test
  public void testTriggerRunMissingArgument() {
    context.repairManager.initializeThreadPool(THREAD_CNT, REPAIR_TIMEOUT_S, TimeUnit.SECONDS,
        RETRY_DELAY_S, TimeUnit.SECONDS);
    RepairRunResource resource = new RepairRunResource(context);
    Response response =
        addRepairRun(
            resource,
            uriInfo,
            CLUSTER_NAME,
            null,
            TABLES,
            OWNER,
            null,
            SEGMENT_CNT,
            NODES,
            BLACKLISTED_TABLES);
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    assertTrue(response.getEntity() instanceof String);
  }

  @Test
  public void testPauseNotRunningRun() throws InterruptedException, ReaperException {
    DateTimeUtils.setCurrentMillisFixed(TIME_CREATE);
    context.repairManager.initializeThreadPool(THREAD_CNT, REPAIR_TIMEOUT_S, TimeUnit.SECONDS,
        RETRY_DELAY_S, TimeUnit.SECONDS);
    RepairRunResource resource = new RepairRunResource(context);
    Response response = addDefaultRepairRun(resource);
    assertTrue(response.getEntity().toString(), response.getEntity() instanceof RepairRunStatus);
    RepairRunStatus repairRunStatus = (RepairRunStatus) response.getEntity();
    UUID runId = repairRunStatus.getId();

    response = resource.modifyRunState(uriInfo, runId,
        Optional.of(RepairRun.RunState.PAUSED.toString()));
    Thread.sleep(200);

    assertEquals(405, response.getStatus());
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
  public void testSplitStateParam() {
    RepairRunResource resource = new RepairRunResource(context);
    Optional<String> stateParam = Optional.of("RUNNING");
    assertEquals(Sets.newHashSet("RUNNING"), resource.splitStateParam(stateParam));
    stateParam = Optional.of("PAUSED,RUNNING");
    assertEquals(Sets.newHashSet("RUNNING", "PAUSED"), resource.splitStateParam(stateParam));
    stateParam = Optional.of("NOT_EXISTING");
    assertEquals(null, resource.splitStateParam(stateParam));
    stateParam = Optional.of("NOT_EXISTING,RUNNING");
    assertEquals(null, resource.splitStateParam(stateParam));
    stateParam = Optional.of("RUNNING,PAUSED,");
    assertEquals(Sets.newHashSet("RUNNING", "PAUSED"), resource.splitStateParam(stateParam));
    stateParam = Optional.of(",RUNNING,PAUSED,");
    assertEquals(Sets.newHashSet("RUNNING", "PAUSED"), resource.splitStateParam(stateParam));
    stateParam = Optional.of("PAUSED ,RUNNING");
    assertEquals(Sets.newHashSet("RUNNING", "PAUSED"), resource.splitStateParam(stateParam));
  }
}
