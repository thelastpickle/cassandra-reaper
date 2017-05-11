package com.spotify.reaper.unit.resources;

import com.datastax.driver.core.utils.UUIDs;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.spotify.reaper.AppContext;
import com.spotify.reaper.ReaperApplicationConfiguration;
import com.spotify.reaper.ReaperException;
import com.spotify.reaper.cassandra.JmxConnectionFactory;
import com.spotify.reaper.cassandra.JmxProxy;
import com.spotify.reaper.cassandra.RepairStatusHandler;
import com.spotify.reaper.core.Cluster;
import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.core.RepairSegment;
import com.spotify.reaper.core.RepairUnit;
import com.spotify.reaper.resources.RepairRunResource;
import com.spotify.reaper.resources.view.RepairRunStatus;
import com.spotify.reaper.service.RepairManager;
import com.spotify.reaper.service.RingRange;
import com.spotify.reaper.service.SegmentRunner;
import com.spotify.reaper.storage.MemoryStorage;
import com.spotify.reaper.unit.service.RepairRunnerTest;
import org.apache.cassandra.repair.RepairParallelism;
import org.joda.time.DateTimeUtils;
import org.junit.Before;
import org.junit.Test;

import java.math.BigInteger;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyCollectionOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RepairRunResourceTest {

  static final String CLUSTER_NAME = "testcluster";
  static final String PARTITIONER = "org.apache.cassandra.dht.RandomPartitioner";
  static final String SEED_HOST = "TestHost";
  static final String KEYSPACE = "testKeyspace";
  static final Boolean INCREMENTAL = false;
  static final Set<String> TABLES = Sets.newHashSet("testTable");
  static final String OWNER = "test";
  int THREAD_CNT = 1;
  int REPAIR_TIMEOUT_S = 60;
  int RETRY_DELAY_S = 10;
  long TIME_CREATE = 42l;
  long TIME_START = 43l;
  URI SAMPLE_URI = URI.create("http://test");
  int SEGMENT_CNT = 6;
  double REPAIR_INTENSITY = 0.5f;
  RepairParallelism REPAIR_PARALLELISM = RepairParallelism.SEQUENTIAL;
  List<BigInteger> TOKENS = Lists.newArrayList(BigInteger.valueOf(0l), BigInteger.valueOf(100l),
                                               BigInteger.valueOf(200l));

  AppContext context;
  UriInfo uriInfo;

  @Before
  public void setUp() throws Exception {
    SegmentRunner.segmentRunners.clear();

    context = new AppContext();
    context.repairManager = new RepairManager();
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
    when(proxy.getTokens()).thenReturn(TOKENS);
    when(proxy.tableExists(anyString(), anyString())).thenReturn(Boolean.TRUE);
    when(proxy.isConnectionAlive()).thenReturn(Boolean.TRUE);
    when(proxy.tokenRangeToEndpoint(anyString(), any(RingRange.class))).thenReturn(
        Collections.singletonList(""));
    when(proxy.getRangeToEndpointMap(anyString())).thenReturn(RepairRunnerTest.sixNodeCluster());
    when(proxy.triggerRepair(any(BigInteger.class), any(BigInteger.class), anyString(),
        any(RepairParallelism.class), anyCollectionOf(String.class), anyBoolean())).thenReturn(1);

    context.jmxConnectionFactory = new JmxConnectionFactory() {
      @Override
      public JmxProxy connect(Optional<RepairStatusHandler> handler, String host)
          throws ReaperException {
        return proxy;
      }
    };

    RepairUnit.Builder repairUnitBuilder = new RepairUnit.Builder(CLUSTER_NAME, KEYSPACE, TABLES, INCREMENTAL);
    context.storage.addRepairUnit(repairUnitBuilder);
  }

  private Response addDefaultRepairRun(RepairRunResource resource) {
    return addRepairRun(resource, uriInfo, CLUSTER_NAME, KEYSPACE, TABLES, OWNER, "", SEGMENT_CNT);
  }

  private Response addRepairRun(RepairRunResource resource, UriInfo uriInfo,
                                String clusterName, String keyspace, Set<String> columnFamilies,
                                String owner, String cause, Integer segments) {
    return resource.addRepairRun(uriInfo,
                                 clusterName == null ? Optional.<String>absent()
                                                     : Optional.of(clusterName),
                                 keyspace == null ? Optional.<String>absent()
                                                  : Optional.of(keyspace),
                                 columnFamilies == null ? Optional.<String>absent()
                                                        : Optional
                                     .of(columnFamilies.iterator().next()),
                                 owner == null ? Optional.<String>absent() : Optional.of(owner),
                                 cause == null ? Optional.<String>absent() : Optional.of(cause),
                                 segments == null ? Optional.<Integer>absent()
                                                  : Optional.of(segments),
                                 Optional.of(REPAIR_PARALLELISM.name()),
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

    // apparently, tokens [0, 100, 200] and 6 requested segments causes generating 8 RepairSegments
    assertEquals(8, context.storage.getSegmentAmountForRepairRunWithState(run.getId(),
        RepairSegment.State.NOT_STARTED));

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
    context.repairManager.initializeThreadPool(THREAD_CNT, REPAIR_TIMEOUT_S, TimeUnit.SECONDS,
                                               RETRY_DELAY_S, TimeUnit.SECONDS);
    RepairRunResource resource = new RepairRunResource(context);
    Response response = addDefaultRepairRun(resource);
    RepairRunStatus repairRunStatus = (RepairRunStatus) response.getEntity();
    UUID runId = repairRunStatus.getId();

    DateTimeUtils.setCurrentMillisFixed(TIME_START);
    Optional<String> newState = Optional.of(RepairRun.RunState.RUNNING.toString());
    resource.modifyRunState(uriInfo, runId, newState);
    Thread.sleep(1000);
    response = resource.modifyRunState(uriInfo, runId, newState);
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
  }
  
  @Test
  public void testTriggerNewRunAlreadyRunningRun() throws InterruptedException, ReaperException {
    DateTimeUtils.setCurrentMillisFixed(TIME_CREATE);
    context.repairManager.initializeThreadPool(THREAD_CNT, REPAIR_TIMEOUT_S, TimeUnit.SECONDS,
                                               RETRY_DELAY_S, TimeUnit.SECONDS);
    RepairRunResource resource = new RepairRunResource(context);
    Response response = addDefaultRepairRun(resource);
    RepairRunStatus repairRunStatus = (RepairRunStatus) response.getEntity();
    UUID runId = repairRunStatus.getId();

    DateTimeUtils.setCurrentMillisFixed(TIME_START);
    Optional<String> newState = Optional.of(RepairRun.RunState.RUNNING.toString());
    resource.modifyRunState(uriInfo, runId, newState);
    Thread.sleep(1000);
    response = resource.modifyRunState(uriInfo, runId, newState);
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    
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
    Response response = addRepairRun(resource, uriInfo, CLUSTER_NAME, null,
                                     TABLES, OWNER, null, SEGMENT_CNT);
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    assertTrue(response.getEntity() instanceof String);
  }

  @Test
  public void testTriggerRunMissingArgument() {
    context.repairManager.initializeThreadPool(THREAD_CNT, REPAIR_TIMEOUT_S, TimeUnit.SECONDS,
                                               RETRY_DELAY_S, TimeUnit.SECONDS);
    RepairRunResource resource = new RepairRunResource(context);
    Response response = addRepairRun(resource, uriInfo, CLUSTER_NAME, null, TABLES, OWNER,
                                     null, SEGMENT_CNT);
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
    RepairRunStatus repairRunStatus = (RepairRunStatus) response.getEntity();
    UUID runId = repairRunStatus.getId();

    response = resource.modifyRunState(uriInfo, runId,
                                       Optional.of(RepairRun.RunState.PAUSED.toString()));
    Thread.sleep(200);

    assertEquals(400, response.getStatus());
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
