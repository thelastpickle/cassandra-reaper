package io.cassandrareaper.resources;

import io.cassandrareaper.AppContext;
import io.cassandrareaper.ReaperApplicationConfiguration;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.RepairRun;
import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.resources.view.RepairRunStatus;
import io.cassandrareaper.service.RepairManager;
import io.cassandrareaper.storage.MemoryStorageFacade;
import io.cassandrareaper.storage.repairrun.IRepairRunDao;

import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriBuilder;
import jakarta.ws.rs.core.UriInfo;
import org.apache.cassandra.repair.RepairParallelism;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RepairRunResourceAdditionalTest {

  private static final String CLUSTER_NAME = "testcluster";
  private static final String KEYSPACE = "testkeyspace";
  private static final String OWNER = "testowner";
  private static final Set<String> TABLES = Sets.newHashSet("table1");
  private static final int SEGMENT_CNT = 6;
  private static final double REPAIR_INTENSITY = 0.5;
  private static final RepairParallelism REPAIR_PARALLELISM = RepairParallelism.SEQUENTIAL;

  private AppContext context;
  private RepairRunResource resource;
  private IRepairRunDao repairRunDao;
  private UriInfo uriInfo;

  @Before
  public void setUp() throws Exception {
    context = new AppContext();
    context.config = new ReaperApplicationConfiguration();
    context.storage = new MemoryStorageFacade();
    repairRunDao = context.storage.getRepairRunDao();

    context.repairManager =
        RepairManager.create(
            context, Executors.newScheduledThreadPool(1), 10, TimeUnit.SECONDS, 1, repairRunDao);

    // Add a test cluster
    Cluster cluster =
        Cluster.builder()
            .withName(CLUSTER_NAME)
            .withPartitioner("org.apache.cassandra.dht.RandomPartitioner")
            .withSeedHosts(ImmutableSet.of("127.0.0.1"))
            .withState(Cluster.State.ACTIVE)
            .build();
    context.storage.getClusterDao().addCluster(cluster);

    // Configure
    context.config.setSegmentCount(SEGMENT_CNT);
    context.config.setRepairIntensity(REPAIR_INTENSITY);
    context.config.setHangingRepairTimeoutMins(30);
    context.config.setPurgeRecordsAfterInDays(30);

    uriInfo = mock(UriInfo.class);
    when(uriInfo.getBaseUriBuilder()).thenReturn(UriBuilder.fromUri(URI.create("http://test/")));

    resource = new RepairRunResource(context, repairRunDao);
  }

  @After
  public void tearDown() {
    DateTimeUtils.setCurrentMillisSystem();
  }

  @Test
  public void testDeleteRepairRun_Success() {
    // Given
    RepairUnit unit = createRepairUnit();
    RepairRun run = createRepairRun(unit, RepairRun.RunState.DONE);

    // When
    Response response = resource.deleteRepairRun(run.getId(), Optional.of(OWNER));

    // Then
    assertEquals(202, response.getStatus());
    assertThat(repairRunDao.getRepairRun(run.getId())).isEmpty();
  }

  @Test
  public void testDeleteRepairRun_MissingOwner() {
    // Given
    RepairUnit unit = createRepairUnit();
    RepairRun run = createRepairRun(unit, RepairRun.RunState.DONE);

    // When
    Response response = resource.deleteRepairRun(run.getId(), Optional.empty());

    // Then
    assertEquals(400, response.getStatus());
    assertThat(response.getEntity().toString()).contains("owner");
  }

  @Test
  public void testDeleteRepairRun_RunningState() {
    // Given
    RepairUnit unit = createRepairUnit();
    RepairRun run = createRepairRun(unit, RepairRun.RunState.RUNNING);

    // When
    Response response = resource.deleteRepairRun(run.getId(), Optional.of(OWNER));

    // Then
    assertEquals(409, response.getStatus());
    assertThat(response.getEntity().toString()).contains("currently running");
  }

  @Test
  public void testDeleteRepairRun_WrongOwner() {
    // Given
    RepairUnit unit = createRepairUnit();
    RepairRun run = createRepairRun(unit, RepairRun.RunState.DONE);

    // When
    Response response = resource.deleteRepairRun(run.getId(), Optional.of("wrongowner"));

    // Then
    assertEquals(409, response.getStatus());
    assertThat(response.getEntity().toString()).contains("not owned by the user");
  }

  @Test
  public void testDeleteRepairRun_NotFound() {
    // When
    Response response = resource.deleteRepairRun(UUID.randomUUID(), Optional.of(OWNER));

    // Then
    assertEquals(404, response.getStatus());
  }

  @Test
  public void testPurgeRepairRuns() throws Exception {
    // Given - create old repair runs
    DateTimeUtils.setCurrentMillisFixed(DateTime.now().minusDays(40).getMillis());
    RepairUnit unit = createRepairUnit();
    createRepairRun(unit, RepairRun.RunState.DONE);
    createRepairRun(unit, RepairRun.RunState.DONE);
    DateTimeUtils.setCurrentMillisSystem();

    // When
    Response response = resource.purgeRepairRuns();

    // Then
    assertEquals(200, response.getStatus());
    assertThat(response.getEntity()).isInstanceOf(Integer.class);
  }

  @Test
  public void testGetRepairRunsForCluster() {
    // Given
    RepairUnit unit = createRepairUnit();
    createRepairRun(unit, RepairRun.RunState.RUNNING);
    createRepairRun(unit, RepairRun.RunState.DONE);

    // When
    Response response = resource.getRepairRunsForCluster(CLUSTER_NAME, Optional.of(10));

    // Then
    assertEquals(200, response.getStatus());
    assertThat(response.getEntity()).isInstanceOf(Collection.class);
    Collection<RepairRunStatus> runs = (Collection<RepairRunStatus>) response.getEntity();
    assertEquals(2, runs.size());
  }

  @Test
  public void testListRepairRuns_WithState() {
    // Given
    RepairUnit unit = createRepairUnit();
    createRepairRun(unit, RepairRun.RunState.RUNNING);
    createRepairRun(unit, RepairRun.RunState.DONE);
    createRepairRun(unit, RepairRun.RunState.PAUSED);

    // When
    Response response =
        resource.listRepairRuns(
            Optional.of("RUNNING,PAUSED"), Optional.empty(), Optional.empty(), Optional.of(10));

    // Then
    assertEquals(200, response.getStatus());
    assertThat(response.getEntity()).isInstanceOf(List.class);
    List<RepairRunStatus> runs = (List<RepairRunStatus>) response.getEntity();
    assertEquals(2, runs.size());
  }

  @Test
  public void testListRepairRuns_InvalidState() {
    // When
    Response response =
        resource.listRepairRuns(
            Optional.of("INVALID_STATE"), Optional.empty(), Optional.empty(), Optional.of(10));

    // Then
    assertEquals(400, response.getStatus());
  }

  @Test
  public void testCheckRequestForAddRepair_MissingKeyspace() throws Exception {
    // When
    Response response =
        RepairRunResource.checkRequestForAddRepair(
            context,
            Optional.of(CLUSTER_NAME),
            Optional.empty(), // missing keyspace
            Optional.empty(),
            Optional.of(OWNER),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    // Then
    assertNotNull(response);
    assertEquals(400, response.getStatus());
    assertThat(response.getEntity().toString()).contains("keyspace");
  }

  @Test
  public void testCheckRequestForAddRepair_InvalidIntensity() throws Exception {
    // When
    Response response =
        RepairRunResource.checkRequestForAddRepair(
            context,
            Optional.of(CLUSTER_NAME),
            Optional.of(KEYSPACE),
            Optional.empty(),
            Optional.of(OWNER),
            Optional.empty(),
            Optional.empty(),
            Optional.of("2.0"), // invalid intensity > 1.0
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    // Then
    assertNotNull(response);
    assertEquals(400, response.getStatus());
    assertThat(response.getEntity().toString()).contains("intensity");
  }

  @Test
  public void testCheckRequestForAddRepair_InvalidIncrementalRepair() throws Exception {
    // When
    Response response =
        RepairRunResource.checkRequestForAddRepair(
            context,
            Optional.of(CLUSTER_NAME),
            Optional.of(KEYSPACE),
            Optional.empty(),
            Optional.of(OWNER),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.of("INVALID"), // invalid boolean
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    // Then
    assertNotNull(response);
    assertEquals(400, response.getStatus());
    assertThat(response.getEntity().toString()).contains("incrementalRepair");
  }

  @Test
  public void testCheckRequestForAddRepair_InvalidForceParam() throws Exception {
    // When
    Response response =
        RepairRunResource.checkRequestForAddRepair(
            context,
            Optional.of(CLUSTER_NAME),
            Optional.of(KEYSPACE),
            Optional.empty(),
            Optional.of(OWNER),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.of("INVALID"), // invalid force param
            Optional.empty());

    // Then
    assertNotNull(response);
    assertEquals(400, response.getStatus());
    assertThat(response.getEntity().toString()).contains("force");
  }

  @Test
  public void testCheckRequestForAddRepair_ZeroTimeout() throws Exception {
    // When
    Response response =
        RepairRunResource.checkRequestForAddRepair(
            context,
            Optional.of(CLUSTER_NAME),
            Optional.of(KEYSPACE),
            Optional.empty(),
            Optional.of(OWNER),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.of(0)); // zero timeout

    // Then
    assertNotNull(response);
    assertEquals(400, response.getStatus());
    assertThat(response.getEntity().toString()).contains("timeout");
  }

  @Test
  public void testCheckRequestForAddRepair_NonExistentCluster() throws Exception {
    // When
    Response response =
        RepairRunResource.checkRequestForAddRepair(
            context,
            Optional.of("nonexistent"),
            Optional.of(KEYSPACE),
            Optional.empty(),
            Optional.of(OWNER),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    // Then
    assertNotNull(response);
    assertEquals(404, response.getStatus());
    assertThat(response.getEntity().toString()).contains("No cluster found");
  }

  @Test
  public void testCheckRequestForAddRepair_ValidRequest() throws Exception {
    // When
    Response response =
        RepairRunResource.checkRequestForAddRepair(
            context,
            Optional.of(CLUSTER_NAME),
            Optional.of(KEYSPACE),
            Optional.empty(),
            Optional.of(OWNER),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    // Then
    assertNull(response); // null means valid request
  }

  @Test
  public void testSplitStateParam_Empty() {
    Set<String> states = RepairRunResource.splitStateParam(Optional.empty());
    assertThat(states).isEmpty();
  }

  @Test
  public void testSplitStateParam_Valid() {
    Set<String> states = RepairRunResource.splitStateParam(Optional.of("RUNNING,PAUSED"));
    assertThat(states).containsExactlyInAnyOrder("RUNNING", "PAUSED");
  }

  @Test
  public void testSplitStateParam_Invalid() {
    Set<String> states = RepairRunResource.splitStateParam(Optional.of("RUNNING,INVALID"));
    assertNull(states);
  }

  // Helper methods
  private RepairUnit createRepairUnit() {
    RepairUnit.Builder builder =
        RepairUnit.builder()
            .clusterName(CLUSTER_NAME)
            .keyspaceName(KEYSPACE)
            .columnFamilies(TABLES)
            .incrementalRepair(false)
            .subrangeIncrementalRepair(false)
            .nodes(Collections.emptySet())
            .datacenters(Collections.emptySet())
            .blacklistedTables(Collections.emptySet())
            .repairThreadCount(2)
            .timeout(30);

    return context.storage.getRepairUnitDao().addRepairUnit(builder);
  }

  private RepairRun createRepairRun(RepairUnit unit, RepairRun.RunState state) {
    RepairRun.Builder builder =
        RepairRun.builder(CLUSTER_NAME, unit.getId())
            .intensity(REPAIR_INTENSITY)
            .segmentCount(SEGMENT_CNT)
            .repairParallelism(REPAIR_PARALLELISM)
            .tables(TABLES)
            .runState(state)
            .owner(OWNER);

    // Set startTime and endTime based on state
    if (state != RepairRun.RunState.NOT_STARTED) {
      builder = builder.startTime(DateTime.now());
      if (state == RepairRun.RunState.DONE
          || state == RepairRun.RunState.ERROR
          || state == RepairRun.RunState.ABORTED) {
        builder = builder.endTime(DateTime.now().plusMinutes(10));
      }
      if (state == RepairRun.RunState.PAUSED) {
        builder = builder.pauseTime(DateTime.now().plusMinutes(5));
      }
    }

    return repairRunDao.addRepairRun(builder, Lists.newArrayList());
  }
}
