package com.spotify.reaper.resources;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.spotify.reaper.ReaperApplicationConfiguration;
import com.spotify.reaper.ReaperException;
import com.spotify.reaper.cassandra.JmxProxy;
import com.spotify.reaper.cassandra.RepairStatusHandler;
import com.spotify.reaper.core.ColumnFamily;
import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.core.RepairSegment;
import com.spotify.reaper.resources.view.ColumnFamilyStatus;
import com.spotify.reaper.service.JmxConnectionFactory;
import com.spotify.reaper.service.RepairRunner;
import com.spotify.reaper.storage.IStorage;
import com.spotify.reaper.storage.MemoryStorage;
import org.joda.time.DateTimeUtils;
import org.junit.Before;
import org.junit.Test;

import java.math.BigInteger;
import java.net.URI;
import java.util.Collection;
import java.util.List;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TableResourceTest {

  long TIME_CREATE = 42l;

  int THREAD_CNT = 1;
  int REPAIR_TIMEOUT_S = 60;

  double REPAIR_INTENSITY = 0.5f;
  int SEGMENT_CNT = 6;
  List<BigInteger> TOKENS = Lists.newArrayList(
      BigInteger.valueOf(0l),
      BigInteger.valueOf(100l),
      BigInteger.valueOf(200l));

  String CLUSTER_NAME = "TestCluster";
  String PARTITIONER = "org.apache.cassandra.dht.RandomPartitioner";
  String SEED_HOST = "TestHost";
  String KEYSPACE = "testKeyspace";
  String TABLE = "testTable";

  URI SAMPLE_URI = URI.create("http://test");

  IStorage storage;
  ReaperApplicationConfiguration config;
  JmxConnectionFactory factory;
  UriInfo uriInfo;

  @Before
  public void setUp() throws Exception {
    storage = new MemoryStorage();
    config = mock(ReaperApplicationConfiguration.class);
    when(config.getSegmentCount()).thenReturn(SEGMENT_CNT);
    when(config.getRepairIntensity()).thenReturn(REPAIR_INTENSITY);

    uriInfo = mock(UriInfo.class);
    when(uriInfo.getAbsolutePath()).thenReturn(SAMPLE_URI);
    when(uriInfo.getBaseUri()).thenReturn(SAMPLE_URI);

    final JmxProxy proxy = mock(JmxProxy.class);
    when(proxy.getClusterName()).thenReturn(CLUSTER_NAME);
    when(proxy.getPartitioner()).thenReturn(PARTITIONER);
    when(proxy.getTokens()).thenReturn(TOKENS);
    when(proxy.tableExists(anyString(), anyString())).thenReturn(Boolean.TRUE);
    factory = new JmxConnectionFactory() {
      @Override
      public JmxProxy create(String host) throws ReaperException {
        return proxy;
      }
      @Override
      public JmxProxy connectAny(Optional<RepairStatusHandler> handler, Collection<String> hosts) {
        return proxy;
      }
    };
  }

  @Test
  public void testAddTableWithoutTrigger() throws Exception {

    TableResource resource = new TableResource(config, storage, factory);
    Optional<String> clusterName = Optional.of(CLUSTER_NAME);
    Optional<String> seedHost = Optional.of(SEED_HOST);
    Optional<String> keyspace = Optional.of(KEYSPACE);
    Optional<String> table = Optional.of(TABLE);
    Optional<Boolean> startRepair = Optional.absent();
    Optional<String> owner = Optional.of("test");
    Optional<String> cause = Optional.of("tetsCase");

    Response response = resource.addTable(uriInfo, clusterName, seedHost, keyspace, table,
        startRepair, owner, cause);

    assertEquals(201, response.getStatus());
    assertTrue(response.getEntity() instanceof ColumnFamilyStatus);

    assertEquals(1, storage.getClusters().size());
    assertEquals(0, storage.getRepairRunsForCluster(CLUSTER_NAME).size());
    assertEquals(0, storage.getRepairRunIdsForCluster(CLUSTER_NAME).size());

    ColumnFamily cf = storage.getColumnFamily(CLUSTER_NAME, KEYSPACE, TABLE);
    assertNotNull("Failed fetch table info from storage", cf);
    assertEquals(SEGMENT_CNT, cf.getSegmentCount());
    assertFalse(cf.isSnapshotRepair());
  }

  @Test
  public void testAddTableWithTrigger() throws Exception {

    TableResource resource = new TableResource(config, storage, factory);
    Optional<String> clusterName = Optional.of(CLUSTER_NAME);
    Optional<String> seedHost = Optional.of(SEED_HOST);
    Optional<String> keyspace = Optional.of(KEYSPACE);
    Optional<String> table = Optional.of(TABLE);
    Optional<Boolean> startRepair = Optional.of(Boolean.TRUE);
    Optional<String> owner = Optional.of("test");
    Optional<String> cause = Optional.of("tetsCase");

    RepairRunner.initializeThreadPool(THREAD_CNT, REPAIR_TIMEOUT_S);
    DateTimeUtils.setCurrentMillisFixed(TIME_CREATE);

    Response response = resource.addTable(uriInfo, clusterName, seedHost, keyspace, table,
        startRepair, owner, cause);

    assertEquals(201, response.getStatus());
    assertTrue(response.getEntity() instanceof ColumnFamilyStatus);

    // give the runner time to start the repair run
    Thread.sleep(200);

    assertEquals(1, storage.getClusters().size());
    assertEquals(1, storage.getRepairRunsForCluster(CLUSTER_NAME).size());
    assertEquals(1, storage.getRepairRunIdsForCluster(CLUSTER_NAME).size());
    Long runId = storage.getRepairRunIdsForCluster(CLUSTER_NAME).iterator().next();
    RepairRun run = storage.getRepairRun(runId);
    assertEquals(RepairRun.RunState.RUNNING, run.getRunState());
    assertEquals(TIME_CREATE, run.getStartTime().getMillis());
    assertEquals(REPAIR_INTENSITY, run.getIntensity(), 0.0f);
    assertNull(run.getEndTime());

    // apparently, tokens [0, 100, 200] and 6 requested segments causes generating 8 RepairSegments
    assertEquals(1, storage.getSegmentAmountForRepairRun(run.getId(), RepairSegment.State.RUNNING));
    assertEquals(7,
        storage.getSegmentAmountForRepairRun(run.getId(), RepairSegment.State.NOT_STARTED));
  }

}
