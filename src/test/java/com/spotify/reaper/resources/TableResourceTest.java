package com.spotify.reaper.resources;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.spotify.reaper.ReaperApplicationConfiguration;
import com.spotify.reaper.ReaperException;
import com.spotify.reaper.cassandra.JmxProxy;
import com.spotify.reaper.cassandra.RepairStatusHandler;
import com.spotify.reaper.core.Cluster;
import com.spotify.reaper.core.ColumnFamily;
import com.spotify.reaper.resources.view.ColumnFamilyStatus;
import com.spotify.reaper.service.JmxConnectionFactory;
import com.spotify.reaper.service.RingRange;
import com.spotify.reaper.storage.IStorage;
import com.spotify.reaper.storage.MemoryStorage;
import org.junit.Before;
import org.junit.Test;

import java.math.BigInteger;
import java.net.URI;
import java.util.Collections;
import java.util.List;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TableResourceTest {

  double REPAIR_INTENSITY = 0.5f;
  int SEGMENT_CNT = 6;
  List<BigInteger> TOKENS = Lists.newArrayList(
      BigInteger.valueOf(0l),
      BigInteger.valueOf(100l),
      BigInteger.valueOf(200l));

  Optional<String> CLUSTER_NAME = Optional.of("TestCluster");
  Optional<String> PARTITIONER = Optional.of("org.apache.cassandra.dht.RandomPartitioner");
  Optional<String> SEED_HOST = Optional.of("TestHost");
  Optional<String> KEYSPACE = Optional.of("testKeyspace");
  Optional<String> TABLE = Optional.of("testTable");

  URI SAMPLE_URI = URI.create("http://test");

  IStorage storage;
  ReaperApplicationConfiguration config;
  JmxConnectionFactory factory;
  UriInfo uriInfo;

  @Before
  public void setUp() throws Exception {
    storage = new MemoryStorage();
    Cluster cluster = new Cluster(CLUSTER_NAME.get(), PARTITIONER.get(),
      Sets.newHashSet(SEED_HOST.get()));
    storage.addCluster(cluster);

    config = mock(ReaperApplicationConfiguration.class);
    when(config.getSegmentCount()).thenReturn(SEGMENT_CNT);
    when(config.getRepairIntensity()).thenReturn(REPAIR_INTENSITY);

    uriInfo = mock(UriInfo.class);
    when(uriInfo.getAbsolutePath()).thenReturn(SAMPLE_URI);
    when(uriInfo.getBaseUri()).thenReturn(SAMPLE_URI);

    final JmxProxy proxy = mock(JmxProxy.class);
    when(proxy.getClusterName()).thenReturn(CLUSTER_NAME.get());
    when(proxy.getPartitioner()).thenReturn(PARTITIONER.get());
    when(proxy.getTokens()).thenReturn(TOKENS);
    when(proxy.tableExists(anyString(), anyString())).thenReturn(Boolean.TRUE);
    when(proxy.isConnectionAlive()).thenReturn(Boolean.TRUE);
    when(proxy.tokenRangeToEndpoint(anyString(), any(RingRange.class))).thenReturn(Collections.singletonList(""));
    factory = new JmxConnectionFactory() {
      @Override
      public JmxProxy create(Optional<RepairStatusHandler> handler, String host) throws ReaperException {
        return proxy;
      }
    };
  }

  @Test
  public void testAddTable() throws Exception {
    TableResource resource = new TableResource(config, storage, factory);
    Response response = resource.addTable(uriInfo, CLUSTER_NAME, KEYSPACE, TABLE);

    assertEquals(201, response.getStatus());
    assertTrue(response.getEntity() instanceof ColumnFamilyStatus);

    assertEquals(1, storage.getClusters().size());
    assertEquals(0, storage.getRepairRunsForCluster(CLUSTER_NAME.get()).size());
    assertEquals(0, storage.getRepairRunIdsForCluster(CLUSTER_NAME.get()).size());

    ColumnFamily cf = storage.getColumnFamily(CLUSTER_NAME.get(), KEYSPACE.get(), TABLE.get());
    assertNotNull("Failed fetch table info from storage", cf);
    assertEquals(SEGMENT_CNT, cf.getSegmentCount());
    assertFalse(cf.isSnapshotRepair());
  }

  @Test
  public void testAddTableArgMissing() throws Exception {
    TableResource resource = new TableResource(config, storage, factory);
    Response response = resource.addTable(uriInfo, CLUSTER_NAME, Optional.<String>absent(), TABLE);
    assertEquals(500, response.getStatus());
    assertTrue(response.getEntity() instanceof String);
    assertTrue(response.getEntity().toString().contains("argument missing"));
  }

  @Test
  public void testAddExistingTable() throws Exception {
    TableResource resource = new TableResource(config, storage, factory);
    resource.addTable(uriInfo, CLUSTER_NAME, KEYSPACE, TABLE);
    Response response = resource.addTable(uriInfo, CLUSTER_NAME, KEYSPACE, TABLE);
    assertEquals(500, response.getStatus());
    assertTrue(response.getEntity() instanceof String);
    assertTrue(response.getEntity().toString().contains("already exists"));
  }

  @Test
  public void testAddTableIfClusterNotExists() throws Exception {
    storage = new MemoryStorage();
    TableResource resource = new TableResource(config, storage, factory);
    Response response = resource.addTable(uriInfo, CLUSTER_NAME, KEYSPACE, TABLE);
    assertEquals(500, response.getStatus());
    assertTrue(response.getEntity() instanceof String);
    assertTrue(response.getEntity().toString().contains("Failed to fetch cluster"));
  }

}
