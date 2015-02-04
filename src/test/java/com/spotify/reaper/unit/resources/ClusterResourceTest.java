package com.spotify.reaper.unit.resources;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;

import com.spotify.reaper.AppContext;
import com.spotify.reaper.ReaperApplicationConfiguration;
import com.spotify.reaper.ReaperException;
import com.spotify.reaper.cassandra.JmxConnectionFactory;
import com.spotify.reaper.cassandra.JmxProxy;
import com.spotify.reaper.cassandra.RepairStatusHandler;
import com.spotify.reaper.core.Cluster;
import com.spotify.reaper.resources.ClusterResource;
import com.spotify.reaper.storage.MemoryStorage;

import org.junit.Before;
import org.junit.Test;

import java.net.URI;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import static org.hibernate.validator.internal.util.Contracts.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClusterResourceTest {

  String CLUSTER_NAME = "testcluster";
  String PARTITIONER = "org.apache.cassandra.dht.RandomPartitioner";
  String SEED_HOST = "TestHost";
  URI SAMPLE_URI = URI.create("http://test");

  AppContext context = new AppContext();
  UriInfo uriInfo;

  @Before
  public void setUp() throws Exception {
    context.storage = new MemoryStorage();
    context.config = mock(ReaperApplicationConfiguration.class);

    uriInfo = mock(UriInfo.class);
    when(uriInfo.getAbsolutePath()).thenReturn(SAMPLE_URI);
    when(uriInfo.getBaseUri()).thenReturn(SAMPLE_URI);

    final JmxProxy proxy = mock(JmxProxy.class);
    when(proxy.getClusterName()).thenReturn(CLUSTER_NAME);
    when(proxy.getPartitioner()).thenReturn(PARTITIONER);
    context.jmxConnectionFactory = new JmxConnectionFactory() {
      @Override
      public JmxProxy connect(Optional<RepairStatusHandler> handler, String host)
          throws ReaperException {
        return proxy;
      }
    };
  }

  @Test
  public void testAddCluster() throws Exception {
    ClusterResource clusterResource = new ClusterResource(context);
    Response response = clusterResource.addCluster(uriInfo, Optional.of(SEED_HOST));

    assertEquals(201, response.getStatus());
    assertEquals(1, context.storage.getClusters().size());

    Cluster cluster = context.storage.getCluster(CLUSTER_NAME).get();
    assertNotNull(cluster, "Did not find expected cluster");
    assertEquals(0, context.storage.getRepairRunsForCluster(cluster.getName()).size());
    assertEquals(CLUSTER_NAME, cluster.getName());
    assertEquals(1, cluster.getSeedHosts().size());
    assertEquals(SEED_HOST, cluster.getSeedHosts().iterator().next());
  }

  @Test
  public void testAddExistingCluster() throws Exception {
    Cluster cluster = new Cluster(CLUSTER_NAME, PARTITIONER, Sets.newHashSet(SEED_HOST));
    context.storage.addCluster(cluster);

    ClusterResource clusterResource = new ClusterResource(context);
    Response response = clusterResource.addCluster(uriInfo, Optional.of(SEED_HOST));
    assertEquals(403, response.getStatus());
    assertTrue(response.getEntity() instanceof String);
    String msg = response.getEntity().toString();
    assertTrue(msg.contains("already exists"));
    assertEquals(1, context.storage.getClusters().size());
  }
}
