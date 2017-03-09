package com.spotify.reaper.unit.resources;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.spotify.reaper.AppContext;
import com.spotify.reaper.ReaperException;
import com.spotify.reaper.cassandra.JmxConnectionFactory;
import com.spotify.reaper.cassandra.JmxProxy;
import com.spotify.reaper.cassandra.RepairStatusHandler;
import com.spotify.reaper.core.Cluster;
import com.spotify.reaper.resources.ClusterResource;
import com.spotify.reaper.storage.MemoryStorage;
import com.spotify.reaper.unit.service.TestRepairConfiguration;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.time.Duration;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import static org.fest.assertions.api.Assertions.assertThat;
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
  JmxProxy jmxProxy;

  @Before
  public void setUp() throws Exception {
    context.storage = new MemoryStorage();
    context.config = TestRepairConfiguration.defaultConfig();

    uriInfo = mock(UriInfo.class);
    when(uriInfo.getAbsolutePath()).thenReturn(SAMPLE_URI);
    when(uriInfo.getBaseUri()).thenReturn(SAMPLE_URI);

    jmxProxy = mock(JmxProxy.class);
    when(jmxProxy.getClusterName()).thenReturn(CLUSTER_NAME);
    when(jmxProxy.getPartitioner()).thenReturn(PARTITIONER);
    context.jmxConnectionFactory = new JmxConnectionFactory() {
      @Override
      public JmxProxy connect(Optional<RepairStatusHandler> handler, String host)
          throws ReaperException {
        return jmxProxy;
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

  @Test
  public void testGetNonExistingCluster() {
    ClusterResource clusterResource = new ClusterResource(context);
    Response response = clusterResource.getCluster("i_dont_exist", Optional.<Integer>absent());
    assertEquals(404, response.getStatus());
  }

  @Test
  public void testGetExistingCluster() {
    final String I_DO_EXIST = "i_do_exist";
    Cluster cluster = new Cluster(I_DO_EXIST, PARTITIONER, Sets.newHashSet(SEED_HOST));
    context.storage.addCluster(cluster);

    ClusterResource clusterResource = new ClusterResource(context);
    Response response = clusterResource.getCluster(I_DO_EXIST, Optional.<Integer>absent());
    assertEquals(200, response.getStatus());
  }

  @Test
  public void testModifyClusterSeeds() {
    ClusterResource clusterResource = new ClusterResource(context);
    clusterResource.addCluster(uriInfo, Optional.of(SEED_HOST));

    Response response = clusterResource.modifyClusterSeed(uriInfo, CLUSTER_NAME,
        Optional.of(SEED_HOST + 1));

    assertEquals(200, response.getStatus());
    assertEquals(1, context.storage.getClusters().size());

    Cluster cluster = context.storage.getCluster(CLUSTER_NAME).get();
    assertEquals(1, cluster.getSeedHosts().size());
    assertEquals(SEED_HOST + 1, cluster.getSeedHosts().iterator().next());


    response = clusterResource.modifyClusterSeed(uriInfo, CLUSTER_NAME, Optional.of(SEED_HOST + 1));
    assertEquals(304, response.getStatus());
  }

  @Test
  public void addingAClusterAutomaticallySetupSchedulingRepairsWhenEnabled() throws Exception {
    when(jmxProxy.getKeyspaces()).thenReturn(Lists.newArrayList("keyspace1"));
    when(jmxProxy.getTableNamesForKeyspace("keyspace1")).thenReturn(Sets.newHashSet("table1"));

    context.config = TestRepairConfiguration.defaultConfigBuilder()
        .withAutoScheduling(TestRepairConfiguration.defaultAutoSchedulingConfigBuilder()
            .thatIsEnabled()
            .withTimeBeforeFirstSchedule(Duration.ofMinutes(1))
            .build())
        .build();

    ClusterResource clusterResource = new ClusterResource(context);
    Response response = clusterResource.addCluster(uriInfo, Optional.of(SEED_HOST));

    assertEquals(201, response.getStatus());
    assertThat(context.storage.getAllRepairSchedules()).hasSize(1);
    assertThat(context.storage.getRepairSchedulesForClusterAndKeyspace(CLUSTER_NAME, "keyspace1")).hasSize(1);
  }

}
