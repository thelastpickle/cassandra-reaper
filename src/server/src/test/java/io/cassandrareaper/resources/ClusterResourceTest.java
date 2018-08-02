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
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.jmx.JmxConnectionFactory;
import io.cassandrareaper.jmx.JmxProxy;
import io.cassandrareaper.service.TestRepairConfiguration;
import io.cassandrareaper.storage.MemoryStorage;

import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.Executors;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.eclipse.jetty.http.HttpStatus;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class ClusterResourceTest {

  static final String CLUSTER_NAME = "testcluster";
  static final String PARTITIONER = "org.apache.cassandra.dht.RandomPartitioner";
  static final String SEED_HOST = "TestHost";
  static final URI SAMPLE_URI = URI.create("http://reaper_host/cluster/");
  static final String I_DO_EXIST = "i_do_exist";
  static final String I_DONT_EXIST = "i_dont_exist";

  @Before
  public void setUp() throws Exception {
  }

  @Test
  public void testAddCluster() throws Exception {
    final MockObjects mocks = initMocks();

    ClusterResource clusterResource = new ClusterResource(mocks.context, Executors.newFixedThreadPool(2));
    Response response = clusterResource.addOrUpdateCluster(mocks.uriInfo, Optional.of(SEED_HOST));

    assertEquals(HttpStatus.CREATED_201, response.getStatus());
    assertEquals(1, mocks.context.storage.getClusters().size());

    Cluster cluster = mocks.context.storage.getCluster(CLUSTER_NAME).get();
    assertNotNull("Did not find expected cluster", cluster);
    assertEquals(0, mocks.context.storage.getRepairRunsForCluster(cluster.getName(), Optional.of(1)).size());
    assertEquals(CLUSTER_NAME, cluster.getName());
    assertEquals(1, cluster.getSeedHosts().size());
    assertEquals(SEED_HOST, cluster.getSeedHosts().iterator().next());
  }

  @Test
  public void testAddExistingCluster() throws Exception {
    final MockObjects mocks = initMocks();
    when(mocks.jmxProxy.getLiveNodes()).thenReturn(Arrays.asList(SEED_HOST));

    Cluster cluster = new Cluster(CLUSTER_NAME, PARTITIONER, Sets.newHashSet(SEED_HOST));
    mocks.context.storage.addCluster(cluster);

    ClusterResource clusterResource = new ClusterResource(mocks.context, Executors.newFixedThreadPool(2));
    Response response = clusterResource.addOrUpdateCluster(mocks.uriInfo, Optional.of(SEED_HOST));

    assertEquals(HttpStatus.NO_CONTENT_204, response.getStatus());
    assertTrue(response.getLocation().toString().endsWith("/cluster/" + cluster.getName()));
    assertEquals(1, mocks.context.storage.getClusters().size());

    cluster = mocks.context.storage.getCluster(CLUSTER_NAME).get();
    assertNotNull("Did not find expected cluster", cluster);
    assertEquals(0, mocks.context.storage.getRepairRunsForCluster(cluster.getName(), Optional.of(1)).size());
    assertEquals(CLUSTER_NAME, cluster.getName());
    assertEquals(1, cluster.getSeedHosts().size());
    assertEquals(SEED_HOST, cluster.getSeedHosts().iterator().next());
  }

  @Test
  public void testAddExistingClusterWithClusterName() throws Exception {
    final MockObjects mocks = initMocks();
    when(mocks.jmxProxy.getLiveNodes()).thenReturn(Arrays.asList(SEED_HOST));

    Cluster cluster = new Cluster(CLUSTER_NAME, PARTITIONER, Sets.newHashSet(SEED_HOST));
    mocks.context.storage.addCluster(cluster);

    ClusterResource clusterResource = new ClusterResource(mocks.context, Executors.newFixedThreadPool(2));
    Response response = clusterResource.addOrUpdateCluster(mocks.uriInfo, CLUSTER_NAME, Optional.of(SEED_HOST));

    assertEquals(HttpStatus.NO_CONTENT_204, response.getStatus());
    assertTrue(response.getLocation().toString().endsWith("/cluster/" + cluster.getName()));
    assertEquals(1, mocks.context.storage.getClusters().size());

    cluster = mocks.context.storage.getCluster(CLUSTER_NAME).get();
    assertNotNull("Did not find expected cluster", cluster);
    assertEquals(0, mocks.context.storage.getRepairRunsForCluster(cluster.getName(), Optional.of(1)).size());
    assertEquals(CLUSTER_NAME, cluster.getName());
    assertEquals(1, cluster.getSeedHosts().size());
    assertEquals(SEED_HOST, cluster.getSeedHosts().iterator().next());
  }

  @Test
  public void testGetNonExistingCluster() throws ReaperException {
    final MockObjects mocks = initMocks();
    when(mocks.jmxProxy.getLiveNodes()).thenReturn(Arrays.asList(SEED_HOST));

    ClusterResource clusterResource = new ClusterResource(mocks.context, Executors.newFixedThreadPool(2));
    Response response = clusterResource.getCluster(I_DONT_EXIST, Optional.<Integer>absent());
    assertEquals(HttpStatus.NOT_FOUND_404, response.getStatus());
  }

  @Test
  public void testGetExistingCluster() throws ReaperException {
    final MockObjects mocks = initMocks();
    when(mocks.jmxProxy.getLiveNodes()).thenReturn(Arrays.asList(SEED_HOST));

    Cluster cluster = new Cluster(I_DO_EXIST, PARTITIONER, Sets.newHashSet(SEED_HOST));
    mocks.context.storage.addCluster(cluster);

    ClusterResource clusterResource = new ClusterResource(mocks.context, Executors.newFixedThreadPool(2));
    Response response = clusterResource.getCluster(I_DO_EXIST, Optional.<Integer>absent());
    assertEquals(HttpStatus.OK_200, response.getStatus());
  }

  @Test
  public void testModifyClusterSeeds() throws ReaperException {
    final MockObjects mocks = initMocks();

    ClusterResource clusterResource = new ClusterResource(mocks.context, Executors.newFixedThreadPool(2));
    clusterResource.addOrUpdateCluster(mocks.uriInfo, Optional.of(SEED_HOST));
    doReturn(Arrays.asList(SEED_HOST + 1)).when(mocks.jmxProxy).getLiveNodes();

    Response response = clusterResource.addOrUpdateCluster(mocks.uriInfo, Optional.of(SEED_HOST + 1));

    assertEquals(HttpStatus.OK_200, response.getStatus());
    assertTrue(response.getLocation().toString().endsWith("/cluster/" + CLUSTER_NAME));
    assertEquals(1, mocks.context.storage.getClusters().size());

    Cluster cluster = mocks.context.storage.getCluster(CLUSTER_NAME).get();
    assertEquals(1, cluster.getSeedHosts().size());
    assertEquals(SEED_HOST + 1, cluster.getSeedHosts().iterator().next());

    response = clusterResource.addOrUpdateCluster(mocks.uriInfo, CLUSTER_NAME, Optional.of(SEED_HOST + 1));
    assertEquals(HttpStatus.NO_CONTENT_204, response.getStatus());
    assertTrue(response.getLocation().toString().endsWith("/cluster/" + cluster.getName()));
  }

  @Test
  public void testModifyClusterSeedsWithClusterName() throws ReaperException {
    final MockObjects mocks = initMocks();

    ClusterResource clusterResource = new ClusterResource(mocks.context, Executors.newFixedThreadPool(2));
    clusterResource.addOrUpdateCluster(mocks.uriInfo, Optional.of(SEED_HOST));
    doReturn(Arrays.asList(SEED_HOST + 1)).when(mocks.jmxProxy).getLiveNodes();

    Response response = clusterResource.addOrUpdateCluster(mocks.uriInfo, CLUSTER_NAME, Optional.of(SEED_HOST + 1));

    assertEquals(HttpStatus.OK_200, response.getStatus());
    assertTrue(response.getLocation().toString().endsWith("/cluster/" + CLUSTER_NAME));
    assertEquals(1, mocks.context.storage.getClusters().size());

    Cluster cluster = mocks.context.storage.getCluster(CLUSTER_NAME).get();
    assertEquals(1, cluster.getSeedHosts().size());
    assertEquals(SEED_HOST + 1, cluster.getSeedHosts().iterator().next());

    response = clusterResource.addOrUpdateCluster(mocks.uriInfo, CLUSTER_NAME, Optional.of(SEED_HOST + 1));
    assertEquals(HttpStatus.NO_CONTENT_204, response.getStatus());
    assertTrue(response.getLocation().toString().endsWith("/cluster/" + cluster.getName()));
  }

  @Test
  public void addingAClusterAutomaticallySetupSchedulingRepairsWhenEnabled() throws Exception {
    final MockObjects mocks = initMocks();
    when(mocks.jmxProxy.getLiveNodes()).thenReturn(Arrays.asList(SEED_HOST));

    when(mocks.jmxProxy.getKeyspaces()).thenReturn(Lists.newArrayList("keyspace1"));
    when(mocks.jmxProxy.getTableNamesForKeyspace("keyspace1"))
        .thenReturn(Sets.newHashSet("table1"));

    mocks.context.config = TestRepairConfiguration.defaultConfigBuilder()
        .withAutoScheduling(
            TestRepairConfiguration.defaultAutoSchedulingConfigBuilder()
                .thatIsEnabled()
                .withTimeBeforeFirstSchedule(Duration.ofMinutes(1))
                .build())
        .build();

    ClusterResource clusterResource = new ClusterResource(mocks.context, Executors.newFixedThreadPool(2));
    Response response = clusterResource.addOrUpdateCluster(mocks.uriInfo, Optional.of(SEED_HOST));

    assertEquals(HttpStatus.CREATED_201, response.getStatus());
    assertEquals(1, mocks.context.storage.getAllRepairSchedules().size());
    assertEquals(1, mocks.context.storage.getRepairSchedulesForClusterAndKeyspace(CLUSTER_NAME, "keyspace1").size());
  }

  @Test
  public void testParseSeedHost() {
    String seedHostStringList = "127.0.0.1 , 127.0.0.2,  127.0.0.3";
    Set<String> seedHostSet = ClusterResource.parseSeedHosts(seedHostStringList);
    Set<String> seedHostExpectedSet = Sets.newHashSet("127.0.0.2", "127.0.0.1", "127.0.0.3");

    assertEquals(seedHostSet, seedHostExpectedSet);
  }

  @Test
  public void testParseSeedHostWithClusterName() {
    String seedHostStringList = "127.0.0.1@cluster1 , 127.0.0.2@cluster1,  127.0.0.3@cluster1";
    Set<String> seedHostSet = ClusterResource.parseSeedHosts(seedHostStringList);
    Set<String> seedHostExpectedSet = Sets.newHashSet("127.0.0.2", "127.0.0.1", "127.0.0.3");

    assertEquals(seedHostSet, seedHostExpectedSet);
  }

  @Test
  public void testParseClusterNameInSeedHost() {
    String seedHostStringList =
        "127.0.0.1@cluster one , 127.0.0.2@cluster one,  127.0.0.3@cluster one";
    Optional<String> clusterName = ClusterResource.parseClusterNameFromSeedHost(seedHostStringList);

    assertEquals("cluster one", clusterName.get());
  }


  private MockObjects initMocks() throws ReaperException {
    AppContext context = new AppContext();
    context.storage = new MemoryStorage();
    context.config = TestRepairConfiguration.defaultConfig();

    UriInfo uriInfo = mock(UriInfo.class);
    when(uriInfo.getBaseUriBuilder()).thenReturn(UriBuilder.fromUri(SAMPLE_URI));

    JmxProxy jmxProxy = mock(JmxProxy.class);
    when(jmxProxy.getClusterName()).thenReturn(CLUSTER_NAME);
    when(jmxProxy.getPartitioner()).thenReturn(PARTITIONER);

    context.jmxConnectionFactory = mock(JmxConnectionFactory.class);

    when(context.jmxConnectionFactory.connectAny(Mockito.any(Cluster.class), Mockito.anyInt()))
        .thenReturn(jmxProxy);

    when(context.jmxConnectionFactory.connectAny(Mockito.anyCollection(), Mockito.anyInt()))
        .thenReturn(jmxProxy);

    return new MockObjects(context, uriInfo, jmxProxy);
  }

  private static final class MockObjects {

    final AppContext context;
    final UriInfo uriInfo;
    final JmxProxy jmxProxy;

    MockObjects(AppContext context, UriInfo uriInfo, JmxProxy jmxProxy) {
      super();
      this.context = context;
      this.uriInfo = uriInfo;
      this.jmxProxy = jmxProxy;
    }

  }

}
