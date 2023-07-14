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
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.Table;
import io.cassandrareaper.crypto.Cryptograph;
import io.cassandrareaper.crypto.NoopCrypotograph;
import io.cassandrareaper.jmx.ClusterFacade;
import io.cassandrareaper.jmx.JmxConnectionFactory;
import io.cassandrareaper.jmx.JmxProxy;
import io.cassandrareaper.service.TestRepairConfiguration;
import io.cassandrareaper.storage.MemoryStorageFacade;

import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.RandomStringUtils;
import org.assertj.core.api.Assertions;
import org.eclipse.jetty.http.HttpStatus;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
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
  static final String JMX_USERNAME = "foo";
  static final String JMX_PASSWORD = "bar";

  private static final String STCS = "SizeTieredCompactionStrategy";

  @Test
  public void testAddCluster() throws Exception {
    final MockObjects mocks = initMocks();

    ClusterResource clusterResource
        = ClusterResource.create(mocks.context, mocks.cryptograph, mocks.context.storage.getEventsDao(),
        mocks.context.storage.getRepairRunDao());

    Response response = clusterResource
        .addOrUpdateCluster(mocks.uriInfo, Optional.of(SEED_HOST),
            Optional.of(Cluster.DEFAULT_JMX_PORT),
            Optional.of(JMX_USERNAME),
            Optional.of(JMX_PASSWORD));

    Assertions.assertThat(response.getStatus()).isEqualTo(HttpStatus.CREATED_201);
    assertEquals(1, mocks.context.storage.getClusterDao().getClusters().size());

    Cluster cluster = mocks.context.storage.getClusterDao().getCluster(CLUSTER_NAME);
    assertNotNull("Did not find expected cluster", cluster);
    assertEquals(0,
        mocks.context.storage.getRepairRunDao().getRepairRunsForCluster(cluster.getName(), Optional.of(1)).size());
    assertEquals(CLUSTER_NAME, cluster.getName());
    assertEquals(1, cluster.getSeedHosts().size());
    assertEquals(SEED_HOST, cluster.getSeedHosts().iterator().next());
    assertTrue(cluster.getJmxCredentials().isPresent());
    assertEquals(JMX_USERNAME, cluster.getJmxCredentials().get().getUsername());
    assertNotEquals(JMX_PASSWORD, cluster.getJmxCredentials().get().getPassword());
  }

  @Test
  public void testAddExistingCluster() throws Exception {
    final MockObjects mocks = initMocks();
    when(mocks.jmxProxy.getLiveNodes()).thenReturn(Arrays.asList(SEED_HOST));

    Cluster cluster = Cluster.builder()
        .withName(CLUSTER_NAME)
        .withPartitioner(PARTITIONER)
        .withSeedHosts(ImmutableSet.of(SEED_HOST))
        .withState(Cluster.State.ACTIVE)
        .build();

    mocks.context.storage.getClusterDao().addCluster(cluster);

    ClusterResource clusterResource = ClusterResource.create(mocks.context, mocks.cryptograph,
        mocks.context.storage.getEventsDao(),
        mocks.context.storage.getRepairRunDao());

    Response response = clusterResource
        .addOrUpdateCluster(mocks.uriInfo, Optional.of(SEED_HOST),
            Optional.of(Cluster.DEFAULT_JMX_PORT),
            Optional.of(JMX_USERNAME),
            Optional.of(JMX_PASSWORD));

    Assertions.assertThat(response.getStatus()).isEqualTo(HttpStatus.NO_CONTENT_204);
    assertTrue(response.getLocation().toString().endsWith("/cluster/" + cluster.getName()));
    assertEquals(1, mocks.context.storage.getClusterDao().getClusters().size());

    cluster = mocks.context.storage.getClusterDao().getCluster(CLUSTER_NAME);
    assertNotNull("Did not find expected cluster", cluster);
    assertEquals(0,
        mocks.context.storage.getRepairRunDao().getRepairRunsForCluster(cluster.getName(), Optional.of(1)).size());
    assertEquals(CLUSTER_NAME, cluster.getName());
    assertEquals(1, cluster.getSeedHosts().size());
    assertEquals(SEED_HOST, cluster.getSeedHosts().iterator().next());
  }

  @Test
  public void testAddExistingClusterWithClusterName() throws Exception {
    final MockObjects mocks = initMocks();
    when(mocks.jmxProxy.getLiveNodes()).thenReturn(Arrays.asList(SEED_HOST));

    Cluster cluster = Cluster.builder()
        .withName(CLUSTER_NAME)
        .withPartitioner(PARTITIONER)
        .withSeedHosts(ImmutableSet.of(SEED_HOST))
        .withState(Cluster.State.ACTIVE)
        .build();

    mocks.context.storage.getClusterDao().addCluster(cluster);

    ClusterResource clusterResource = ClusterResource.create(mocks.context, mocks.cryptograph,
        mocks.context.storage.getEventsDao(),
        mocks.context.storage.getRepairRunDao());

    Response response = clusterResource.addOrUpdateCluster(
        mocks.uriInfo,
        CLUSTER_NAME,
        Optional.of(SEED_HOST),
        Optional.of(Cluster.DEFAULT_JMX_PORT),
        Optional.of(JMX_USERNAME),
        Optional.of(JMX_PASSWORD));

    Assertions.assertThat(response.getStatus()).isEqualTo(HttpStatus.NO_CONTENT_204);
    assertTrue(response.getLocation().toString().endsWith("/cluster/" + cluster.getName()));
    assertEquals(1, mocks.context.storage.getClusterDao().getClusters().size());

    cluster = mocks.context.storage.getClusterDao().getCluster(CLUSTER_NAME);
    assertNotNull("Did not find expected cluster", cluster);
    assertEquals(0,
        mocks.context.storage.getRepairRunDao().getRepairRunsForCluster(cluster.getName(), Optional.of(1)).size());
    assertEquals(CLUSTER_NAME, cluster.getName());
    assertEquals(1, cluster.getSeedHosts().size());
    assertEquals(SEED_HOST, cluster.getSeedHosts().iterator().next());
  }

  @Test
  public void testFailAddingJmxCredentialsWithoutEncryptionConfigured() throws Exception {
    final MockObjects mocks = initMocks();
    ClusterFacade clusterFacade = mock(ClusterFacade.class);

    ClusterResource clusterResource
        = ClusterResource.create(mocks.context, new NoopCrypotograph(), () -> clusterFacade,
        mocks.context.storage.getEventsDao(),
        mocks.context.storage.getRepairRunDao());

    Response response = clusterResource.addOrUpdateCluster(mocks.uriInfo,
        Optional.of(SEED_HOST),
        Optional.of(Cluster.DEFAULT_JMX_PORT),
        Optional.of(JMX_USERNAME),
        Optional.of(JMX_PASSWORD));

    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
  }

  @Test
  public void testGetNonExistingCluster() throws ReaperException {
    final MockObjects mocks = initMocks();
    when(mocks.jmxProxy.getLiveNodes()).thenReturn(Arrays.asList(SEED_HOST));

    ClusterResource clusterResource
        = ClusterResource.create(mocks.context, new NoopCrypotograph(), () -> mocks.clusterFacade,
        mocks.context.storage.getEventsDao(),
        mocks.context.storage.getRepairRunDao());
    Response response = clusterResource.getCluster(I_DONT_EXIST, Optional.<Integer>empty());
    Assertions.assertThat(response.getStatus()).isEqualTo(HttpStatus.NOT_FOUND_404);
  }

  @Test
  public void testGetExistingCluster() throws ReaperException {
    final MockObjects mocks = initMocks();
    when(mocks.jmxProxy.getLiveNodes()).thenReturn(Arrays.asList(SEED_HOST));

    Cluster cluster = Cluster.builder()
        .withName(I_DO_EXIST)
        .withPartitioner(PARTITIONER)
        .withSeedHosts(ImmutableSet.of(SEED_HOST))
        .withState(Cluster.State.ACTIVE)
        .build();

    mocks.context.storage.getClusterDao().addCluster(cluster);

    ClusterResource clusterResource
        = ClusterResource.create(mocks.context, new NoopCrypotograph(), () -> mocks.clusterFacade,
        mocks.context.storage.getEventsDao(),
        mocks.context.storage.getRepairRunDao());
    Response response = clusterResource.getCluster(I_DO_EXIST, Optional.<Integer>empty());
    Assertions.assertThat(response.getStatus()).isEqualTo(HttpStatus.OK_200);
  }

  @Test
  public void testGetClusters_all() throws ReaperException {
    final MockObjects mocks = initMocks();

    Cluster cluster = Cluster.builder()
        .withName(CLUSTER_NAME)
        .withSeedHosts(ImmutableSet.of(SEED_HOST))
        .withState(Cluster.State.ACTIVE)
        .build();

    mocks.context.storage.getClusterDao().addCluster(cluster);

    ClusterResource clusterResource
        = ClusterResource.create(mocks.context, new NoopCrypotograph(), () -> mocks.clusterFacade,
        mocks.context.storage.getEventsDao(),
        mocks.context.storage.getRepairRunDao());
    Response response = clusterResource.getClusterList(Optional.empty());
    Assertions.assertThat(response.getStatus()).isEqualTo(HttpStatus.OK_200);
    List<String> clusterNames = (List<String>) response.getEntity();
    Assertions.assertThat(clusterNames).hasSize(1);
    Assertions.assertThat(clusterNames).contains(CLUSTER_NAME);
  }

  @Test
  public void testGetClusters_specified() throws ReaperException {
    final MockObjects mocks = initMocks();

    Cluster cluster = Cluster.builder()
        .withName(CLUSTER_NAME)
        .withSeedHosts(ImmutableSet.of(SEED_HOST))
        .withState(Cluster.State.ACTIVE)
        .build();

    mocks.context.storage.getClusterDao().addCluster(cluster);

    ClusterResource clusterResource
        = ClusterResource.create(mocks.context, new NoopCrypotograph(), () -> mocks.clusterFacade,
        mocks.context.storage.getEventsDao(),
        mocks.context.storage.getRepairRunDao());
    Response response = clusterResource.getClusterList(Optional.of(SEED_HOST));
    Assertions.assertThat(response.getStatus()).isEqualTo(HttpStatus.OK_200);
    List<String> clusterNames = (List<String>) response.getEntity();
    Assertions.assertThat(clusterNames).hasSize(1);
    Assertions.assertThat(clusterNames).contains(CLUSTER_NAME);
  }

  @Test
  public void testGetClusters_only_specified_first() throws ReaperException {
    final MockObjects mocks = initMocks();

    Cluster cluster = Cluster.builder()
        .withName(CLUSTER_NAME)
        .withSeedHosts(ImmutableSet.of(SEED_HOST))
        .withState(Cluster.State.ACTIVE)
        .build();

    mocks.context.storage.getClusterDao().addCluster(cluster);

    cluster = Cluster.builder()
        .withName("cluster2")
        .withSeedHosts(ImmutableSet.of("host2"))
        .withState(Cluster.State.ACTIVE)
        .build();

    mocks.context.storage.getClusterDao().addCluster(cluster);

    ClusterResource clusterResource
        = ClusterResource.create(mocks.context, new NoopCrypotograph(), () -> mocks.clusterFacade,
        mocks.context.storage.getEventsDao(),
        mocks.context.storage.getRepairRunDao());
    Response response = clusterResource.getClusterList(Optional.of(SEED_HOST));
    Assertions.assertThat(response.getStatus()).isEqualTo(HttpStatus.OK_200);
    List<String> clusterNames = (List<String>) response.getEntity();
    Assertions.assertThat(clusterNames).hasSize(1);
    Assertions.assertThat(clusterNames).contains(CLUSTER_NAME);
  }

  @Test
  public void testGetClusters_only_specified_second() throws ReaperException {
    final MockObjects mocks = initMocks();

    Cluster cluster = Cluster.builder()
        .withName(CLUSTER_NAME)
        .withSeedHosts(ImmutableSet.of(SEED_HOST))
        .withState(Cluster.State.ACTIVE)
        .build();

    mocks.context.storage.getClusterDao().addCluster(cluster);

    cluster = Cluster.builder()
        .withName("cluster2")
        .withSeedHosts(ImmutableSet.of("host2"))
        .withState(Cluster.State.ACTIVE)
        .build();

    mocks.context.storage.getClusterDao().addCluster(cluster);

    ClusterResource clusterResource
        = ClusterResource.create(mocks.context, new NoopCrypotograph(), () -> mocks.clusterFacade,
        mocks.context.storage.getEventsDao(),
        mocks.context.storage.getRepairRunDao());
    Response response = clusterResource.getClusterList(Optional.of("host2"));
    Assertions.assertThat(response.getStatus()).isEqualTo(HttpStatus.OK_200);
    List<String> clusterNames = (List<String>) response.getEntity();
    Assertions.assertThat(clusterNames).hasSize(1);
    Assertions.assertThat(clusterNames).contains("cluster2");
  }

  @Test
  public void testGetClusters_multiple() throws ReaperException {
    final MockObjects mocks = initMocks();

    Cluster cluster = Cluster.builder()
        .withName(CLUSTER_NAME)
        .withSeedHosts(ImmutableSet.of(SEED_HOST))
        .withState(Cluster.State.ACTIVE)
        .build();

    mocks.context.storage.getClusterDao().addCluster(cluster);

    cluster = Cluster.builder()
        .withName("cluster2")
        .withSeedHosts(ImmutableSet.of("host2"))
        .withState(Cluster.State.ACTIVE)
        .build();

    mocks.context.storage.getClusterDao().addCluster(cluster);

    ClusterResource clusterResource
        = ClusterResource.create(mocks.context, new NoopCrypotograph(), () -> mocks.clusterFacade,
        mocks.context.storage.getEventsDao(),
        mocks.context.storage.getRepairRunDao());
    Response response = clusterResource.getClusterList(Optional.empty());
    Assertions.assertThat(response.getStatus()).isEqualTo(HttpStatus.OK_200);
    List<String> clusterNames = (List<String>) response.getEntity();
    Assertions.assertThat(clusterNames).hasSize(2);
    Assertions.assertThat(clusterNames).contains(CLUSTER_NAME, "cluster2");
  }

  @Test
  public void testGetClusters_multiple_ordered_by_name() throws ReaperException {
    final MockObjects mocks = initMocks();

    Cluster cluster = Cluster.builder()
        .withName(CLUSTER_NAME)
        .withSeedHosts(ImmutableSet.of(SEED_HOST))
        .withState(Cluster.State.ACTIVE)
        .build();

    mocks.context.storage.getClusterDao().addCluster(cluster);

    cluster = Cluster.builder()
        .withName("abc")
        .withSeedHosts(ImmutableSet.of("host2"))
        .withState(Cluster.State.ACTIVE)
        .build();

    mocks.context.storage.getClusterDao().addCluster(cluster);

    ClusterResource clusterResource
        = ClusterResource.create(mocks.context, new NoopCrypotograph(), () -> mocks.clusterFacade,
        mocks.context.storage.getEventsDao(),
        mocks.context.storage.getRepairRunDao());
    Response response = clusterResource.getClusterList(Optional.empty());
    Assertions.assertThat(response.getStatus()).isEqualTo(HttpStatus.OK_200);
    List<String> clusterNames = (List<String>) response.getEntity();
    Assertions.assertThat(clusterNames).hasSize(2);
    Assertions.assertThat(clusterNames).containsExactly("abc", CLUSTER_NAME);
  }

  @Test
  public void testGetClusters_multiple_ordered_by_state() throws ReaperException {
    final MockObjects mocks = initMocks();

    Cluster cluster = Cluster.builder()
        .withName(CLUSTER_NAME)
        .withSeedHosts(ImmutableSet.of(SEED_HOST))
        .withState(Cluster.State.UNREACHABLE)
        .build();

    mocks.context.storage.getClusterDao().addCluster(cluster);

    cluster = Cluster.builder()
        .withName("cluster2")
        .withSeedHosts(ImmutableSet.of("host2"))
        .withState(Cluster.State.ACTIVE)
        .build();

    mocks.context.storage.getClusterDao().addCluster(cluster);

    ClusterResource clusterResource
        = ClusterResource.create(mocks.context, new NoopCrypotograph(), () -> mocks.clusterFacade,
        mocks.context.storage.getEventsDao(),
        mocks.context.storage.getRepairRunDao());
    Response response = clusterResource.getClusterList(Optional.empty());
    Assertions.assertThat(response.getStatus()).isEqualTo(HttpStatus.OK_200);
    List<String> clusterNames = (List<String>) response.getEntity();
    Assertions.assertThat(clusterNames).hasSize(2);
    Assertions.assertThat(clusterNames).containsExactly("cluster2", CLUSTER_NAME);
  }

  @Test(expected = IllegalStateException.class)
  public void testGetClusters_fail_persisting_unknown() throws ReaperException {
    final MockObjects mocks = initMocks();

    Cluster cluster = Cluster.builder()
        .withName(CLUSTER_NAME)
        .withSeedHosts(ImmutableSet.of(SEED_HOST))
        .build();

    mocks.context.storage.getClusterDao().addCluster(cluster);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetClusters_fail_persisting_two_clusters_same_name() throws ReaperException {
    final MockObjects mocks = initMocks();

    Cluster cluster = Cluster.builder()
        .withName(CLUSTER_NAME)
        .withSeedHosts(ImmutableSet.of(SEED_HOST))
        .withState(Cluster.State.ACTIVE)
        .build();

    mocks.context.storage.getClusterDao().addCluster(cluster);

    cluster = Cluster.builder()
        .withName(CLUSTER_NAME)
        .withSeedHosts(ImmutableSet.of("test_host_2"))
        .withState(Cluster.State.ACTIVE)
        .build();

    mocks.context.storage.getClusterDao().addCluster(cluster);
  }

  @Test
  public void testModifyClusterSeeds() throws ReaperException {
    final MockObjects mocks = initMocks();

    ClusterResource clusterResource
        = ClusterResource.create(mocks.context, mocks.cryptograph, mocks.context.storage.getEventsDao(),
        mocks.context.storage.getRepairRunDao());
    clusterResource.addOrUpdateCluster(mocks.uriInfo, Optional.of(SEED_HOST), Optional.of(Cluster.DEFAULT_JMX_PORT),
        Optional.of(JMX_USERNAME), Optional.of(JMX_PASSWORD));
    doReturn(Arrays.asList(SEED_HOST + 1, SEED_HOST)).when(mocks.jmxProxy).getLiveNodes();

    Response response = clusterResource
        .addOrUpdateCluster(mocks.uriInfo, Optional.of(SEED_HOST + 1),
            Optional.of(Cluster.DEFAULT_JMX_PORT),
            Optional.of(JMX_USERNAME),
            Optional.of(JMX_PASSWORD));

    assertEquals(HttpStatus.OK_200, response.getStatus());
    assertTrue(response.getLocation().toString().endsWith("/cluster/" + CLUSTER_NAME));
    assertEquals(1, mocks.context.storage.getClusterDao().getClusters().size());

    Cluster cluster = mocks.context.storage.getClusterDao().getCluster(CLUSTER_NAME);
    Assertions.assertThat(cluster.getSeedHosts()).hasSize(2);
    Assertions.assertThat(cluster.getSeedHosts()).contains(SEED_HOST + 1);

    response = clusterResource.addOrUpdateCluster(
        mocks.uriInfo,
        CLUSTER_NAME,
        Optional.of(SEED_HOST + 1),
        Optional.of(Cluster.DEFAULT_JMX_PORT),
        Optional.of(JMX_USERNAME),
        Optional.of(JMX_PASSWORD));

    assertEquals(HttpStatus.NO_CONTENT_204, response.getStatus());
    assertTrue(response.getLocation().toString().endsWith("/cluster/" + cluster.getName()));
  }

  @Test
  public void testModifyClusterSeedsWithClusterName() throws ReaperException {
    final MockObjects mocks = initMocks();

    ClusterResource clusterResource
        = ClusterResource.create(mocks.context, mocks.cryptograph,
        mocks.context.storage.getEventsDao(),
        mocks.context.storage.getRepairRunDao());
    ;
    clusterResource.addOrUpdateCluster(mocks.uriInfo, Optional.of(SEED_HOST), Optional.of(Cluster.DEFAULT_JMX_PORT),
        Optional.of(JMX_USERNAME), Optional.of(JMX_PASSWORD));
    doReturn(Arrays.asList(SEED_HOST + 1, SEED_HOST)).when(mocks.jmxProxy).getLiveNodes();

    Response response = clusterResource.addOrUpdateCluster(
        mocks.uriInfo,
        CLUSTER_NAME,
        Optional.of(SEED_HOST + 1),
        Optional.of(Cluster.DEFAULT_JMX_PORT),
        Optional.of(JMX_USERNAME),
        Optional.of(JMX_PASSWORD));

    assertEquals(HttpStatus.OK_200, response.getStatus());
    assertTrue(response.getLocation().toString().endsWith("/cluster/" + CLUSTER_NAME));
    assertEquals(1, mocks.context.storage.getClusterDao().getClusters().size());

    Cluster cluster = mocks.context.storage.getClusterDao().getCluster(CLUSTER_NAME);
    Assertions.assertThat(cluster.getSeedHosts()).hasSize(2);
    Assertions.assertThat(cluster.getSeedHosts()).contains(SEED_HOST + 1);

    response = clusterResource.addOrUpdateCluster(
        mocks.uriInfo,
        CLUSTER_NAME,
        Optional.of(SEED_HOST + 1),
        Optional.of(Cluster.DEFAULT_JMX_PORT),
        Optional.of(JMX_USERNAME),
        Optional.of(JMX_PASSWORD));
    assertEquals(HttpStatus.NO_CONTENT_204, response.getStatus());

    assertTrue(response.getLocation().toString().endsWith("/cluster/" + cluster.getName()));
  }

  @Test
  public void addingAClusterAutomaticallySetupSchedulingRepairsWhenEnabled() throws Exception {
    final MockObjects mocks = initMocks();
    when(mocks.jmxProxy.getLiveNodes()).thenReturn(Arrays.asList(SEED_HOST));
    when(mocks.jmxProxy.getKeyspaces()).thenReturn(Lists.newArrayList("keyspace1"));

    when(mocks.jmxProxy.getTablesForKeyspace("keyspace1"))
        .thenReturn(Sets.newHashSet(Table.builder().withName("table1").withCompactionStrategy(STCS).build()));

    mocks.context.config = TestRepairConfiguration.defaultConfigBuilder()
        .withAutoScheduling(
            TestRepairConfiguration.defaultAutoSchedulingConfigBuilder()
                .thatIsEnabled()
                .withTimeBeforeFirstSchedule(Duration.ofMinutes(1))
                .build())
        .build();

    ClusterResource clusterResource
        = ClusterResource.create(mocks.context, mocks.cryptograph,
        mocks.context.storage.getEventsDao(),
        mocks.context.storage.getRepairRunDao());
    ;

    Response response = clusterResource
        .addOrUpdateCluster(mocks.uriInfo, Optional.of(SEED_HOST), Optional.of(Cluster.DEFAULT_JMX_PORT),
            Optional.of(JMX_USERNAME), Optional.of(JMX_PASSWORD));

    assertEquals(HttpStatus.CREATED_201, response.getStatus());
    assertEquals(1, mocks.context.storage.getRepairScheduleDao().getAllRepairSchedules().size());
    assertEquals(1,
        mocks.context.storage.getRepairScheduleDao().getRepairSchedulesForClusterAndKeyspace(CLUSTER_NAME, "keyspace1")
            .size());
  }

  @Test
  public void testClusterDeleting() throws Exception {
    final MockObjects mocks = initMocks();
    when(mocks.jmxProxy.getLiveNodes()).thenReturn(Arrays.asList(SEED_HOST));
    when(mocks.jmxProxy.getKeyspaces()).thenReturn(Lists.newArrayList("keyspace1"));

    when(mocks.jmxProxy.getTablesForKeyspace("keyspace1"))
        .thenReturn(Sets.newHashSet(Table.builder().withName("table1").withCompactionStrategy(STCS).build()));

    mocks.context.config = TestRepairConfiguration.defaultConfigBuilder()
        .withAutoScheduling(
            TestRepairConfiguration.defaultAutoSchedulingConfigBuilder()
                .thatIsEnabled()
                .withTimeBeforeFirstSchedule(Duration.ofMinutes(1))
                .build())
        .build();

    ClusterResource clusterResource
        = ClusterResource.create(mocks.context, mocks.cryptograph,
        mocks.context.storage.getEventsDao(),
        mocks.context.storage.getRepairRunDao());
    ;

    Response response = clusterResource
        .addOrUpdateCluster(mocks.uriInfo, Optional.of(SEED_HOST), Optional.of(Cluster.DEFAULT_JMX_PORT),
            Optional.of(JMX_USERNAME), Optional.of(JMX_PASSWORD));

    assertEquals(HttpStatus.CREATED_201, response.getStatus());
    assertEquals(1, mocks.context.storage.getRepairScheduleDao().getAllRepairSchedules().size());
    assertEquals(1,
        mocks.context.storage.getRepairScheduleDao().getRepairSchedulesForClusterAndKeyspace(CLUSTER_NAME, "keyspace1")
            .size());

    assertEquals(HttpStatus.CONFLICT_409, clusterResource.deleteCluster(CLUSTER_NAME, Optional.empty()).getStatus());

    assertEquals(
        HttpStatus.CONFLICT_409,
        clusterResource.deleteCluster(CLUSTER_NAME, Optional.of(Boolean.FALSE)).getStatus());

    assertEquals(
        HttpStatus.ACCEPTED_202,
        clusterResource.deleteCluster(CLUSTER_NAME, Optional.of(Boolean.TRUE)).getStatus());
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
    String seedHostStringList = "127.0.0.1@cluster one , 127.0.0.2@cluster one,  127.0.0.3@cluster one";
    Optional<String> clusterName = ClusterResource.parseClusterNameFromSeedHost(seedHostStringList);

    assertEquals("cluster one", clusterName.get());
  }


  private MockObjects initMocks() throws ReaperException {
    AppContext context = new AppContext();
    context.storage = new MemoryStorageFacade();
    context.config = TestRepairConfiguration.defaultConfig();

    UriInfo uriInfo = mock(UriInfo.class);
    when(uriInfo.getBaseUriBuilder()).thenReturn(UriBuilder.fromUri(SAMPLE_URI));

    JmxProxy jmxProxy = mock(JmxProxy.class);
    when(jmxProxy.getClusterName()).thenReturn(CLUSTER_NAME);
    when(jmxProxy.getPartitioner()).thenReturn(PARTITIONER);

    context.jmxConnectionFactory = mock(JmxConnectionFactory.class);

    when(context.jmxConnectionFactory.connectAny(Mockito.anyCollection())).thenReturn(jmxProxy);

    Cryptograph cryptograph = mock(Cryptograph.class);
    when(cryptograph.encrypt(any(String.class))).thenReturn(RandomStringUtils.randomNumeric(10));

    ClusterFacade clusterFacade = mock(ClusterFacade.class);

    return new MockObjects(context, cryptograph, uriInfo, jmxProxy, clusterFacade);
  }

  private static final class MockObjects {

    final AppContext context;
    final Cryptograph cryptograph;
    final UriInfo uriInfo;
    final JmxProxy jmxProxy;
    final ClusterFacade clusterFacade;

    MockObjects(
        AppContext context,
        Cryptograph cryptograph,
        UriInfo uriInfo,
        JmxProxy jmxProxy,
        ClusterFacade clusterFacade) {
      super();
      this.context = context;
      this.cryptograph = cryptograph;
      this.uriInfo = uriInfo;
      this.jmxProxy = jmxProxy;
      this.clusterFacade = clusterFacade;
    }

  }

}