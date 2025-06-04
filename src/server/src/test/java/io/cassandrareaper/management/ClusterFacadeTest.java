/*
 * Copyright 2019-2019 The Last Pickle Ltd
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cassandrareaper.management;

import static io.cassandrareaper.service.RepairRunnerTest.scyllaThreeNodeClusterWithIps;
import static io.cassandrareaper.service.RepairRunnerTest.threeNodeClusterWithIps;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.collect.ImmutableList;
import io.cassandrareaper.AppContext;
import io.cassandrareaper.ReaperApplicationConfiguration;
import io.cassandrareaper.ReaperApplicationConfiguration.DatacenterAvailability;
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.Compaction;
import io.cassandrareaper.core.CompactionStats;
import io.cassandrareaper.core.JmxCredentials;
import io.cassandrareaper.management.jmx.JmxCassandraManagementProxy;
import io.cassandrareaper.management.jmx.JmxManagementConnectionFactory;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.Test;
import org.mockito.Mockito;

public class ClusterFacadeTest {

  @Test
  public void nodeIsAccessibleThroughJmxSidecarTest() throws ReaperException {
    final AppContext cxt = new AppContext();
    cxt.config = new ReaperApplicationConfiguration();
    AppContext contextSpy = Mockito.spy(cxt);
    Mockito.doReturn("127.0.0.1").when(contextSpy).getLocalNodeAddress();

    contextSpy.config.setDatacenterAvailability(DatacenterAvailability.SIDECAR);
    JmxManagementConnectionFactory jmxManagementConnectionFactory =
        mock(JmxManagementConnectionFactory.class);
    when(jmxManagementConnectionFactory.getAccessibleDatacenters())
        .thenReturn(new HashSet<String>(Arrays.asList("dc1")));
    contextSpy.managementConnectionFactory = jmxManagementConnectionFactory;
    ClusterFacade clusterFacade = ClusterFacade.create(contextSpy);
    assertTrue(clusterFacade.nodeIsDirectlyAccessible("dc1", contextSpy.getLocalNodeAddress()));
    assertFalse(clusterFacade.nodeIsDirectlyAccessible("dc1", "127.0.0.2"));
  }

  @Test
  public void nodeIsAccessibleThroughJmxAllTest() throws ReaperException {
    final AppContext context = new AppContext();
    context.config = new ReaperApplicationConfiguration();
    context.managementConnectionFactory = Mockito.mock(JmxManagementConnectionFactory.class);

    Mockito.when(context.managementConnectionFactory.getAccessibleDatacenters())
        .thenReturn(new HashSet<>(Arrays.asList("dc1")));

    context.config.setDatacenterAvailability(DatacenterAvailability.ALL);
    assertTrue(ClusterFacade.create(context).nodeIsDirectlyAccessible("dc1", "127.0.0.1"));
    assertTrue(ClusterFacade.create(context).nodeIsDirectlyAccessible("dc2", "127.0.0.2"));
  }

  @Test
  public void nodeIsAccessibleThroughJmxLocalTest() throws ReaperException {
    final AppContext context = new AppContext();
    context.config = new ReaperApplicationConfiguration();
    context.managementConnectionFactory = Mockito.mock(JmxManagementConnectionFactory.class);

    Mockito.when(context.managementConnectionFactory.getAccessibleDatacenters())
        .thenReturn(new HashSet<>(Arrays.asList("dc1")));

    context.config.setDatacenterAvailability(DatacenterAvailability.LOCAL);
    // it's in another DC so LOCAL disallows attempting it
    assertFalse(ClusterFacade.create(context).nodeIsDirectlyAccessible("dc2", "127.0.0.2"));
    // Should be accessible, same DC
    assertTrue(ClusterFacade.create(context).nodeIsDirectlyAccessible("dc1", "127.0.0.2"));
  }

  @Test
  public void nodeIsAccessibleThroughJmxEachTest() throws ReaperException {
    final AppContext context = new AppContext();
    context.config = new ReaperApplicationConfiguration();
    context.managementConnectionFactory = Mockito.mock(JmxManagementConnectionFactory.class);

    Mockito.when(context.managementConnectionFactory.getAccessibleDatacenters())
        .thenReturn(new HashSet<>(Arrays.asList("dc1")));

    context.config.setDatacenterAvailability(DatacenterAvailability.EACH);
    // Should not be accessible as it's in another DC
    assertFalse(ClusterFacade.create(context).nodeIsDirectlyAccessible("dc2", "127.0.0.2"));
    // Should be accessible, same DC
    assertTrue(ClusterFacade.create(context).nodeIsDirectlyAccessible("dc1", "127.0.0.2"));
  }

  @Test
  public void parseListCompactionTest() throws IOException {
    final ObjectMapper objectMapper = new ObjectMapper();
    Compaction compaction =
        Compaction.builder()
            .withId("foo")
            .withKeyspace("ks")
            .withTable("t")
            .withProgress(64L)
            .withTotal(128L)
            .withType("Validation")
            .withUnit("unit")
            .build();
    String compactionsJson = objectMapper.writeValueAsString(ImmutableList.of(compaction));
    CompactionStats compactionStats = ClusterFacade.parseCompactionStats(compactionsJson);
    assertFalse(compactionStats.getPendingCompactions().isPresent());
  }

  @Test
  public void parseCompactionStatsTest() throws IOException {
    final ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.registerModule(new Jdk8Module());
    Compaction compaction =
        Compaction.builder()
            .withId("foo")
            .withKeyspace("ks")
            .withTable("t")
            .withProgress(64L)
            .withTotal(128L)
            .withType("Validation")
            .withUnit("unit")
            .build();
    CompactionStats originalCompactionStats =
        CompactionStats.builder()
            .withActiveCompactions(ImmutableList.of(compaction))
            .withPendingCompactions(Optional.of(42))
            .build();
    String compactionJson = objectMapper.writeValueAsString(originalCompactionStats);
    CompactionStats compactionStats = ClusterFacade.parseCompactionStats(compactionJson);
    assertEquals(42L, compactionStats.getPendingCompactions().get().longValue());
  }

  @Test(expected = IOException.class)
  public void parseCompactionStatsErrorTest() throws IOException {
    String compactionJson = "{\"json\": \"thats_not_compactionstats\"}";
    ClusterFacade.parseCompactionStats(compactionJson);
  }

  @Test
  public void parseEmptyCompactionStats() throws IOException {
    String compactionJson = "";
    CompactionStats compactionStats = ClusterFacade.parseCompactionStats(compactionJson);
    assertFalse(compactionStats.getPendingCompactions().isPresent());
  }

  @Test
  public void endpointCleanupCassandra() {
    Map<List<String>, List<String>> endpoints =
        ClusterFacade.maybeCleanupEndpointFromScylla(threeNodeClusterWithIps());
    for (Map.Entry<List<String>, List<String>> entry : endpoints.entrySet()) {
      assertEquals(endpoints, threeNodeClusterWithIps());
    }
  }

  @Test
  public void endpointCleanupScylla() {
    Map<List<String>, List<String>> endpoints =
        ClusterFacade.maybeCleanupEndpointFromScylla(scyllaThreeNodeClusterWithIps());
    for (Map.Entry<List<String>, List<String>> entry : endpoints.entrySet()) {
      assertEquals(endpoints, threeNodeClusterWithIps());
    }
  }

  @Test(expected = ReaperException.class)
  public void getRangeToEndpointMapArgumentExceptionTest() throws ReaperException {
    final AppContext cxt = new AppContext();
    cxt.config = new ReaperApplicationConfiguration();
    AppContext contextSpy = Mockito.spy(cxt);
    Mockito.doReturn("127.0.0.1").when(contextSpy).getLocalNodeAddress();

    contextSpy.config.setDatacenterAvailability(DatacenterAvailability.SIDECAR);
    JmxManagementConnectionFactory jmxManagementConnectionFactory =
        mock(JmxManagementConnectionFactory.class);
    JmxCassandraManagementProxy mockProxy = mock(JmxCassandraManagementProxy.class);
    when(jmxManagementConnectionFactory.connectAny(any(Collection.class))).thenReturn(mockProxy);
    contextSpy.managementConnectionFactory = jmxManagementConnectionFactory;
    final ClusterFacade cf = ClusterFacade.create(contextSpy);
    JmxCredentials jmxCredentials =
        JmxCredentials.builder().withUsername("test").withPassword("testPwd").build();
    Set<String> seedHosts = new HashSet<>();
    seedHosts.add("127.0.0.1");
    Cluster.Builder builder = Cluster.builder();
    builder.withName("testCluster");
    builder.withJmxCredentials(jmxCredentials);
    builder.withSeedHosts(seedHosts);
    Cluster mockCluster = builder.build();
    when(mockProxy.getRangeToEndpointMap(eq("fake-ks"))).thenThrow(new AssertionError(""));
    cf.getRangeToEndpointMap(mockCluster, "fake-ks");
  }
}
