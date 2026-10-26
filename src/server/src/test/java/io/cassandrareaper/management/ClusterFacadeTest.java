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

import io.cassandrareaper.AppContext;
import io.cassandrareaper.ReaperApplicationConfiguration;
import io.cassandrareaper.ReaperApplicationConfiguration.DatacenterAvailability;
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.Compaction;
import io.cassandrareaper.core.CompactionStats;
import io.cassandrareaper.core.JmxCredentials;
import io.cassandrareaper.core.Segment;
import io.cassandrareaper.management.jmx.JmxCassandraManagementProxy;
import io.cassandrareaper.management.jmx.JmxManagementConnectionFactory;
import io.cassandrareaper.service.RingRange;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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

  // -------------------------------------------------------------------------
  // tokenRangeToEndpoint – all-ranges validation (Fix A regression tests)
  // -------------------------------------------------------------------------

  /**
   * Builds a stubbed ClusterFacade whose getRangeToEndpointMap always returns the supplied map. The
   * JMX proxy is configured to accept any connect call, so the facade can be instantiated without a
   * real Cassandra cluster. Clears static caches so that each test exercises a fresh cache miss.
   */
  private ClusterFacade buildFacadeWithRangeMap(Map<List<String>, List<String>> rangeToEndpointMap)
      throws ReaperException {
    ClusterFacade.clearCaches();
    AppContext ctx = new AppContext();
    ctx.config = new ReaperApplicationConfiguration();
    JmxManagementConnectionFactory factory = mock(JmxManagementConnectionFactory.class);
    JmxCassandraManagementProxy proxy = mock(JmxCassandraManagementProxy.class);
    when(factory.connectAny(any())).thenReturn(proxy);
    when(proxy.getRangeToEndpointMap(any())).thenReturn(rangeToEndpointMap);
    ctx.managementConnectionFactory = factory;
    return ClusterFacade.create(ctx);
  }

  private static Cluster testCluster() {
    return Cluster.builder()
        .withName("test")
        .withSeedHosts(new HashSet<>(Arrays.asList("127.0.0.1")))
        .withState(Cluster.State.ACTIVE)
        .build();
  }

  private static Map<List<String>, List<String>> rangeMap(
      String start, String end, String... hosts) {
    Map<List<String>, List<String>> m = new HashMap<>();
    m.put(Arrays.asList(start, end), Arrays.asList(hosts));
    return m;
  }

  /**
   * Test A-a: coalesced segment with all ranges enclosed by the same replica set → replicas
   * returned.
   */
  @Test
  public void tokenRangeToEndpoint_allRangesEnclosedSameReplicas_returnsReplicas()
      throws ReaperException {
    // Ring: [0,100) → node1,node2 and [100,200) → node1,node2 (same replicas)
    Map<List<String>, List<String>> ring = new HashMap<>();
    ring.put(Arrays.asList("0", "100"), Arrays.asList("127.0.0.1", "127.0.0.2"));
    ring.put(Arrays.asList("100", "200"), Arrays.asList("127.0.0.1", "127.0.0.2"));

    // Coalesced segment: ranges [10,50) and [110,150) – both enclosed, same replicas
    Segment segment =
        Segment.builder()
            .withTokenRanges(
                Arrays.asList(
                    new RingRange(BigInteger.valueOf(10), BigInteger.valueOf(50)),
                    new RingRange(BigInteger.valueOf(110), BigInteger.valueOf(150))))
            .build();

    ClusterFacade facade = buildFacadeWithRangeMap(ring);
    List<String> result = facade.tokenRangeToEndpoint(testCluster(), "ks", segment);
    assertEquals(Arrays.asList("127.0.0.1", "127.0.0.2"), result);
  }

  /**
   * Test A-b: second range of a coalesced segment spans two ring entries (no single entry encloses
   * it) → empty list.
   */
  @Test
  public void tokenRangeToEndpoint_secondRangeSpansTwoRingEntries_returnsEmpty()
      throws ReaperException {
    // Ring has a boundary at 50; a sub-range [20,80) crosses that boundary
    Map<List<String>, List<String>> ring = new HashMap<>();
    ring.put(Arrays.asList("0", "50"), Arrays.asList("127.0.0.1", "127.0.0.2"));
    ring.put(Arrays.asList("50", "100"), Arrays.asList("127.0.0.1", "127.0.0.2"));

    // First range [10,40) is enclosed; second range [20,80) crosses the 50 boundary
    Segment segment =
        Segment.builder()
            .withTokenRanges(
                Arrays.asList(
                    new RingRange(BigInteger.valueOf(10), BigInteger.valueOf(40)),
                    new RingRange(BigInteger.valueOf(20), BigInteger.valueOf(80))))
            .build();

    ClusterFacade facade = buildFacadeWithRangeMap(ring);
    List<String> result = facade.tokenRangeToEndpoint(testCluster(), "ks", segment);
    assertTrue("Expected empty list when second range crosses ring boundary", result.isEmpty());
  }

  /**
   * Test A-c: ranges are each individually enclosed, but by ring entries with different replica
   * sets → empty list.
   */
  @Test
  public void tokenRangeToEndpoint_rangesEnclosedButDifferentReplicaSets_returnsEmpty()
      throws ReaperException {
    // Two ring entries covering [0,100) and [100,200), each with a different replica set
    Map<List<String>, List<String>> ring = new HashMap<>();
    ring.put(Arrays.asList("0", "100"), Arrays.asList("127.0.0.1", "127.0.0.2"));
    ring.put(Arrays.asList("100", "200"), Arrays.asList("127.0.0.3", "127.0.0.4"));

    // Coalesced segment: [10,50) enclosed in first entry, [110,150) enclosed in second entry
    Segment segment =
        Segment.builder()
            .withTokenRanges(
                Arrays.asList(
                    new RingRange(BigInteger.valueOf(10), BigInteger.valueOf(50)),
                    new RingRange(BigInteger.valueOf(110), BigInteger.valueOf(150))))
            .build();

    ClusterFacade facade = buildFacadeWithRangeMap(ring);
    List<String> result = facade.tokenRangeToEndpoint(testCluster(), "ks", segment);
    assertTrue("Expected empty list when sub-ranges have different replica sets", result.isEmpty());
  }

  /** Test A-d: single-range segment – behaviour must be identical to the pre-fix implementation. */
  @Test
  public void tokenRangeToEndpoint_singleRangeSegment_returnsReplicas() throws ReaperException {
    Map<List<String>, List<String>> ring = rangeMap("0", "100", "127.0.0.1", "127.0.0.2");

    Segment segment =
        Segment.builder()
            .withTokenRange(new RingRange(BigInteger.valueOf(10), BigInteger.valueOf(50)))
            .build();

    ClusterFacade facade = buildFacadeWithRangeMap(ring);
    List<String> result = facade.tokenRangeToEndpoint(testCluster(), "ks", segment);
    assertEquals(Arrays.asList("127.0.0.1", "127.0.0.2"), result);
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
