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

package io.cassandrareaper.service;

import io.cassandrareaper.AppContext;
import io.cassandrareaper.ReaperApplicationConfiguration;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.CompactionStats;
import io.cassandrareaper.core.Node;
import io.cassandrareaper.core.RepairRun;
import io.cassandrareaper.core.RepairSegment;
import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.core.Segment;
import io.cassandrareaper.management.ClusterFacade;
import io.cassandrareaper.management.ICassandraManagementProxy;
import io.cassandrareaper.storage.IStorageDao;
import io.cassandrareaper.storage.MemoryStorageFacade;

import java.math.BigInteger;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.commons.lang3.RandomStringUtils;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

/**
 * Non-subrange incremental segments hold all of a node's ranges, which have different replica sets.
 * {@link RepairRunner} must resolve replicas from the base range only for those segments.
 */
public final class RepairRunnerIncrementalReplicasTest {

  private static final Set<String> TABLES = ImmutableSet.of("table1");

  @After
  public void tearDown() {
    SegmentRunner.SEGMENT_RUNNERS.clear();
  }

  @Test
  public void testNonSubrangeIncrementalUsesBaseRangeReplicas() throws Exception {
    final Cluster cluster =
        Cluster.builder()
            .withName("test_" + RandomStringUtils.randomAlphabetic(12))
            .withSeedHosts(ImmutableSet.of("127.0.0.1"))
            .withState(Cluster.State.ACTIVE)
            .build();
    final IStorageDao storage = new MemoryStorageFacade();
    AppContext context = new AppContext();
    context.storage = storage;
    context.config = new ReaperApplicationConfiguration();
    context.schedulingManager = mock(SchedulingManager.class);
    context.repairManager = mock(RepairManager.class);
    when(context.repairManager.submitSegment(any())).thenReturn(SettableFuture.create());
    doNothing().when(context.schedulingManager).maybeRegisterRepairRunCompleted(any());
    storage.getClusterDao().addCluster(cluster);

    UUID unitId =
        storage
            .getRepairUnitDao()
            .addRepairUnit(
                RepairUnit.builder()
                    .clusterName(cluster.getName())
                    .keyspaceName("test_keyspace")
                    .columnFamilies(Sets.newHashSet("table1"))
                    .incrementalRepair(true)
                    .subrangeIncrementalRepair(false)
                    .nodes(Sets.newHashSet("127.0.0.1", "127.0.0.2", "127.0.0.3"))
                    .datacenters(Collections.emptySet())
                    .blacklistedTables(Collections.emptySet())
                    .repairThreadCount(1)
                    .timeout(30))
            .getId();

    // Two ranges of one node with different replica sets; the first one is the base range.
    final RingRange baseRange = new RingRange(BigInteger.valueOf(0), BigInteger.valueOf(100));
    final RingRange otherRange = new RingRange(BigInteger.valueOf(100), BigInteger.valueOf(200));
    final UUID hostId = UUID.randomUUID();
    Segment multiRange =
        Segment.builder().withTokenRanges(Lists.newArrayList(baseRange, otherRange)).build();
    UUID runId =
        storage
            .getRepairRunDao()
            .addRepairRun(
                RepairRun.builder(cluster.getName(), unitId)
                    .intensity(0.5)
                    .segmentCount(1)
                    .repairParallelism(RepairParallelism.PARALLEL)
                    .tables(TABLES),
                Lists.newArrayList(RepairSegment.builder(multiRange, unitId).withHostID(hostId)))
            .getId();
    storage
        .getRepairRunDao()
        .updateRepairRun(
            storage
                .getRepairRunDao()
                .getRepairRun(runId)
                .get()
                .with()
                .runState(RepairRun.RunState.RUNNING)
                .startTime(DateTime.now())
                .build(runId));

    // Replica sets per range start: [0,100) -> .1/.2 ; [100,200) -> .3. A segment spanning both
    // ranges has no common replica set, so the full multi-range segment resolves to nothing.
    RepairRunService repairRunService = mock(RepairRunService.class);
    ArgumentCaptor<Segment> segmentCaptor = ArgumentCaptor.forClass(Segment.class);
    when(repairRunService.getDCsByNodeForRepairSegment(
            any(), segmentCaptor.capture(), anyString(), any()))
        .thenAnswer(
            inv -> {
              Segment seg = inv.getArgument(1);
              Map<String, String> replicas = new HashMap<>();
              if (seg.getTokenRanges().size() > 1) {
                return replicas;
              }
              if (seg.getBaseRange().getStart().equals(BigInteger.ZERO)) {
                replicas.put("127.0.0.1", "dc1");
                replicas.put("127.0.0.2", "dc1");
              } else {
                replicas.put("127.0.0.3", "dc1");
              }
              return replicas;
            });

    ICassandraManagementProxy coordinator = mock(ICassandraManagementProxy.class);
    when(coordinator.getUntranslatedHost()).thenReturn("127.0.0.1");
    when(coordinator.getDatacenter(anyString())).thenReturn("dc1");

    ClusterFacade clusterFacade = mock(ClusterFacade.class);
    when(clusterFacade.connect(any(Cluster.class), any())).thenReturn(coordinator);
    when(clusterFacade.getEndpointToHostId(any()))
        .thenReturn(ImmutableMap.of("127.0.0.1", hostId.toString()));
    when(clusterFacade.listActiveCompactions(any()))
        .thenReturn(
            CompactionStats.builder()
                .withActiveCompactions(Collections.emptyList())
                .withPendingCompactions(Optional.of(0))
                .build());

    RepairRunner runner =
        new RepairRunner(
            context, runId, clusterFacade, storage.getRepairRunDao(), repairRunService);

    java.lang.reflect.Method startNextSegment =
        RepairRunner.class.getDeclaredMethod("startNextSegment");
    startNextSegment.setAccessible(true);
    startNextSegment.invoke(runner);

    // Replicas were resolved from a single-range Segment built from the base range only.
    List<Segment> requested = segmentCaptor.getAllValues();
    assertEquals(1, requested.size());
    assertEquals(1, requested.get(0).getTokenRanges().size());
    assertEquals(baseRange.getStart(), requested.get(0).getBaseRange().getStart());
    assertEquals(baseRange.getEnd(), requested.get(0).getBaseRange().getEnd());

    // The non-empty potentialReplicaMap (base range replicas) reached nodesReadyForNewRepair,
    // which checks compactions on exactly those nodes.
    ArgumentCaptor<Node> nodeCaptor = ArgumentCaptor.forClass(Node.class);
    verify(clusterFacade, atLeastOnce()).listActiveCompactions(nodeCaptor.capture());
    Set<String> checkedHosts = Sets.newHashSet();
    nodeCaptor.getAllValues().forEach(n -> checkedHosts.add(n.getHostname()));
    assertEquals(Sets.newHashSet("127.0.0.1", "127.0.0.2"), checkedHosts);

    // The segment is startable: it was handed to the segment executor.
    verify(context.repairManager, timeout(TimeUnit.SECONDS.toMillis(30))).submitSegment(any());
  }
}
