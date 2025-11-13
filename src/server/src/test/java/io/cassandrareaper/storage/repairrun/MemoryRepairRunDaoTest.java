/*
 * Copyright 2024 The Last Pickle Ltd
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

package io.cassandrareaper.storage.repairrun;

import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.RepairRun;
import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.storage.MemoryStorageFacade;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.cassandra.repair.RepairParallelism;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public final class MemoryRepairRunDaoTest {

  private MemoryStorageFacade storage;
  private String clusterName;
  private String keyspaceName;
  private Set<String> tables;
  private UUID repairUnitId;

  @Before
  public void setUp() {
    storage = new MemoryStorageFacade();
    clusterName = "test_cluster";
    keyspaceName = "test_keyspace";
    tables = Sets.newHashSet("table1", "table2");

    // Create cluster
    Cluster cluster =
        Cluster.builder()
            .withName(clusterName)
            .withPartitioner("org.apache.cassandra.dht.Murmur3Partitioner")
            .withSeedHosts(ImmutableSet.of("127.0.0.1"))
            .withState(Cluster.State.ACTIVE)
            .build();
    storage.getClusterDao().addCluster(cluster);

    // Create repair unit
    RepairUnit.Builder unitBuilder =
        RepairUnit.builder()
            .clusterName(clusterName)
            .keyspaceName(keyspaceName)
            .columnFamilies(tables)
            .incrementalRepair(false)
            .subrangeIncrementalRepair(false)
            .nodes(Collections.emptySet())
            .datacenters(Collections.emptySet())
            .blacklistedTables(Collections.emptySet())
            .repairThreadCount(1)
            .timeout(30);

    RepairUnit unit = storage.getRepairUnitDao().addRepairUnit(unitBuilder);
    repairUnitId = unit.getId();
  }

  @After
  public void tearDown() {
    if (storage != null) {
      storage.clearDatabase();
      storage.stop();
    }
  }

  @Test
  public void testCaseInsensitiveClusterNameQuery() {
    // Create a repair run with lowercase cluster name (as stored)
    RepairRun.Builder runBuilder =
        RepairRun.builder(clusterName, repairUnitId)
            .intensity(0.5)
            .segmentCount(10)
            .repairParallelism(RepairParallelism.SEQUENTIAL)
            .cause("test")
            .owner("test_owner")
            .tables(tables);

    RepairRun run = storage.getRepairRunDao().addRepairRun(runBuilder, Collections.emptyList());
    assertNotNull(run);

    // Query with lowercase - should find it
    Collection<RepairRun> runsLowercase =
        storage
            .getRepairRunDao()
            .getRepairRunsForCluster(clusterName.toLowerCase(), Optional.of(10));
    assertEquals(1, runsLowercase.size());
    assertEquals(run.getId(), runsLowercase.iterator().next().getId());

    // Query with uppercase - should still find it (case-insensitive)
    Collection<RepairRun> runsUppercase =
        storage
            .getRepairRunDao()
            .getRepairRunsForCluster(clusterName.toUpperCase(), Optional.of(10));
    assertEquals(1, runsUppercase.size());
    assertEquals(run.getId(), runsUppercase.iterator().next().getId());

    // Query with mixed case - should still find it
    String mixedCase = "TeSt_ClUsTeR";
    Collection<RepairRun> runsMixed =
        storage.getRepairRunDao().getRepairRunsForCluster(mixedCase, Optional.of(10));
    assertEquals(1, runsMixed.size());
    assertEquals(run.getId(), runsMixed.iterator().next().getId());

    // Test getRepairRunIdsForCluster with different cases
    assertEquals(
        1,
        storage
            .getRepairRunDao()
            .getRepairRunIdsForCluster(clusterName.toLowerCase(), Optional.empty())
            .size());
    assertEquals(
        1,
        storage
            .getRepairRunDao()
            .getRepairRunIdsForCluster(clusterName.toUpperCase(), Optional.empty())
            .size());
    assertEquals(
        1, storage.getRepairRunDao().getRepairRunIdsForCluster(mixedCase, Optional.empty()).size());
  }

  @Test
  public void testConcurrentRepairRunAccess() throws InterruptedException {
    // Create multiple repair runs
    int numRuns = 10;
    List<UUID> runIds = new ArrayList<>();

    for (int i = 0; i < numRuns; i++) {
      RepairRun.Builder runBuilder =
          RepairRun.builder(clusterName, repairUnitId)
              .intensity(0.5)
              .segmentCount(10)
              .repairParallelism(RepairParallelism.SEQUENTIAL)
              .cause("test_" + i)
              .owner("test_owner")
              .tables(tables);

      RepairRun run = storage.getRepairRunDao().addRepairRun(runBuilder, Collections.emptyList());
      runIds.add(run.getId());
    }

    // Test concurrent reads
    int numThreads = 10;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch latch = new CountDownLatch(numThreads);
    AtomicInteger successCount = new AtomicInteger(0);

    for (int i = 0; i < numThreads; i++) {
      executor.submit(
          () -> {
            try {
              // Query repair runs
              Collection<RepairRun> runs =
                  storage.getRepairRunDao().getRepairRunsForCluster(clusterName, Optional.of(100));
              if (runs.size() == numRuns) {
                successCount.incrementAndGet();
              }

              // Query by ID
              for (UUID runId : runIds) {
                storage.getRepairRunDao().getRepairRun(runId);
              }
            } finally {
              latch.countDown();
            }
          });
    }

    assertTrue(latch.await(10, TimeUnit.SECONDS));
    assertEquals(
        "All threads should successfully query repair runs", numThreads, successCount.get());
    executor.shutdown();
  }

  @Test
  public void testUpdateRepairRun() {
    // Create a repair run
    RepairRun.Builder runBuilder =
        RepairRun.builder(clusterName, repairUnitId)
            .intensity(0.5)
            .segmentCount(10)
            .repairParallelism(RepairParallelism.SEQUENTIAL)
            .cause("test")
            .owner("test_owner")
            .tables(tables);

    RepairRun run = storage.getRepairRunDao().addRepairRun(runBuilder, Collections.emptyList());
    assertNotNull(run);
    assertEquals(RepairRun.RunState.NOT_STARTED, run.getRunState());

    // Update the run state
    RepairRun updatedRun =
        run.with()
            .runState(RepairRun.RunState.RUNNING)
            .lastEvent("Started repair")
            .build(run.getId());

    boolean updated = storage.getRepairRunDao().updateRepairRun(updatedRun);
    assertTrue(updated);

    // Verify update
    Optional<RepairRun> retrievedRun = storage.getRepairRunDao().getRepairRun(run.getId());
    assertTrue(retrievedRun.isPresent());
    assertEquals(RepairRun.RunState.RUNNING, retrievedRun.get().getRunState());
    assertEquals("Started repair", retrievedRun.get().getLastEvent());
  }

  @Test
  public void testGetRepairRunsWithState() {
    // Create repair runs with different states
    RepairRun.Builder runBuilder1 =
        RepairRun.builder(clusterName, repairUnitId)
            .intensity(0.5)
            .segmentCount(10)
            .repairParallelism(RepairParallelism.SEQUENTIAL)
            .cause("test1")
            .owner("test_owner")
            .tables(tables);

    RepairRun run1 = storage.getRepairRunDao().addRepairRun(runBuilder1, Collections.emptyList());

    // Update first run to RUNNING
    RepairRun runningRun = run1.with().runState(RepairRun.RunState.RUNNING).build(run1.getId());
    storage.getRepairRunDao().updateRepairRun(runningRun);

    // Create second run (will be NOT_STARTED)
    RepairRun.Builder runBuilder2 =
        RepairRun.builder(clusterName, repairUnitId)
            .intensity(0.5)
            .segmentCount(10)
            .repairParallelism(RepairParallelism.SEQUENTIAL)
            .cause("test2")
            .owner("test_owner")
            .tables(tables);

    storage.getRepairRunDao().addRepairRun(runBuilder2, Collections.emptyList());

    // Query by state
    assertEquals(
        1, storage.getRepairRunDao().getRepairRunsWithState(RepairRun.RunState.RUNNING).size());
    assertEquals(
        1, storage.getRepairRunDao().getRepairRunsWithState(RepairRun.RunState.NOT_STARTED).size());
    assertEquals(
        0, storage.getRepairRunDao().getRepairRunsWithState(RepairRun.RunState.DONE).size());
  }

  @Test
  public void testDeleteRepairRun() {
    // Create a repair run
    RepairRun.Builder runBuilder =
        RepairRun.builder(clusterName, repairUnitId)
            .intensity(0.5)
            .segmentCount(10)
            .repairParallelism(RepairParallelism.SEQUENTIAL)
            .cause("test")
            .owner("test_owner")
            .tables(tables);

    RepairRun run = storage.getRepairRunDao().addRepairRun(runBuilder, Collections.emptyList());
    assertNotNull(run);

    // Delete the run
    Optional<RepairRun> deleted = storage.getRepairRunDao().deleteRepairRun(run.getId());
    assertTrue(deleted.isPresent());
    assertEquals(RepairRun.RunState.DELETED, deleted.get().getRunState());

    // Verify it's deleted
    Collection<RepairRun> runs =
        storage.getRepairRunDao().getRepairRunsForCluster(clusterName, Optional.of(10));
    assertEquals(0, runs.size());
  }

  @Test
  public void testGetRepairRunsForUnit() {
    // Create multiple repair runs for the same unit
    int numRuns = 5;
    for (int i = 0; i < numRuns; i++) {
      RepairRun.Builder runBuilder =
          RepairRun.builder(clusterName, repairUnitId)
              .intensity(0.5)
              .segmentCount(10)
              .repairParallelism(RepairParallelism.SEQUENTIAL)
              .cause("test_" + i)
              .owner("test_owner")
              .tables(tables);

      storage.getRepairRunDao().addRepairRun(runBuilder, Collections.emptyList());
    }

    // Query by repair unit
    assertEquals(numRuns, storage.getRepairRunDao().getRepairRunsForUnit(repairUnitId).size());
  }

  @Test
  public void testGetRepairRunsForClusterPrioritiseRunning() {
    // Create runs with different states
    RepairRun.Builder notStartedBuilder =
        RepairRun.builder(clusterName, repairUnitId)
            .intensity(0.5)
            .segmentCount(10)
            .repairParallelism(RepairParallelism.SEQUENTIAL)
            .cause("not_started")
            .owner("test_owner")
            .tables(tables);
    RepairRun notStartedRun =
        storage.getRepairRunDao().addRepairRun(notStartedBuilder, Collections.emptyList());

    RepairRun.Builder runningBuilder =
        RepairRun.builder(clusterName, repairUnitId)
            .intensity(0.5)
            .segmentCount(10)
            .repairParallelism(RepairParallelism.SEQUENTIAL)
            .cause("running")
            .owner("test_owner")
            .tables(tables);
    RepairRun runningRun =
        storage.getRepairRunDao().addRepairRun(runningBuilder, Collections.emptyList());

    // Update one to RUNNING
    RepairRun updated =
        runningRun.with().runState(RepairRun.RunState.RUNNING).build(runningRun.getId());
    storage.getRepairRunDao().updateRepairRun(updated);

    // Get prioritized runs - RUNNING should come first
    Collection<RepairRun> prioritized =
        storage
            .getRepairRunDao()
            .getRepairRunsForClusterPrioritiseRunning(clusterName, Optional.of(10));

    assertEquals(2, prioritized.size());
    assertEquals(RepairRun.RunState.RUNNING, prioritized.iterator().next().getRunState());
  }
}
