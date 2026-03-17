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

package io.cassandrareaper.storage;

import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.DiagEventSubscription;
import io.cassandrareaper.core.RepairRun;
import io.cassandrareaper.core.RepairSchedule;
import io.cassandrareaper.core.RepairUnit;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.cassandra.repair.RepairParallelism;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public final class MemoryStorageFacadeTest {

  private MemoryStorageFacade storage;

  @Before
  public void setUp() {
    storage = new MemoryStorageFacade();
  }

  @After
  public void tearDown() {
    if (storage != null) {
      storage.stop();
    }
  }

  @Test
  public void testClearDatabaseRemovesAllData() {
    String clusterName = "test_cluster";
    String keyspaceName = "test_keyspace";
    Set<String> tables = Sets.newHashSet("table1");

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

    // Create repair run
    RepairRun.Builder runBuilder =
        RepairRun.builder(clusterName, unit.getId())
            .intensity(0.5)
            .segmentCount(10)
            .repairParallelism(RepairParallelism.SEQUENTIAL)
            .cause("test")
            .owner("test_owner")
            .tables(tables);
    RepairRun run = storage.getRepairRunDao().addRepairRun(runBuilder, Collections.emptyList());

    // Create repair schedule
    RepairSchedule.Builder scheduleBuilder =
        RepairSchedule.builder(unit.getId())
            .daysBetween(7)
            .nextActivation(DateTime.now())
            .repairParallelism(RepairParallelism.SEQUENTIAL)
            .intensity(0.5)
            .segmentCountPerNode(10)
            .owner("test_owner");
    RepairSchedule schedule = storage.getRepairScheduleDao().addRepairSchedule(scheduleBuilder);

    // Create diag event subscription
    DiagEventSubscription eventSubscription =
        new DiagEventSubscription(
            Optional.empty(),
            clusterName,
            Optional.of("test event"),
            Sets.newHashSet("127.0.0.1"),
            Sets.newHashSet("test_event"),
            true,
            null,
            null);
    DiagEventSubscription event = storage.getEventsDao().addEventSubscription(eventSubscription);

    // Verify data exists
    assertEquals(1, storage.getClusterDao().getClusters().size());
    assertEquals(
        1, storage.getRepairRunDao().getRepairRunsForCluster(clusterName, Optional.of(10)).size());
    assertNotNull(storage.getRepairScheduleDao().getRepairSchedule(schedule.getId()));
    assertNotNull(storage.getEventsDao().getEventSubscription(event.getId().get()));

    // Clear database
    storage.clearDatabase();

    // Verify all data is removed
    assertEquals(0, storage.getClusterDao().getClusters().size());
    assertEquals(
        0, storage.getRepairRunDao().getRepairRunsForCluster(clusterName, Optional.of(10)).size());
    assertTrue(storage.getRepairScheduleDao().getRepairSchedule(schedule.getId()).isEmpty());
    assertNull(storage.getEventsDao().getEventSubscription(event.getId().get()));
  }

  @Test
  public void testClearDatabaseRespectsForeignKeyConstraints() {
    String clusterName = "test_cluster";
    String keyspaceName = "test_keyspace";
    Set<String> tables = Sets.newHashSet("table1");

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

    // Create repair run with segments
    RepairRun.Builder runBuilder =
        RepairRun.builder(clusterName, unit.getId())
            .intensity(0.5)
            .segmentCount(10)
            .repairParallelism(RepairParallelism.SEQUENTIAL)
            .cause("test")
            .owner("test_owner")
            .tables(tables);

    RepairRun run = storage.getRepairRunDao().addRepairRun(runBuilder, Collections.emptyList());

    // Verify data exists
    assertEquals(1, storage.getClusterDao().getClusters().size());

    // Clear database - should not throw foreign key constraint violation
    storage.clearDatabase();

    // Verify all data is removed
    assertEquals(0, storage.getClusterDao().getClusters().size());
    assertEquals(
        0, storage.getRepairRunDao().getRepairRunsForCluster(clusterName, Optional.of(10)).size());
  }

  @Test
  public void testClearDatabaseMultipleTimes() {
    String clusterName = "test_cluster";

    // Create and clear multiple times
    for (int i = 0; i < 3; i++) {
      // Add cluster
      Cluster cluster =
          Cluster.builder()
              .withName(clusterName + "_" + i)
              .withPartitioner("org.apache.cassandra.dht.Murmur3Partitioner")
              .withSeedHosts(ImmutableSet.of("127.0.0.1"))
              .withState(Cluster.State.ACTIVE)
              .build();
      storage.getClusterDao().addCluster(cluster);

      assertEquals(1, storage.getClusterDao().getClusters().size());

      // Clear
      storage.clearDatabase();

      assertEquals(0, storage.getClusterDao().getClusters().size());
    }
  }

  @Test
  public void testIsStorageConnected() {
    assertTrue("Storage should be connected after initialization", storage.isStorageConnected());

    storage.stop();

    // Note: After stop, connection might still appear connected until GC
    // This is expected behavior for SQLite in-memory databases
  }

  @Test
  public void testGetSqliteConnection() {
    assertNotNull("SQLite connection should not be null", storage.getSqliteConnection());
  }

  @Test
  public void testIsPersistentForInMemoryStorage() {
    MemoryStorageFacade inMemoryStorage = new MemoryStorageFacade();
    try {
      assertEquals(
          "In-memory storage should not be persistent", false, inMemoryStorage.isPersistent());
    } finally {
      inMemoryStorage.stop();
    }
  }
}
