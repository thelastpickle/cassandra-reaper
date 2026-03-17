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

package io.cassandrareaper.storage.repairschedule;

import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.RepairSchedule;
import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.storage.MemoryStorageFacade;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.cassandra.repair.RepairParallelism;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public final class MemoryRepairScheduleDaoTest {

  private MemoryStorageFacade storage;
  private String clusterName;
  private String keyspaceName;
  private Set<String> tables;

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
  }

  @After
  public void tearDown() {
    if (storage != null) {
      storage.clearDatabase();
      storage.stop();
    }
  }

  @Test
  public void testAddAndRetrieveRepairSchedule() {
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

    // Create repair schedule
    DateTime nextActivation = DateTime.now().plusDays(1);
    RepairSchedule.Builder scheduleBuilder =
        RepairSchedule.builder(unit.getId())
            .daysBetween(7)
            .nextActivation(nextActivation)
            .repairParallelism(RepairParallelism.SEQUENTIAL)
            .intensity(0.5)
            .segmentCountPerNode(10)
            .owner("test_owner")
            .adaptive(false)
            .percentUnrepairedThreshold(10);

    RepairSchedule schedule = storage.getRepairScheduleDao().addRepairSchedule(scheduleBuilder);
    assertNotNull(schedule);
    assertNotNull(schedule.getId());

    // Retrieve and verify
    Optional<RepairSchedule> retrieved =
        storage.getRepairScheduleDao().getRepairSchedule(schedule.getId());
    assertTrue(retrieved.isPresent());
    assertEquals(unit.getId(), retrieved.get().getRepairUnitId());
    assertEquals(Integer.valueOf(7), Integer.valueOf(retrieved.get().getDaysBetween()));
    assertEquals(0.5, retrieved.get().getIntensity(), 0.001);
    assertEquals(Integer.valueOf(10), Integer.valueOf(retrieved.get().getSegmentCountPerNode()));
    assertEquals("test_owner", retrieved.get().getOwner());
    assertFalse(retrieved.get().getAdaptive());
    assertEquals(
        Integer.valueOf(10), Integer.valueOf(retrieved.get().getPercentUnrepairedThreshold()));
  }

  @Test
  public void testUpdateRepairSchedule() {
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

    // Create repair schedule
    RepairSchedule.Builder scheduleBuilder =
        RepairSchedule.builder(unit.getId())
            .daysBetween(7)
            .nextActivation(DateTime.now().plusDays(1))
            .repairParallelism(RepairParallelism.SEQUENTIAL)
            .intensity(0.5)
            .segmentCountPerNode(10)
            .owner("test_owner");

    RepairSchedule schedule = storage.getRepairScheduleDao().addRepairSchedule(scheduleBuilder);

    // Update schedule
    DateTime newNextActivation = DateTime.now().plusDays(2);
    RepairSchedule updatedSchedule =
        schedule
            .with()
            .state(RepairSchedule.State.ACTIVE)
            .nextActivation(newNextActivation)
            .intensity(0.7)
            .build(schedule.getId());

    boolean updated = storage.getRepairScheduleDao().updateRepairSchedule(updatedSchedule);
    assertTrue(updated);

    // Verify update
    Optional<RepairSchedule> retrieved =
        storage.getRepairScheduleDao().getRepairSchedule(schedule.getId());
    assertTrue(retrieved.isPresent());
    assertEquals(RepairSchedule.State.ACTIVE, retrieved.get().getState());
    assertEquals(0.7, retrieved.get().getIntensity(), 0.001);
  }

  @Test
  public void testDeleteRepairSchedule() {
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

    // Create repair schedule
    RepairSchedule.Builder scheduleBuilder =
        RepairSchedule.builder(unit.getId())
            .daysBetween(7)
            .nextActivation(DateTime.now().plusDays(1))
            .repairParallelism(RepairParallelism.SEQUENTIAL)
            .intensity(0.5)
            .segmentCountPerNode(10)
            .owner("test_owner");

    RepairSchedule schedule = storage.getRepairScheduleDao().addRepairSchedule(scheduleBuilder);

    // Delete schedule
    Optional<RepairSchedule> deleted =
        storage.getRepairScheduleDao().deleteRepairSchedule(schedule.getId());
    assertTrue(deleted.isPresent());

    // Verify it's deleted
    Optional<RepairSchedule> retrieved =
        storage.getRepairScheduleDao().getRepairSchedule(schedule.getId());
    assertFalse(retrieved.isPresent());
  }

  @Test
  public void testGetAllRepairSchedules() {
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

    // Create multiple schedules
    for (int i = 0; i < 5; i++) {
      RepairSchedule.Builder scheduleBuilder =
          RepairSchedule.builder(unit.getId())
              .daysBetween(7 + i)
              .nextActivation(DateTime.now().plusDays(i + 1))
              .repairParallelism(RepairParallelism.SEQUENTIAL)
              .intensity(0.5)
              .segmentCountPerNode(10)
              .owner("test_owner_" + i);

      storage.getRepairScheduleDao().addRepairSchedule(scheduleBuilder);
    }

    // Get all schedules
    Collection<RepairSchedule> allSchedules =
        storage.getRepairScheduleDao().getAllRepairSchedules();
    assertEquals(5, allSchedules.size());
  }

  @Test
  public void testGetRepairSchedulesForCluster() {
    // Create multiple repair units for different clusters
    RepairUnit.Builder unit1Builder =
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
    RepairUnit unit1 = storage.getRepairUnitDao().addRepairUnit(unit1Builder);

    // Create another cluster
    String cluster2Name = "cluster2";
    Cluster cluster2 =
        Cluster.builder()
            .withName(cluster2Name)
            .withPartitioner("org.apache.cassandra.dht.Murmur3Partitioner")
            .withSeedHosts(ImmutableSet.of("127.0.0.2"))
            .withState(Cluster.State.ACTIVE)
            .build();
    storage.getClusterDao().addCluster(cluster2);

    RepairUnit.Builder unit2Builder =
        RepairUnit.builder()
            .clusterName(cluster2Name)
            .keyspaceName(keyspaceName)
            .columnFamilies(tables)
            .incrementalRepair(false)
            .subrangeIncrementalRepair(false)
            .nodes(Collections.emptySet())
            .datacenters(Collections.emptySet())
            .blacklistedTables(Collections.emptySet())
            .repairThreadCount(1)
            .timeout(30);
    RepairUnit unit2 = storage.getRepairUnitDao().addRepairUnit(unit2Builder);

    // Create schedules for both clusters
    RepairSchedule.Builder schedule1Builder =
        RepairSchedule.builder(unit1.getId())
            .daysBetween(7)
            .nextActivation(DateTime.now().plusDays(1))
            .repairParallelism(RepairParallelism.SEQUENTIAL)
            .intensity(0.5)
            .segmentCountPerNode(10)
            .owner("owner1");
    storage.getRepairScheduleDao().addRepairSchedule(schedule1Builder);

    RepairSchedule.Builder schedule2Builder =
        RepairSchedule.builder(unit2.getId())
            .daysBetween(7)
            .nextActivation(DateTime.now().plusDays(1))
            .repairParallelism(RepairParallelism.SEQUENTIAL)
            .intensity(0.5)
            .segmentCountPerNode(10)
            .owner("owner2");
    storage.getRepairScheduleDao().addRepairSchedule(schedule2Builder);

    // Get schedules for first cluster
    Collection<RepairSchedule> cluster1Schedules =
        storage.getRepairScheduleDao().getRepairSchedulesForCluster(clusterName);
    assertEquals(1, cluster1Schedules.size());

    // Get schedules for second cluster
    Collection<RepairSchedule> cluster2Schedules =
        storage.getRepairScheduleDao().getRepairSchedulesForCluster(cluster2Name);
    assertEquals(1, cluster2Schedules.size());
  }

  @Test
  public void testGetRepairSchedulesForKeyspace() {
    // Create repair units for different keyspaces
    String keyspace1 = "keyspace1";
    String keyspace2 = "keyspace2";

    RepairUnit.Builder unit1Builder =
        RepairUnit.builder()
            .clusterName(clusterName)
            .keyspaceName(keyspace1)
            .columnFamilies(tables)
            .incrementalRepair(false)
            .subrangeIncrementalRepair(false)
            .nodes(Collections.emptySet())
            .datacenters(Collections.emptySet())
            .blacklistedTables(Collections.emptySet())
            .repairThreadCount(1)
            .timeout(30);
    RepairUnit unit1 = storage.getRepairUnitDao().addRepairUnit(unit1Builder);

    RepairUnit.Builder unit2Builder =
        RepairUnit.builder()
            .clusterName(clusterName)
            .keyspaceName(keyspace2)
            .columnFamilies(tables)
            .incrementalRepair(false)
            .subrangeIncrementalRepair(false)
            .nodes(Collections.emptySet())
            .datacenters(Collections.emptySet())
            .blacklistedTables(Collections.emptySet())
            .repairThreadCount(1)
            .timeout(30);
    RepairUnit unit2 = storage.getRepairUnitDao().addRepairUnit(unit2Builder);

    // Create schedules for both keyspaces
    RepairSchedule.Builder schedule1Builder =
        RepairSchedule.builder(unit1.getId())
            .daysBetween(7)
            .nextActivation(DateTime.now().plusDays(1))
            .repairParallelism(RepairParallelism.SEQUENTIAL)
            .intensity(0.5)
            .segmentCountPerNode(10)
            .owner("owner1");
    storage.getRepairScheduleDao().addRepairSchedule(schedule1Builder);

    RepairSchedule.Builder schedule2Builder =
        RepairSchedule.builder(unit2.getId())
            .daysBetween(7)
            .nextActivation(DateTime.now().plusDays(1))
            .repairParallelism(RepairParallelism.SEQUENTIAL)
            .intensity(0.5)
            .segmentCountPerNode(10)
            .owner("owner2");
    storage.getRepairScheduleDao().addRepairSchedule(schedule2Builder);

    // Get schedules for first keyspace
    Collection<RepairSchedule> keyspace1Schedules =
        storage
            .getRepairScheduleDao()
            .getRepairSchedulesForClusterAndKeyspace(clusterName, keyspace1);
    assertEquals(1, keyspace1Schedules.size());

    // Get schedules for second keyspace
    Collection<RepairSchedule> keyspace2Schedules =
        storage
            .getRepairScheduleDao()
            .getRepairSchedulesForClusterAndKeyspace(clusterName, keyspace2);
    assertEquals(1, keyspace2Schedules.size());
  }

  @Test
  public void testAdaptiveSchedule() {
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

    // Create adaptive schedule
    RepairSchedule.Builder scheduleBuilder =
        RepairSchedule.builder(unit.getId())
            .daysBetween(7)
            .nextActivation(DateTime.now().plusDays(1))
            .repairParallelism(RepairParallelism.SEQUENTIAL)
            .intensity(0.5)
            .segmentCountPerNode(10)
            .owner("test_owner")
            .adaptive(true)
            .percentUnrepairedThreshold(20);

    RepairSchedule schedule = storage.getRepairScheduleDao().addRepairSchedule(scheduleBuilder);

    // Verify adaptive settings
    Optional<RepairSchedule> retrieved =
        storage.getRepairScheduleDao().getRepairSchedule(schedule.getId());
    assertTrue(retrieved.isPresent());
    assertTrue(retrieved.get().getAdaptive());
    assertEquals(
        Integer.valueOf(20), Integer.valueOf(retrieved.get().getPercentUnrepairedThreshold()));
  }
}
