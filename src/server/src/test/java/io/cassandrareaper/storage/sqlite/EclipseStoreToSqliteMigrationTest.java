/*
 * Copyright 2025-2025 The Last Pickle Ltd
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

package io.cassandrareaper.storage.sqlite;

import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.RepairRun;
import io.cassandrareaper.core.RepairSchedule;
import io.cassandrareaper.core.RepairSegment;
import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.storage.memory.MemoryStorageRoot;

import java.io.File;
import java.math.BigInteger;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.Sets;
import org.apache.cassandra.repair.RepairParallelism;
import org.eclipse.store.storage.embedded.types.EmbeddedStorage;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for EclipseStore to SQLite migration.
 *
 * <p>These tests verify that the migration correctly transfers data from the old EclipseStore
 * format to the new SQLite database.
 */
public class EclipseStoreToSqliteMigrationTest {

  private File tempStorageDir;
  private Connection sqliteConnection;

  @Before
  public void setUp() throws Exception {
    // Create temp directory for EclipseStore
    tempStorageDir = Files.createTempDirectory("migration-test").toFile();

    // Create in-memory SQLite connection
    sqliteConnection = DriverManager.getConnection("jdbc:sqlite:file::memory:?cache=private");
    sqliteConnection.setAutoCommit(true);

    // Initialize SQLite schema
    SqliteMigrationManager.initializeSchema(sqliteConnection);
  }

  @After
  public void tearDown() throws Exception {
    if (sqliteConnection != null && !sqliteConnection.isClosed()) {
      sqliteConnection.close();
    }
    if (tempStorageDir != null && tempStorageDir.exists()) {
      deleteDirectory(tempStorageDir);
    }
  }

  @Test
  public void testNoMigrationWhenNoEclipseStoreData() throws Exception {
    // Empty directory - no EclipseStore data
    boolean migrated =
        EclipseStoreToSqliteMigration.migrateIfNeeded(tempStorageDir, sqliteConnection);

    // Should return false (nothing to migrate)
    assertThat(migrated).isFalse();
  }

  @Test
  public void testMigrateEmptyStorage() throws Exception {
    // Create empty EclipseStore
    MemoryStorageRoot root = new MemoryStorageRoot();
    EmbeddedStorageManager storage = EmbeddedStorage.Foundation(tempStorageDir.toPath()).start();
    storage.setRoot(root);
    storage.storeRoot();
    storage.shutdown();

    // Migrate
    boolean migrated =
        EclipseStoreToSqliteMigration.migrateIfNeeded(tempStorageDir, sqliteConnection);

    // Verify
    assertThat(migrated).isTrue();
    assertThat(countClusters()).isEqualTo(0);
    assertThat(countRepairUnits()).isEqualTo(0);
  }

  @Test
  public void testMigrateSingleCluster() throws Exception {
    // Create EclipseStore with 1 cluster
    MemoryStorageRoot root = new MemoryStorageRoot();
    // Use HashSet instead of ImmutableSet to avoid Guava serialization issues
    Cluster cluster =
        Cluster.builder()
            .withName("test-cluster")
            .withPartitioner("org.apache.cassandra.dht.Murmur3Partitioner")
            .withSeedHosts(Sets.newHashSet("127.0.0.1", "127.0.0.2"))
            .withState(Cluster.State.ACTIVE)
            .build();
    root.getClusters().put(cluster.getName(), cluster);

    EmbeddedStorageManager storage = EmbeddedStorage.Foundation(tempStorageDir.toPath()).start();
    storage.setRoot(root);
    storage.storeRoot();
    storage.shutdown();

    // Migrate
    boolean migrated =
        EclipseStoreToSqliteMigration.migrateIfNeeded(tempStorageDir, sqliteConnection);

    // Verify
    assertThat(migrated).isTrue();
    assertThat(countClusters()).isEqualTo(1);

    // Verify cluster data
    String sql = "SELECT name, partitioner, seed_hosts, state FROM cluster WHERE name = ?";
    try (PreparedStatement stmt = sqliteConnection.prepareStatement(sql)) {
      stmt.setString(1, "test-cluster");
      ResultSet rs = stmt.executeQuery();
      assertThat(rs.next()).isTrue();
      assertThat(rs.getString("name")).isEqualTo("test-cluster");
      assertThat(rs.getString("partitioner"))
          .isEqualTo("org.apache.cassandra.dht.Murmur3Partitioner");
      assertThat(rs.getString("seed_hosts")).contains("127.0.0.1");
      assertThat(rs.getString("seed_hosts")).contains("127.0.0.2");
      assertThat(rs.getString("state")).isEqualTo("ACTIVE");
    }
  }

  @Test
  public void testMigrateClusterWithRepairUnit() throws Exception {
    // Create EclipseStore with cluster + repair unit
    MemoryStorageRoot root = new MemoryStorageRoot();

    // Use HashSet instead of ImmutableSet to avoid Guava serialization issues
    Cluster cluster =
        Cluster.builder()
            .withName("test-cluster")
            .withSeedHosts(Sets.newHashSet("127.0.0.1"))
            .withState(Cluster.State.ACTIVE)
            .build();
    root.getClusters().put(cluster.getName(), cluster);

    UUID repairUnitId = UUIDs.timeBased();
    RepairUnit repairUnit =
        RepairUnit.builder()
            .clusterName("test-cluster")
            .keyspaceName("test_keyspace")
            .columnFamilies(Sets.newHashSet("table1", "table2"))
            .incrementalRepair(false)
            .subrangeIncrementalRepair(false)
            .nodes(Sets.newHashSet("node1", "node2"))
            .datacenters(Sets.newHashSet("dc1"))
            .blacklistedTables(Sets.newHashSet())
            .repairThreadCount(1)
            .timeout(30)
            .build(repairUnitId);
    root.getRepairUnits().put(repairUnitId, repairUnit);

    EmbeddedStorageManager storage = EmbeddedStorage.Foundation(tempStorageDir.toPath()).start();
    storage.setRoot(root);
    storage.storeRoot();
    storage.shutdown();

    // Migrate
    boolean migrated =
        EclipseStoreToSqliteMigration.migrateIfNeeded(tempStorageDir, sqliteConnection);

    // Verify
    assertThat(migrated).isTrue();
    assertThat(countClusters()).isEqualTo(1);
    assertThat(countRepairUnits()).isEqualTo(1);

    // Verify repair unit data
    String sql =
        "SELECT cluster_name, keyspace_name, column_families, incremental_repair "
            + "FROM repair_unit WHERE cluster_name = ?";
    try (PreparedStatement stmt = sqliteConnection.prepareStatement(sql)) {
      stmt.setString(1, "test-cluster");
      ResultSet rs = stmt.executeQuery();
      assertThat(rs.next()).isTrue();
      assertThat(rs.getString("cluster_name")).isEqualTo("test-cluster");
      assertThat(rs.getString("keyspace_name")).isEqualTo("test_keyspace");
      assertThat(rs.getString("column_families")).contains("table1");
      assertThat(rs.getString("column_families")).contains("table2");
      assertThat(rs.getInt("incremental_repair")).isEqualTo(0);
    }
  }

  @Test
  public void testMigrateCompleteDataSet() throws Exception {
    // Create full dataset: cluster, repair unit, repair run, schedule, segments
    MemoryStorageRoot root = new MemoryStorageRoot();

    // Cluster (use HashSet instead of ImmutableSet to avoid Guava serialization issues)
    Cluster cluster =
        Cluster.builder()
            .withName("full-cluster")
            .withSeedHosts(Sets.newHashSet("10.0.0.1"))
            .withState(Cluster.State.ACTIVE)
            .build();
    root.getClusters().put(cluster.getName(), cluster);

    // Repair Unit
    UUID repairUnitId = UUIDs.timeBased();
    RepairUnit repairUnit =
        RepairUnit.builder()
            .clusterName("full-cluster")
            .keyspaceName("ks1")
            .columnFamilies(Sets.newHashSet("cf1"))
            .incrementalRepair(false)
            .subrangeIncrementalRepair(false)
            .nodes(Sets.newHashSet())
            .datacenters(Sets.newHashSet())
            .blacklistedTables(Sets.newHashSet())
            .repairThreadCount(1)
            .timeout(30)
            .build(repairUnitId);
    root.getRepairUnits().put(repairUnitId, repairUnit);

    // Repair Run
    UUID repairRunId = UUIDs.timeBased();
    RepairRun repairRun =
        RepairRun.builder("full-cluster", repairUnitId)
            .intensity(0.9)
            .segmentCount(5)
            .repairParallelism(RepairParallelism.PARALLEL)
            .tables(Sets.newHashSet("cf1"))
            .runState(RepairRun.RunState.RUNNING)
            .startTime(DateTime.now())
            .build(repairRunId);
    root.getRepairRuns().put(repairRunId, repairRun);

    // Repair Schedule
    UUID scheduleId = UUIDs.timeBased();
    RepairSchedule schedule =
        RepairSchedule.builder(repairUnitId)
            .daysBetween(7)
            .owner("test-owner")
            .repairParallelism(RepairParallelism.PARALLEL)
            .intensity(0.9)
            .segmentCountPerNode(16)
            .nextActivation(DateTime.now())
            .build(scheduleId);
    root.getRepairSchedules().put(scheduleId, schedule);

    // Note: RepairSegments are created automatically when repair runs are added,
    // so we don't need to manually create them for this migration test

    EmbeddedStorageManager storage = EmbeddedStorage.Foundation(tempStorageDir.toPath()).start();
    storage.setRoot(root);
    storage.storeRoot();
    storage.shutdown();

    // Migrate
    boolean migrated =
        EclipseStoreToSqliteMigration.migrateIfNeeded(tempStorageDir, sqliteConnection);

    // Verify all entities migrated
    assertThat(migrated).isTrue();
    assertThat(countClusters()).isEqualTo(1);
    assertThat(countRepairUnits()).isEqualTo(1);
    assertThat(countRepairRuns()).isEqualTo(1);
    assertThat(countRepairSchedules()).isEqualTo(1);
    // RepairSegments are not manually created in this test
  }

  @Test
  public void testMigrationIdempotency() throws Exception {
    // Create data and migrate once
    MemoryStorageRoot root = new MemoryStorageRoot();
    // Use HashSet instead of ImmutableSet to avoid Guava serialization issues
    Cluster cluster =
        Cluster.builder()
            .withName("idempotent-cluster")
            .withSeedHosts(Sets.newHashSet("127.0.0.1"))
            .withState(Cluster.State.ACTIVE)
            .build();
    root.getClusters().put(cluster.getName(), cluster);

    EmbeddedStorageManager storage = EmbeddedStorage.Foundation(tempStorageDir.toPath()).start();
    storage.setRoot(root);
    storage.storeRoot();
    storage.shutdown();

    boolean firstMigration =
        EclipseStoreToSqliteMigration.migrateIfNeeded(tempStorageDir, sqliteConnection);
    assertThat(firstMigration).isTrue();

    // Try to migrate again - should detect backup and skip
    boolean secondMigration =
        EclipseStoreToSqliteMigration.migrateIfNeeded(tempStorageDir, sqliteConnection);
    assertThat(secondMigration).isFalse(); // Already migrated
  }

  @Test
  public void testBackupCreated() throws Exception {
    // Create and migrate data
    MemoryStorageRoot root = new MemoryStorageRoot();
    // Use HashSet instead of ImmutableSet to avoid Guava serialization issues
    Cluster cluster =
        Cluster.builder()
            .withName("backup-test-cluster")
            .withSeedHosts(Sets.newHashSet("127.0.0.1"))
            .withState(Cluster.State.ACTIVE)
            .build();
    root.getClusters().put(cluster.getName(), cluster);

    EmbeddedStorageManager storage = EmbeddedStorage.Foundation(tempStorageDir.toPath()).start();
    storage.setRoot(root);
    storage.storeRoot();
    storage.shutdown();

    EclipseStoreToSqliteMigration.migrateIfNeeded(tempStorageDir, sqliteConnection);

    // Verify backup exists
    File backupDir = new File(tempStorageDir, ".eclipsestore.backup");
    assertThat(backupDir.exists()).isTrue();
    assertThat(backupDir.isDirectory()).isTrue();

    // Verify original channel_0 file was moved to backup
    File backupChannel = new File(backupDir, "channel_0");
    assertThat(backupChannel.exists()).isTrue();
  }

  @Test
  public void testMigrateMultipleClusters() throws Exception {
    // Create 5 clusters, each with repair units and schedules
    MemoryStorageRoot root = new MemoryStorageRoot();

    for (int i = 0; i < 5; i++) {
      // Create cluster
      Cluster cluster =
          Cluster.builder()
              .withName("multi-cluster-" + i)
              .withSeedHosts(Sets.newHashSet("10.0.0." + i))
              .withState(i % 2 == 0 ? Cluster.State.ACTIVE : Cluster.State.UNREACHABLE)
              .withPartitioner("org.apache.cassandra.dht.Murmur3Partitioner")
              .build();
      root.getClusters().put(cluster.getName(), cluster);

      // Create 2 repair units per cluster
      for (int j = 0; j < 2; j++) {
        UUID repairUnitId = UUIDs.timeBased();
        RepairUnit repairUnit =
            RepairUnit.builder()
                .clusterName("multi-cluster-" + i)
                .keyspaceName("keyspace_" + j)
                .columnFamilies(Sets.newHashSet("table_" + j))
                .incrementalRepair(j % 2 == 0)
                .subrangeIncrementalRepair(false)
                .nodes(Sets.newHashSet())
                .datacenters(Sets.newHashSet())
                .blacklistedTables(Sets.newHashSet())
                .repairThreadCount(1)
                .timeout(30)
                .build(repairUnitId);
        root.getRepairUnits().put(repairUnitId, repairUnit);
      }
    }

    EmbeddedStorageManager storage = EmbeddedStorage.Foundation(tempStorageDir.toPath()).start();
    storage.setRoot(root);
    storage.storeRoot();
    storage.shutdown();

    // Migrate
    boolean migrated =
        EclipseStoreToSqliteMigration.migrateIfNeeded(tempStorageDir, sqliteConnection);

    // Verify all entities migrated
    assertThat(migrated).isTrue();
    assertThat(countClusters()).isEqualTo(5);
    assertThat(countRepairUnits()).isEqualTo(10); // 5 clusters * 2 units each

    // Verify different cluster states were preserved
    String sql = "SELECT state FROM cluster ORDER BY name";
    try (Statement stmt = sqliteConnection.createStatement();
        ResultSet rs = stmt.executeQuery(sql)) {
      int activeCount = 0;
      int unreachableCount = 0;
      while (rs.next()) {
        String state = rs.getString("state");
        if ("ACTIVE".equals(state)) {
          activeCount++;
        } else if ("UNREACHABLE".equals(state)) {
          unreachableCount++;
        }
      }
      assertThat(activeCount).isEqualTo(3); // cluster-0, cluster-2, cluster-4
      assertThat(unreachableCount).isEqualTo(2); // cluster-1, cluster-3
    }
  }

  @Test
  public void testMigrateRepairRunAllStates() throws Exception {
    // Create cluster, repair unit, and repair runs in all possible states
    MemoryStorageRoot root = new MemoryStorageRoot();

    // Setup cluster
    Cluster cluster =
        Cluster.builder()
            .withName("state-test-cluster")
            .withSeedHosts(Sets.newHashSet("127.0.0.1"))
            .withState(Cluster.State.ACTIVE)
            .build();
    root.getClusters().put(cluster.getName(), cluster);

    // Setup repair unit
    UUID repairUnitId = UUIDs.timeBased();
    RepairUnit repairUnit =
        RepairUnit.builder()
            .clusterName("state-test-cluster")
            .keyspaceName("test_ks")
            .columnFamilies(Sets.newHashSet("test_table"))
            .incrementalRepair(false)
            .subrangeIncrementalRepair(false)
            .nodes(Sets.newHashSet())
            .datacenters(Sets.newHashSet())
            .blacklistedTables(Sets.newHashSet())
            .repairThreadCount(1)
            .timeout(30)
            .build(repairUnitId);
    root.getRepairUnits().put(repairUnitId, repairUnit);

    // Create repair runs in each state
    RepairRun.RunState[] states = {
      RepairRun.RunState.NOT_STARTED,
      RepairRun.RunState.RUNNING,
      RepairRun.RunState.PAUSED,
      RepairRun.RunState.DONE,
      RepairRun.RunState.ERROR,
      RepairRun.RunState.ABORTED,
      RepairRun.RunState.DELETED
    };

    for (RepairRun.RunState state : states) {
      UUID runId = UUIDs.timeBased();
      RepairRun.Builder builder =
          RepairRun.builder("state-test-cluster", repairUnitId)
              .intensity(0.9)
              .segmentCount(5)
              .repairParallelism(RepairParallelism.PARALLEL)
              .tables(Sets.newHashSet("test_table"))
              .runState(state);

      // Only set startTime for states that have started
      if (state != RepairRun.RunState.NOT_STARTED) {
        builder.startTime(DateTime.now().minusHours(1));
      }

      // Set pauseTime for PAUSED state
      if (state == RepairRun.RunState.PAUSED) {
        builder.pauseTime(DateTime.now().minusMinutes(30));
      }

      // Set endTime for completed states
      if (state == RepairRun.RunState.DONE
          || state == RepairRun.RunState.ERROR
          || state == RepairRun.RunState.ABORTED
          || state == RepairRun.RunState.DELETED) {
        builder.endTime(DateTime.now());
      }

      RepairRun run = builder.build(runId);
      root.getRepairRuns().put(runId, run);
    }

    EmbeddedStorageManager storage = EmbeddedStorage.Foundation(tempStorageDir.toPath()).start();
    storage.setRoot(root);
    storage.storeRoot();
    storage.shutdown();

    // Migrate
    boolean migrated =
        EclipseStoreToSqliteMigration.migrateIfNeeded(tempStorageDir, sqliteConnection);

    // Verify all states migrated correctly
    assertThat(migrated).isTrue();
    assertThat(countRepairRuns()).isEqualTo(7);

    // Verify each state was preserved
    String sql = "SELECT state, start_time, end_time FROM repair_run ORDER BY state";
    try (Statement stmt = sqliteConnection.createStatement();
        ResultSet rs = stmt.executeQuery(sql)) {
      java.util.Set<String> migratedStates = new java.util.HashSet<>();
      while (rs.next()) {
        String state = rs.getString("state");
        migratedStates.add(state);

        // Verify startTime/endTime consistency with state
        if ("NOT_STARTED".equals(state)) {
          // NOT_STARTED repairs should have no start time (NULL or 0 in SQLite)
          Object startTime = rs.getObject("start_time");
          assertThat(startTime == null || startTime.equals(0L) || startTime.equals(0))
              .as("NOT_STARTED repair should have NULL or 0 start_time")
              .isTrue();
        } else {
          assertThat(rs.getObject("start_time")).isNotNull();
        }

        if ("DONE".equals(state)
            || "ERROR".equals(state)
            || "ABORTED".equals(state)
            || "DELETED".equals(state)) {
          assertThat(rs.getObject("end_time")).isNotNull();
        }
      }

      // Verify all states were migrated
      assertThat(migratedStates).hasSize(7);
      assertThat(migratedStates)
          .containsExactlyInAnyOrder(
              "NOT_STARTED", "RUNNING", "PAUSED", "DONE", "ERROR", "ABORTED", "DELETED");
    }
  }

  @Test
  public void testMigrateRepairSegmentAllStates() throws Exception {
    // Create repair segments in all possible states
    MemoryStorageRoot root = new MemoryStorageRoot();

    // Setup cluster
    Cluster cluster =
        Cluster.builder()
            .withName("segment-state-cluster")
            .withSeedHosts(Sets.newHashSet("127.0.0.1"))
            .withState(Cluster.State.ACTIVE)
            .build();
    root.getClusters().put(cluster.getName(), cluster);

    // Setup repair unit
    UUID repairUnitId = UUIDs.timeBased();
    RepairUnit repairUnit =
        RepairUnit.builder()
            .clusterName("segment-state-cluster")
            .keyspaceName("test_ks")
            .columnFamilies(Sets.newHashSet("test_table"))
            .incrementalRepair(false)
            .subrangeIncrementalRepair(false)
            .nodes(Sets.newHashSet())
            .datacenters(Sets.newHashSet())
            .blacklistedTables(Sets.newHashSet())
            .repairThreadCount(1)
            .timeout(30)
            .build(repairUnitId);
    root.getRepairUnits().put(repairUnitId, repairUnit);

    // Setup repair run
    UUID repairRunId = UUIDs.timeBased();
    RepairRun repairRun =
        RepairRun.builder("segment-state-cluster", repairUnitId)
            .intensity(0.9)
            .segmentCount(4)
            .repairParallelism(RepairParallelism.PARALLEL)
            .tables(Sets.newHashSet("test_table"))
            .runState(RepairRun.RunState.RUNNING)
            .startTime(DateTime.now())
            .build(repairRunId);
    root.getRepairRuns().put(repairRunId, repairRun);

    // Create segments in each state
    RepairSegment.State[] states = {
      RepairSegment.State.NOT_STARTED,
      RepairSegment.State.RUNNING,
      RepairSegment.State.DONE,
      RepairSegment.State.STARTED
    };

    for (int i = 0; i < states.length; i++) {
      UUID segmentId = UUIDs.timeBased();
      io.cassandrareaper.service.RingRange range =
          new io.cassandrareaper.service.RingRange(
              new BigInteger(String.valueOf(i * 100)),
              new BigInteger(String.valueOf((i + 1) * 100)));
      io.cassandrareaper.core.Segment tokenRange =
          io.cassandrareaper.core.Segment.builder()
              .withTokenRange(range)
              .withReplicas(new java.util.HashMap<>())
              .build();

      RepairSegment.Builder builder =
          RepairSegment.builder(tokenRange, repairUnitId)
              .withRunId(repairRunId)
              .withState(states[i])
              .withCoordinatorHost("127.0.0.1")
              .withFailCount(i); // Different fail counts for variety

      // Add timestamps for started/running/done states
      if (states[i] != RepairSegment.State.NOT_STARTED) {
        builder.withStartTime(DateTime.now().minusMinutes(i * 10));
      }
      if (states[i] == RepairSegment.State.DONE) {
        builder.withEndTime(DateTime.now());
      }

      RepairSegment segment = builder.withId(segmentId).build();
      root.getRepairSegments().put(segmentId, segment);
    }

    EmbeddedStorageManager storage = EmbeddedStorage.Foundation(tempStorageDir.toPath()).start();
    storage.setRoot(root);
    storage.storeRoot();
    storage.shutdown();

    // Migrate
    boolean migrated =
        EclipseStoreToSqliteMigration.migrateIfNeeded(tempStorageDir, sqliteConnection);

    // Verify all states migrated correctly
    assertThat(migrated).isTrue();
    assertThat(countRepairSegments()).isEqualTo(4);

    // Verify each state was preserved
    String sql =
        "SELECT state, start_time, end_time, fail_count FROM repair_segment ORDER BY fail_count";
    try (Statement stmt = sqliteConnection.createStatement();
        ResultSet rs = stmt.executeQuery(sql)) {
      java.util.Set<String> migratedStates = new java.util.HashSet<>();
      int segmentCount = 0;
      while (rs.next()) {
        String state = rs.getString("state");
        migratedStates.add(state);
        segmentCount++;

        // Verify startTime/endTime consistency with state
        if ("NOT_STARTED".equals(state)) {
          // NOT_STARTED segments should have no start/end time (NULL or 0 in SQLite)
          Object startTime = rs.getObject("start_time");
          Object endTime = rs.getObject("end_time");
          assertThat(startTime == null || startTime.equals(0L) || startTime.equals(0))
              .as("NOT_STARTED segment should have NULL or 0 start_time")
              .isTrue();
          assertThat(endTime == null || endTime.equals(0L) || endTime.equals(0))
              .as("NOT_STARTED segment should have NULL or 0 end_time")
              .isTrue();
        } else if ("DONE".equals(state)) {
          assertThat(rs.getObject("start_time")).isNotNull();
          assertThat(rs.getObject("end_time")).isNotNull();
        } else {
          assertThat(rs.getObject("start_time")).isNotNull();
        }
      }

      assertThat(segmentCount).isEqualTo(4);
      assertThat(migratedStates).hasSize(4);
      assertThat(migratedStates)
          .containsExactlyInAnyOrder("NOT_STARTED", "RUNNING", "DONE", "STARTED");
    }
  }

  @Test
  public void testMigrateClusterWithEmptySeedHosts() throws Exception {
    // This tests handling of empty seed hosts (edge case from corrupted data)
    MemoryStorageRoot root = new MemoryStorageRoot();

    // We can't directly create a Cluster with empty seedHosts via builder (validation fails)
    // So we'll use reflection to simulate corrupted/edge-case data from old EclipseStore
    Cluster normalCluster =
        Cluster.builder()
            .withName("placeholder-cluster")
            .withSeedHosts(Sets.newHashSet("127.0.0.1")) // Start with valid
            .withState(Cluster.State.ACTIVE)
            .build();

    // Use reflection to replace seedHosts with empty set (simulating edge case)
    try {
      java.lang.reflect.Field seedHostsField = Cluster.class.getDeclaredField("seedHosts");
      seedHostsField.setAccessible(true);
      seedHostsField.set(normalCluster, java.util.Collections.emptySet());
    } catch (NoSuchFieldException | IllegalAccessException e) {
      // If reflection fails, skip this test (field structure may have changed)
      org.junit.Assume.assumeNoException(
          "Skipping test - unable to simulate empty seedHosts via reflection", e);
      return;
    }

    root.getClusters().put(normalCluster.getName(), normalCluster);

    EmbeddedStorageManager storage = EmbeddedStorage.Foundation(tempStorageDir.toPath()).start();
    storage.setRoot(root);
    storage.storeRoot();
    storage.shutdown();

    // Migrate
    boolean migrated =
        EclipseStoreToSqliteMigration.migrateIfNeeded(tempStorageDir, sqliteConnection);

    // Verify migration occurred
    assertThat(migrated).isTrue();
    assertThat(countClusters()).isEqualTo(1);

    // Verify empty seedHosts was migrated as empty JSON array
    // (Note: MIGRATION_PLACEHOLDER is only added for CORRUPTED sets with null internals,
    // not for valid empty sets. This test verifies empty sets are handled gracefully.)
    String sql = "SELECT seed_hosts FROM cluster WHERE name = ?";
    try (PreparedStatement stmt = sqliteConnection.prepareStatement(sql)) {
      stmt.setString(1, "placeholder-cluster");
      ResultSet rs = stmt.executeQuery();
      assertThat(rs.next()).isTrue();
      String seedHostsJson = rs.getString("seed_hosts");
      // Empty set becomes "[]" in JSON
      assertThat(seedHostsJson).isEqualTo("[]");
    }
  }

  // Helper methods for counting rows
  private int countClusters() throws SQLException {
    return countRows("cluster");
  }

  private int countRepairUnits() throws SQLException {
    return countRows("repair_unit");
  }

  private int countRepairRuns() throws SQLException {
    return countRows("repair_run");
  }

  private int countRepairSchedules() throws SQLException {
    return countRows("repair_schedule");
  }

  private int countRepairSegments() throws SQLException {
    return countRows("repair_segment");
  }

  private int countRows(String tableName) throws SQLException {
    try (Statement stmt = sqliteConnection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableName)) {
      return rs.next() ? rs.getInt(1) : 0;
    }
  }

  @Test
  public void testMigrateDiagEventSubscriptions() throws Exception {
    // Create cluster + DiagEventSubscription
    MemoryStorageRoot root = new MemoryStorageRoot();

    // Create cluster first (parent entity)
    Cluster cluster =
        Cluster.builder()
            .withName("diag-cluster")
            .withSeedHosts(Sets.newHashSet("127.0.0.1"))
            .withState(Cluster.State.ACTIVE)
            .build();
    root.getClusters().put(cluster.getName(), cluster);

    // Create DiagEventSubscription
    UUID subscriptionId = UUIDs.timeBased();
    io.cassandrareaper.core.DiagEventSubscription subscription =
        new io.cassandrareaper.core.DiagEventSubscription(
            java.util.Optional.of(subscriptionId),
            "diag-cluster",
            java.util.Optional.of("Test subscription"),
            Sets.newHashSet("node1", "node2"),
            Sets.newHashSet("ProgressEvent", "StatusEvent"),
            true, // exportSse
            "/var/log/reaper/diag.log", // exportFileLogger
            "http://localhost:9000/events" // exportHttpEndpoint
            );
    root.getSubscriptionsById().put(subscriptionId, subscription);

    EmbeddedStorageManager storage = EmbeddedStorage.Foundation(tempStorageDir.toPath()).start();
    storage.setRoot(root);
    storage.storeRoot();
    storage.shutdown();

    // Migrate
    boolean migrated =
        EclipseStoreToSqliteMigration.migrateIfNeeded(tempStorageDir, sqliteConnection);

    // Verify
    assertThat(migrated).isTrue();
    assertThat(countClusters()).isEqualTo(1);

    // Verify subscription was migrated
    String sql =
        "SELECT cluster, description, nodes, events, export_sse, "
            + "export_file_logger, export_http_endpoint "
            + "FROM diag_event_subscription WHERE cluster = ?";
    try (PreparedStatement stmt = sqliteConnection.prepareStatement(sql)) {
      stmt.setString(1, "diag-cluster");
      ResultSet rs = stmt.executeQuery();
      assertThat(rs.next()).isTrue();
      assertThat(rs.getString("cluster")).isEqualTo("diag-cluster");
      assertThat(rs.getString("description")).isEqualTo("Test subscription");
      assertThat(rs.getString("nodes")).contains("node1");
      assertThat(rs.getString("nodes")).contains("node2");
      assertThat(rs.getString("events")).contains("ProgressEvent");
      assertThat(rs.getInt("export_sse")).isEqualTo(1);
      assertThat(rs.getString("export_file_logger")).isEqualTo("/var/log/reaper/diag.log");
      assertThat(rs.getString("export_http_endpoint")).isEqualTo("http://localhost:9000/events");
    }
  }

  @Test
  public void testMigrateRepairSegments() throws Exception {
    // Create cluster, repair unit, repair run, and repair segments
    MemoryStorageRoot root = new MemoryStorageRoot();

    // Cluster
    Cluster cluster =
        Cluster.builder()
            .withName("segment-cluster")
            .withSeedHosts(Sets.newHashSet("127.0.0.1"))
            .withState(Cluster.State.ACTIVE)
            .build();
    root.getClusters().put(cluster.getName(), cluster);

    // Repair Unit
    UUID repairUnitId = UUIDs.timeBased();
    RepairUnit repairUnit =
        RepairUnit.builder()
            .clusterName("segment-cluster")
            .keyspaceName("test_ks")
            .columnFamilies(Sets.newHashSet("test_table"))
            .incrementalRepair(false)
            .subrangeIncrementalRepair(false)
            .nodes(Sets.newHashSet())
            .datacenters(Sets.newHashSet())
            .blacklistedTables(Sets.newHashSet())
            .repairThreadCount(1)
            .timeout(30)
            .build(repairUnitId);
    root.getRepairUnits().put(repairUnitId, repairUnit);

    // Repair Run
    UUID repairRunId = UUIDs.timeBased();
    RepairRun repairRun =
        RepairRun.builder("segment-cluster", repairUnitId)
            .intensity(0.9)
            .segmentCount(2)
            .repairParallelism(RepairParallelism.PARALLEL)
            .tables(Sets.newHashSet("test_table"))
            .runState(RepairRun.RunState.RUNNING)
            .startTime(DateTime.now())
            .build(repairRunId);
    root.getRepairRuns().put(repairRunId, repairRun);

    // Repair Segments
    UUID segment1Id = UUIDs.timeBased();
    io.cassandrareaper.service.RingRange range1 =
        new io.cassandrareaper.service.RingRange(BigInteger.ZERO, new BigInteger("100"));
    io.cassandrareaper.core.Segment tokenRange1 =
        io.cassandrareaper.core.Segment.builder()
            .withTokenRange(range1)
            .withReplicas(
                new java.util.HashMap<String, String>() {
                  {
                    put("127.0.0.1", "dc1");
                  }
                })
            .build();

    RepairSegment segment1 =
        RepairSegment.builder(tokenRange1, repairUnitId)
            .withRunId(repairRunId)
            .withState(RepairSegment.State.DONE)
            .withCoordinatorHost("127.0.0.1")
            .withStartTime(DateTime.now().minusHours(1))
            .withEndTime(DateTime.now())
            .withFailCount(0)
            .withId(segment1Id)
            .build();
    root.getRepairSegments().put(segment1Id, segment1);

    UUID segment2Id = UUIDs.timeBased();
    io.cassandrareaper.service.RingRange range2 =
        new io.cassandrareaper.service.RingRange(new BigInteger("101"), new BigInteger("200"));
    io.cassandrareaper.core.Segment tokenRange2 =
        io.cassandrareaper.core.Segment.builder()
            .withTokenRange(range2)
            .withReplicas(
                new java.util.HashMap<String, String>() {
                  {
                    put("127.0.0.1", "dc1");
                  }
                })
            .build();

    RepairSegment segment2 =
        RepairSegment.builder(tokenRange2, repairUnitId)
            .withRunId(repairRunId)
            .withState(RepairSegment.State.RUNNING)
            .withCoordinatorHost("127.0.0.1")
            .withStartTime(DateTime.now())
            .withFailCount(1)
            .withId(segment2Id)
            .build();
    root.getRepairSegments().put(segment2Id, segment2);

    EmbeddedStorageManager storage = EmbeddedStorage.Foundation(tempStorageDir.toPath()).start();
    storage.setRoot(root);
    storage.storeRoot();
    storage.shutdown();

    // Migrate
    boolean migrated =
        EclipseStoreToSqliteMigration.migrateIfNeeded(tempStorageDir, sqliteConnection);

    // Verify
    assertThat(migrated).isTrue();
    assertThat(countClusters()).isEqualTo(1);
    assertThat(countRepairUnits()).isEqualTo(1);
    assertThat(countRepairRuns()).isEqualTo(1);
    assertThat(countRepairSegments()).isEqualTo(2);

    // Verify segment data
    String sql =
        "SELECT state, coordinator_host, start_time, end_time, fail_count "
            + "FROM repair_segment ORDER BY fail_count";
    try (Statement stmt = sqliteConnection.createStatement();
        ResultSet rs = stmt.executeQuery(sql)) {
      // First segment (DONE, failCount=0)
      assertThat(rs.next()).isTrue();
      assertThat(rs.getString("state")).isEqualTo("DONE");
      assertThat(rs.getString("coordinator_host")).isEqualTo("127.0.0.1");
      assertThat(rs.getObject("start_time")).isNotNull();
      assertThat(rs.getObject("end_time")).isNotNull();
      assertThat(rs.getInt("fail_count")).isEqualTo(0);

      // Second segment (RUNNING, failCount=1)
      assertThat(rs.next()).isTrue();
      assertThat(rs.getString("state")).isEqualTo("RUNNING");
      assertThat(rs.getInt("fail_count")).isEqualTo(1);
    }
  }

  @Test
  public void testMigrateClusterWithJmxCredentials() throws Exception {
    // Create cluster with JMX credentials
    MemoryStorageRoot root = new MemoryStorageRoot();

    io.cassandrareaper.core.JmxCredentials jmxCredentials =
        io.cassandrareaper.core.JmxCredentials.builder()
            .withUsername("jmx_user")
            .withPassword("jmx_password_encrypted")
            .build();

    Cluster cluster =
        Cluster.builder()
            .withName("jmx-cluster")
            .withSeedHosts(Sets.newHashSet("192.168.1.1"))
            .withState(Cluster.State.ACTIVE)
            .withJmxCredentials(jmxCredentials)
            .build();
    root.getClusters().put(cluster.getName(), cluster);

    EmbeddedStorageManager storage = EmbeddedStorage.Foundation(tempStorageDir.toPath()).start();
    storage.setRoot(root);
    storage.storeRoot();
    storage.shutdown();

    // Migrate
    boolean migrated =
        EclipseStoreToSqliteMigration.migrateIfNeeded(tempStorageDir, sqliteConnection);

    // Verify
    assertThat(migrated).isTrue();
    assertThat(countClusters()).isEqualTo(1);

    // Verify JMX credentials were migrated
    String sql = "SELECT name, jmx_username, jmx_password FROM cluster WHERE name = ?";
    try (PreparedStatement stmt = sqliteConnection.prepareStatement(sql)) {
      stmt.setString(1, "jmx-cluster");
      ResultSet rs = stmt.executeQuery();
      assertThat(rs.next()).isTrue();
      assertThat(rs.getString("name")).isEqualTo("jmx-cluster");
      assertThat(rs.getString("jmx_username")).isEqualTo("jmx_user");
      assertThat(rs.getString("jmx_password")).isEqualTo("jmx_password_encrypted");
    }
  }

  private void deleteDirectory(File dir) {
    if (dir.isDirectory()) {
      File[] files = dir.listFiles();
      if (files != null) {
        for (File file : files) {
          deleteDirectory(file);
        }
      }
    }
    dir.delete();
  }
}
