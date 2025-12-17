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
import io.cassandrareaper.service.RingRange;
import io.cassandrareaper.storage.memory.MemoryStorageRoot;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.cassandra.repair.RepairParallelism;
import org.eclipse.serializer.persistence.types.PersistenceFieldEvaluator;
import org.eclipse.store.storage.embedded.types.EmbeddedStorage;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Real-world migration test that simulates the actual user upgrade path: 1. Create EclipseStore
 * data with ImmutableSet (Reaper 4.0) 2. Run migration (Reaper 4.1.0) 3. Verify all data migrated
 * correctly (especially seed hosts) 4. Test idempotency (retry migration)
 */
public class RealWorldMigrationTest {

  // Same evaluator that production Reaper 4.0 would have used
  private static final PersistenceFieldEvaluator TRANSIENT_FIELD_EVALUATOR =
      (clazz, field) -> !field.getName().startsWith("_");

  private File tempStorageDir;
  private Connection sqliteConnection;

  @Before
  public void setUp() throws Exception {
    // Create temp directory for EclipseStore
    tempStorageDir = Files.createTempDirectory("reaper-realworld-test-").toFile();

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
      deleteDirectory(tempStorageDir.toPath());
    }
  }

  @Test
  public void testRealWorldMigrationWithImmutableSet() throws Exception {
    System.out.println("\n========================================");
    System.out.println("REAL-WORLD MIGRATION TEST");
    System.out.println("Testing ImmutableSet seed hosts (the problematic case!)");
    System.out.println("========================================\n");

    // PHASE 1: Create EclipseStore data with ImmutableSet (simulating Reaper 4.0)
    System.out.println("--- PHASE 1: Creating EclipseStore Data (Reaper 4.0) ---");
    createEclipseStoreDataWithImmutableSet();
    System.out.println("✓ EclipseStore data created with ImmutableSet");

    // PHASE 2: Run migration (Reaper 4.1.0)
    System.out.println("\n--- PHASE 2: Running Migration (Reaper 4.1.0) ---");
    boolean migrated =
        EclipseStoreToSqliteMigration.migrateIfNeeded(tempStorageDir, sqliteConnection);
    assertThat(migrated).isTrue();
    System.out.println("✓ Migration completed");

    // PHASE 3: Verify migrated data
    System.out.println("\n--- PHASE 3: Verifying Migrated Data ---");
    verifyMigratedData();
    System.out.println("✓ All data verified");

    // PHASE 4: Test idempotency (retry migration)
    System.out.println("\n--- PHASE 4: Testing Idempotency (Retry) ---");
    restoreEclipseStoreForRetry();
    boolean migrated2 =
        EclipseStoreToSqliteMigration.migrateIfNeeded(tempStorageDir, sqliteConnection);
    assertThat(migrated2).isTrue();
    System.out.println("✓ Retry migration succeeded (no PRIMARY KEY errors!)");

    // Verify data again after retry
    verifyMigratedData();
    System.out.println("✓ Data still correct after retry");

    System.out.println("\n========================================");
    System.out.println("✅ REAL-WORLD MIGRATION TEST PASSED!");
    System.out.println("========================================");
  }

  private void createEclipseStoreDataWithImmutableSet() throws Exception {
    System.out.println("Creating EclipseStore data in: " + tempStorageDir);

    MemoryStorageRoot root = new MemoryStorageRoot();

    // Create cluster with ImmutableSet seed hosts (the problematic case!)
    System.out.println("  Creating cluster with ImmutableSet seed hosts...");
    Set<String> seedHosts =
        ImmutableSet.of(
            "cassandra-node1.example.com",
            "cassandra-node2.example.com",
            "cassandra-node3.example.com");

    Cluster cluster =
        Cluster.builder()
            .withName("test-cluster")
            .withSeedHosts(seedHosts)
            .withPartitioner("org.apache.cassandra.dht.Murmur3Partitioner")
            .withState(Cluster.State.ACTIVE)
            .build();
    root.getClusters().put(cluster.getName(), cluster);
    System.out.println("    Seed hosts (ImmutableSet): " + seedHosts);

    // Create repair unit
    System.out.println("  Creating repair unit...");
    UUID repairUnitId = UUIDs.timeBased();
    RepairUnit repairUnit =
        RepairUnit.builder()
            .clusterName("test-cluster")
            .keyspaceName("test_keyspace")
            .columnFamilies(Sets.newHashSet("test_table1", "test_table2"))
            .incrementalRepair(false)
            .subrangeIncrementalRepair(false)
            .nodes(Sets.newHashSet())
            .datacenters(Sets.newHashSet())
            .blacklistedTables(Sets.newHashSet())
            .repairThreadCount(4)
            .timeout(60)
            .build(repairUnitId);
    root.getRepairUnits().put(repairUnitId, repairUnit);
    root.getRepairUnitsByKey().put(repairUnit.with(), repairUnit);

    // Create repair schedule
    System.out.println("  Creating repair schedule...");
    UUID scheduleId = UUIDs.timeBased();
    RepairSchedule schedule =
        RepairSchedule.builder(repairUnitId)
            .daysBetween(7)
            .owner("admin")
            .repairParallelism(RepairParallelism.DATACENTER_AWARE)
            .intensity(0.9)
            .segmentCountPerNode(64)
            .state(RepairSchedule.State.ACTIVE)
            .creationTime(DateTime.now())
            .nextActivation(DateTime.now().plusDays(1))
            .build(scheduleId);
    root.getRepairSchedules().put(scheduleId, schedule);

    // Create repair run
    System.out.println("  Creating repair run...");
    UUID repairRunId = UUIDs.timeBased();
    RepairRun repairRun =
        RepairRun.builder("test-cluster", repairUnitId)
            .intensity(0.9)
            .segmentCount(10)
            .repairParallelism(RepairParallelism.DATACENTER_AWARE)
            .tables(Sets.newHashSet("test_table1", "test_table2"))
            .runState(RepairRun.RunState.DONE)
            .startTime(DateTime.now().minusHours(2))
            .endTime(DateTime.now().minusHours(1))
            .cause("Scheduled repair")
            .owner("admin")
            .build(repairRunId);
    root.getRepairRuns().put(repairRunId, repairRun);

    // Create repair segments
    System.out.println("  Creating repair segments...");
    for (int i = 0; i < 5; i++) {
      UUID segmentId = UUIDs.timeBased();
      io.cassandrareaper.core.Segment tokenRange =
          io.cassandrareaper.core.Segment.builder()
              .withTokenRange(
                  new RingRange(BigInteger.valueOf(i * 1000L), BigInteger.valueOf((i + 1) * 1000L)))
              .withReplicas(new java.util.HashMap<>())
              .build();

      RepairSegment segment =
          RepairSegment.builder(tokenRange, repairUnitId)
              .withRunId(repairRunId)
              .withState(RepairSegment.State.DONE)
              .withStartTime(DateTime.now().minusHours(2))
              .withEndTime(DateTime.now().minusHours(1))
              .withCoordinatorHost("cassandra-node1.example.com")
              .withId(segmentId)
              .build();
      root.getRepairSegments().put(segmentId, segment);
    }

    System.out.println("  Data summary:");
    System.out.println("    Clusters: " + root.getClusters().size());
    System.out.println("    Repair Units: " + root.getRepairUnits().size());
    System.out.println("    Repair Schedules: " + root.getRepairSchedules().size());
    System.out.println("    Repair Runs: " + root.getRepairRuns().size());
    System.out.println("    Repair Segments: " + root.getRepairSegments().size());

    // Write to EclipseStore WITH TRANSIENT_FIELD_EVALUATOR
    // This simulates how production Reaper 4.0 would have written data
    System.out.println("  Writing to EclipseStore with TRANSIENT_FIELD_EVALUATOR...");
    EmbeddedStorageManager storage =
        EmbeddedStorage.Foundation(tempStorageDir.toPath())
            .onConnectionFoundation(c -> c.setFieldEvaluatorPersistable(TRANSIENT_FIELD_EVALUATOR))
            .start();
    storage.setRoot(root);
    storage.storeRoot();
    storage.shutdown();

    // Verify channel files were created
    File[] channelFiles = tempStorageDir.listFiles((dir, name) -> name.startsWith("channel_"));
    assertThat(channelFiles).isNotNull();
    assertThat(channelFiles.length).isGreaterThan(0);
    System.out.println("  ✓ EclipseStore channel files created: " + channelFiles.length);
  }

  private void verifyMigratedData() throws SQLException {
    // Verify clusters
    System.out.println("  Checking clusters...");
    String sql = "SELECT name, seed_hosts, state FROM cluster WHERE name = ?";
    try (PreparedStatement stmt = sqliteConnection.prepareStatement(sql)) {
      stmt.setString(1, "test-cluster");
      ResultSet rs = stmt.executeQuery();
      assertThat(rs.next()).isTrue();
      assertThat(rs.getString("name")).isEqualTo("test-cluster");
      assertThat(rs.getString("state")).isEqualTo("ACTIVE");

      // CRITICAL: Check for MIGRATION_PLACEHOLDER
      String seedHostsJson = rs.getString("seed_hosts");
      System.out.println("    Seed hosts JSON: " + seedHostsJson);
      assertThat(seedHostsJson).doesNotContain("MIGRATION_PLACEHOLDER");
      assertThat(seedHostsJson).contains("cassandra-node1.example.com");
      assertThat(seedHostsJson).contains("cassandra-node2.example.com");
      assertThat(seedHostsJson).contains("cassandra-node3.example.com");
      System.out.println("    ✓ Seed hosts valid (no MIGRATION_PLACEHOLDER)");
    }

    // Verify repair units
    System.out.println("  Checking repair units...");
    String unitSql = "SELECT cluster_name, keyspace_name FROM repair_unit WHERE cluster_name = ?";
    try (PreparedStatement stmt = sqliteConnection.prepareStatement(unitSql)) {
      stmt.setString(1, "test-cluster");
      ResultSet rs = stmt.executeQuery();
      assertThat(rs.next()).isTrue();
      assertThat(rs.getString("keyspace_name")).isEqualTo("test_keyspace");
      System.out.println("    ✓ Repair unit verified");
    }

    // Verify repair schedules
    System.out.println("  Checking repair schedules...");
    String scheduleSql = "SELECT owner, state, days_between FROM repair_schedule";
    try (PreparedStatement stmt = sqliteConnection.prepareStatement(scheduleSql)) {
      ResultSet rs = stmt.executeQuery();
      assertThat(rs.next()).isTrue();
      assertThat(rs.getString("owner")).isEqualTo("admin");
      assertThat(rs.getInt("days_between")).isEqualTo(7);
      System.out.println("    ✓ Repair schedule verified");
    }

    // Verify repair runs
    System.out.println("  Checking repair runs...");
    String runSql = "SELECT cluster_name, state FROM repair_run WHERE cluster_name = ?";
    try (PreparedStatement stmt = sqliteConnection.prepareStatement(runSql)) {
      stmt.setString(1, "test-cluster");
      ResultSet rs = stmt.executeQuery();
      assertThat(rs.next()).isTrue();
      assertThat(rs.getString("state")).isEqualTo("DONE");
      System.out.println("    ✓ Repair run verified");
    }

    // Verify repair segments
    System.out.println("  Checking repair segments...");
    String segmentSql = "SELECT COUNT(*) as count FROM repair_segment";
    try (PreparedStatement stmt = sqliteConnection.prepareStatement(segmentSql)) {
      ResultSet rs = stmt.executeQuery();
      assertThat(rs.next()).isTrue();
      assertThat(rs.getInt("count")).isEqualTo(5);
      System.out.println("    ✓ 5 repair segments verified");
    }

    // Verify backup was created
    File backupDir = new File(tempStorageDir, ".eclipsestore.backup");
    assertThat(backupDir.exists()).isTrue();
    System.out.println("    ✓ Backup directory verified");
  }

  private void restoreEclipseStoreForRetry() throws IOException {
    System.out.println("  Restoring EclipseStore files from backup...");
    File backupDir = new File(tempStorageDir, ".eclipsestore.backup");
    assertThat(backupDir.exists()).isTrue();

    // Copy backup files back
    File[] backupFiles = backupDir.listFiles();
    assertThat(backupFiles).isNotNull();
    for (File file : backupFiles) {
      if (file.isFile()) {
        Files.copy(
            file.toPath(),
            new File(tempStorageDir, file.getName()).toPath(),
            java.nio.file.StandardCopyOption.REPLACE_EXISTING);
      } else if (file.isDirectory()) {
        copyDirectory(file, new File(tempStorageDir, file.getName()));
      }
    }

    // Remove backup directory to simulate fresh retry
    deleteDirectory(backupDir.toPath());
    System.out.println("  ✓ EclipseStore files restored for retry");
  }

  private void copyDirectory(File source, File target) throws IOException {
    if (!target.exists()) {
      target.mkdirs();
    }
    File[] files = source.listFiles();
    if (files != null) {
      for (File file : files) {
        File newFile = new File(target, file.getName());
        if (file.isDirectory()) {
          copyDirectory(file, newFile);
        } else {
          Files.copy(
              file.toPath(), newFile.toPath(), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        }
      }
    }
  }

  private void deleteDirectory(Path path) throws IOException {
    if (Files.exists(path)) {
      Files.walk(path)
          .sorted(Comparator.reverseOrder())
          .forEach(
              p -> {
                try {
                  Files.delete(p);
                } catch (IOException e) {
                  // Ignore cleanup errors
                }
              });
    }
  }
}
