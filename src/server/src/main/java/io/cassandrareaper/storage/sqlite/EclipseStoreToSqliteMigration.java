/*
 * Copyright 2025-2025 DataStax, Inc.
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
import io.cassandrareaper.core.DiagEventSubscription;
import io.cassandrareaper.core.RepairRun;
import io.cassandrareaper.core.RepairSchedule;
import io.cassandrareaper.core.RepairSegment;
import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.storage.memory.MemoryStorageRoot;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.eclipse.store.storage.embedded.types.EmbeddedStorage;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility to migrate data from EclipseStore to SQLite for the memory storage backend. */
public final class EclipseStoreToSqliteMigration {

  private static final Logger LOG = LoggerFactory.getLogger(EclipseStoreToSqliteMigration.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String MIGRATION_BANNER = "========================================";

  private EclipseStoreToSqliteMigration() {
    throw new UnsupportedOperationException("Utility class");
  }

  /**
   * Check if EclipseStore data exists and migrate it to SQLite if needed.
   *
   * @param storageDir The storage directory that may contain EclipseStore data
   * @param sqliteConnection The SQLite connection to migrate data into
   * @return true if migration was performed, false if no migration was needed
   */
  public static boolean migrateIfNeeded(File storageDir, Connection sqliteConnection) {
    if (!hasEclipseStoreData(storageDir)) {
      return false; // No migration needed
    }

    LOG.info(MIGRATION_BANNER);
    LOG.info("EclipseStore data detected in: {}", storageDir.getAbsolutePath());
    LOG.info("Starting automatic migration to SQLite...");
    LOG.info(MIGRATION_BANNER);

    try {
      migrate(storageDir, sqliteConnection);
      backupEclipseStoreFiles(storageDir);
      LOG.info(MIGRATION_BANNER);
      LOG.info("Migration completed successfully!");
      LOG.info("EclipseStore files backed up");
      LOG.info(MIGRATION_BANNER);
      return true;
    } catch (SQLException | IOException e) {
      LOG.error("Migration failed", e);
      throw new RuntimeException("Failed to migrate EclipseStore data to SQLite", e);
    }
  }

  /**
   * Check if the directory contains EclipseStore data files.
   *
   * @param storageDir The directory to check
   * @return true if EclipseStore data exists
   */
  private static boolean hasEclipseStoreData(File storageDir) {
    if (!storageDir.exists() || !storageDir.isDirectory()) {
      return false;
    }

    // Check for EclipseStore marker files (channel_* files)
    File[] files = storageDir.listFiles((dir, name) -> name.startsWith("channel_"));
    return files != null && files.length > 0;
  }

  /**
   * Perform the actual migration from EclipseStore to SQLite.
   *
   * @param storageDir The directory containing EclipseStore data
   * @param sqliteConn The SQLite connection
   */
  private static void migrate(File storageDir, Connection sqliteConn) throws SQLException {
    EmbeddedStorageManager eclipseStore = null;

    try {
      // Load EclipseStore data
      LOG.info("Loading EclipseStore data from: {}", storageDir);
      eclipseStore = EmbeddedStorage.Foundation(storageDir.toPath()).createEmbeddedStorageManager();
      eclipseStore.start();

      Object root = eclipseStore.root();
      Preconditions.checkState(
          root instanceof MemoryStorageRoot,
          "Unexpected root type: " + (root == null ? "null" : root.getClass()));

      MemoryStorageRoot oldRoot = (MemoryStorageRoot) root;

      // Migrate in a transaction
      sqliteConn.setAutoCommit(false);
      try {
        migrateClusters(oldRoot.getClusters().values(), sqliteConn);
        migrateRepairUnits(oldRoot.getRepairUnits().values(), sqliteConn);
        migrateRepairSchedules(oldRoot.getRepairSchedules().values(), sqliteConn);
        migrateRepairRuns(oldRoot.getRepairRuns().values(), sqliteConn);
        migrateRepairSegments(oldRoot.getRepairSegments().values(), sqliteConn);
        migrateDiagEventSubscriptions(oldRoot.getSubscriptionsById().values(), sqliteConn);

        sqliteConn.commit();
        LOG.info("All data migrated successfully");
      } catch (SQLException e) {
        sqliteConn.rollback();
        throw e;
      } finally {
        sqliteConn.setAutoCommit(true);
      }

    } finally {
      if (eclipseStore != null) {
        eclipseStore.shutdown();
      }
    }
  }

  private static void migrateClusters(Collection<Cluster> clusters, Connection conn)
      throws SQLException {
    if (clusters.isEmpty()) {
      LOG.info("No clusters to migrate");
      return;
    }

    LOG.info("Migrating {} clusters...", clusters.size());
    String sql =
        "INSERT INTO cluster (name, partitioner, seed_hosts, properties, state, "
            + "last_contact, namespace) VALUES (?, ?, ?, ?, ?, ?, ?)";

    try (PreparedStatement stmt = conn.prepareStatement(sql)) {
      for (Cluster cluster : clusters) {
        stmt.setString(1, cluster.getName());
        stmt.setString(2, cluster.getPartitioner().orElse(null));
        stmt.setString(3, toJson(cluster.getSeedHosts()));
        stmt.setString(4, toJson(cluster.getProperties()));
        stmt.setString(5, cluster.getState().name());
        // LocalDate conversion: convert to epoch day * milliseconds per day
        stmt.setLong(
            6,
            cluster.getLastContact() != null
                ? cluster.getLastContact().toEpochDay() * 86400000L
                : 0);
        stmt.setString(7, null); // namespace not used
        stmt.executeUpdate();
      }
    }
    LOG.info("Migrated {} clusters", clusters.size());
  }

  private static void migrateRepairUnits(Collection<RepairUnit> units, Connection conn)
      throws SQLException {
    if (units.isEmpty()) {
      LOG.info("No repair units to migrate");
      return;
    }

    LOG.info("Migrating {} repair units...", units.size());
    String sql =
        "INSERT INTO repair_unit (id, cluster_name, keyspace_name, column_families, "
            + "incremental_repair, subrange_incremental, nodes, datacenters, blacklisted_tables, "
            + "repair_thread_count, timeout) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    try (PreparedStatement stmt = conn.prepareStatement(sql)) {
      for (RepairUnit unit : units) {
        stmt.setBytes(1, UuidUtil.toBytes(unit.getId()));
        stmt.setString(2, unit.getClusterName());
        stmt.setString(3, unit.getKeyspaceName());
        stmt.setString(4, toJson(unit.getColumnFamilies()));
        stmt.setInt(5, unit.getIncrementalRepair() ? 1 : 0);
        stmt.setInt(6, unit.getSubrangeIncrementalRepair() ? 1 : 0);
        stmt.setString(7, toJson(unit.getNodes()));
        stmt.setString(8, toJson(unit.getDatacenters()));
        stmt.setString(9, toJson(unit.getBlacklistedTables()));
        stmt.setInt(10, unit.getRepairThreadCount());
        stmt.setInt(11, unit.getTimeout());
        stmt.executeUpdate();
      }
    }
    LOG.info("Migrated {} repair units", units.size());
  }

  private static void migrateRepairSchedules(Collection<RepairSchedule> schedules, Connection conn)
      throws SQLException {
    if (schedules.isEmpty()) {
      LOG.info("No repair schedules to migrate");
      return;
    }

    LOG.info("Migrating {} repair schedules...", schedules.size());
    String sql =
        "INSERT INTO repair_schedule (id, repair_unit_id, owner, state, days_between, "
            + "next_activation, creation_time, pause_time, intensity, segment_count, "
            + "segment_count_per_node, repair_parallelism, adaptive, percent_unrepaired_threshold, "
            + "run_history, last_run) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    try (PreparedStatement stmt = conn.prepareStatement(sql)) {
      for (RepairSchedule schedule : schedules) {
        stmt.setBytes(1, UuidUtil.toBytes(schedule.getId()));
        stmt.setBytes(2, UuidUtil.toBytes(schedule.getRepairUnitId()));
        stmt.setString(3, schedule.getOwner());
        stmt.setString(4, schedule.getState().name());
        stmt.setInt(5, schedule.getDaysBetween() != null ? schedule.getDaysBetween() : 0);
        stmt.setLong(
            6, schedule.getNextActivation() != null ? schedule.getNextActivation().getMillis() : 0);
        stmt.setLong(
            7, schedule.getCreationTime() != null ? schedule.getCreationTime().getMillis() : 0);
        stmt.setLong(8, schedule.getPauseTime() != null ? schedule.getPauseTime().getMillis() : 0);
        stmt.setDouble(9, schedule.getIntensity());
        stmt.setInt(10, 0); // segment_count - not used in current model
        stmt.setInt(
            11, schedule.getSegmentCountPerNode() != null ? schedule.getSegmentCountPerNode() : 0);
        stmt.setString(12, schedule.getRepairParallelism().name());
        stmt.setInt(13, schedule.getAdaptive() ? 1 : 0);
        stmt.setInt(14, schedule.getPercentUnrepairedThreshold());
        stmt.setString(15, toJson(schedule.getRunHistory()));
        stmt.setBytes(
            16, schedule.getLastRun() != null ? UuidUtil.toBytes(schedule.getLastRun()) : null);
        stmt.executeUpdate();
      }
    }
    LOG.info("Migrated {} repair schedules", schedules.size());
  }

  private static void migrateRepairRuns(Collection<RepairRun> runs, Connection conn)
      throws SQLException {
    if (runs.isEmpty()) {
      LOG.info("No repair runs to migrate");
      return;
    }

    LOG.info("Migrating {} repair runs...", runs.size());
    String sql =
        "INSERT INTO repair_run (id, cluster_name, repair_unit_id, cause, owner, state, "
            + "creation_time, start_time, end_time, pause_time, intensity, last_event, "
            + "segment_count, repair_parallelism, tables, adaptive_schedule) "
            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    try (PreparedStatement stmt = conn.prepareStatement(sql)) {
      for (RepairRun run : runs) {
        stmt.setBytes(1, UuidUtil.toBytes(run.getId()));
        stmt.setString(2, run.getClusterName());
        stmt.setBytes(3, UuidUtil.toBytes(run.getRepairUnitId()));
        stmt.setString(4, run.getCause());
        stmt.setString(5, run.getOwner());
        stmt.setString(6, run.getRunState().name());
        stmt.setLong(7, run.getCreationTime() != null ? run.getCreationTime().getMillis() : 0);
        stmt.setLong(8, run.getStartTime() != null ? run.getStartTime().getMillis() : 0);
        stmt.setLong(9, run.getEndTime() != null ? run.getEndTime().getMillis() : 0);
        stmt.setLong(10, run.getPauseTime() != null ? run.getPauseTime().getMillis() : 0);
        stmt.setDouble(11, run.getIntensity());
        stmt.setString(12, run.getLastEvent());
        stmt.setInt(13, run.getSegmentCount());
        stmt.setString(14, run.getRepairParallelism().name());
        stmt.setString(15, toJson(run.getTables()));
        stmt.setInt(16, run.getAdaptiveSchedule() ? 1 : 0);
        stmt.executeUpdate();
      }
    }
    LOG.info("Migrated {} repair runs", runs.size());
  }

  private static void migrateRepairSegments(Collection<RepairSegment> segments, Connection conn)
      throws SQLException {
    if (segments.isEmpty()) {
      LOG.info("No repair segments to migrate");
      return;
    }

    LOG.info("Migrating {} repair segments (this may take a while)...", segments.size());
    String sql =
        "INSERT INTO repair_segment (id, run_id, repair_unit_id, start_token, end_token, "
            + "token_ranges, state, coordinator_host, start_time, end_time, fail_count, replicas, "
            + "host_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    try (PreparedStatement stmt = conn.prepareStatement(sql)) {
      int count = 0;
      for (RepairSegment segment : segments) {
        stmt.setBytes(1, UuidUtil.toBytes(segment.getId()));
        stmt.setBytes(2, UuidUtil.toBytes(segment.getRunId()));
        stmt.setBytes(3, UuidUtil.toBytes(segment.getRepairUnitId()));
        stmt.setString(4, segment.getTokenRange().getBaseRange().getStart().toString());
        stmt.setString(5, segment.getTokenRange().getBaseRange().getEnd().toString());
        stmt.setString(6, toJson(segment.getTokenRange()));
        stmt.setString(7, segment.getState().name());
        stmt.setString(8, segment.getCoordinatorHost());
        stmt.setLong(9, segment.getStartTime() != null ? segment.getStartTime().getMillis() : 0);
        stmt.setLong(10, segment.getEndTime() != null ? segment.getEndTime().getMillis() : 0);
        stmt.setInt(11, segment.getFailCount());
        stmt.setString(12, toJson(segment.getReplicas()));
        stmt.setBytes(
            13, segment.getHostID() != null ? UuidUtil.toBytes(segment.getHostID()) : null);
        stmt.executeUpdate();

        count++;
        if (count % 1000 == 0) {
          LOG.info("Migrated {}/{} repair segments...", count, segments.size());
        }
      }
    }
    LOG.info("Migrated {} repair segments", segments.size());
  }

  private static void migrateDiagEventSubscriptions(
      Collection<DiagEventSubscription> subscriptions, Connection conn) throws SQLException {
    if (subscriptions.isEmpty()) {
      LOG.info("No diagnostic event subscriptions to migrate");
      return;
    }

    LOG.info("Migrating {} diagnostic event subscriptions...", subscriptions.size());
    String sql =
        "INSERT INTO diag_event_subscription (id, cluster, description, nodes, events, "
            + "export_sse, export_file_logger, export_http_endpoint) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

    try (PreparedStatement stmt = conn.prepareStatement(sql)) {
      for (DiagEventSubscription sub : subscriptions) {
        if (sub.getId().isPresent()) {
          stmt.setBytes(1, UuidUtil.toBytes(sub.getId().get()));
          stmt.setString(2, sub.getCluster());
          stmt.setString(3, sub.getDescription());
          stmt.setString(4, toJson(sub.getNodes()));
          stmt.setString(5, toJson(sub.getEvents()));
          stmt.setInt(6, sub.getExportSse() ? 1 : 0);
          stmt.setString(7, sub.getExportFileLogger());
          stmt.setString(8, sub.getExportHttpEndpoint());
          stmt.executeUpdate();
        }
      }
    }
    LOG.info("Migrated {} diagnostic event subscriptions", subscriptions.size());
  }

  /**
   * Backup EclipseStore files to a .backup subdirectory.
   *
   * @param storageDir The directory containing EclipseStore files
   */
  private static void backupEclipseStoreFiles(File storageDir) throws IOException {
    File backupDir = new File(storageDir, ".eclipsestore.backup");
    backupDir.mkdirs();

    LOG.info("Backing up EclipseStore files to: {}", backupDir.getAbsolutePath());

    File[] files =
        storageDir.listFiles(
            (dir, name) -> name.startsWith("channel_") || name.startsWith("transactions_"));

    if (files != null) {
      for (File file : files) {
        Path source = file.toPath();
        Path target = backupDir.toPath().resolve(file.getName());
        Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
        LOG.debug("Backed up: {}", file.getName());
      }
    }

    LOG.info("EclipseStore files backed up successfully");
  }

  /**
   * Convert an object to JSON string for storage.
   *
   * @param obj The object to convert
   * @return JSON string representation
   */
  private static String toJson(Object obj) {
    if (obj == null) {
      return null;
    }
    try {
      return OBJECT_MAPPER.writeValueAsString(obj);
    } catch (JsonProcessingException e) {
      LOG.warn("Failed to serialize object to JSON: {}", obj, e);
      return obj.toString();
    }
  }
}
