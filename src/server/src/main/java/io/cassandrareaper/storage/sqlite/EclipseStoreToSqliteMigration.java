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

import java.io.File;
import java.sql.Connection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DEPRECATED: Utility to migrate data from EclipseStore to SQLite.
 *
 * <p>This class is now deprecated as EclipseStore has been fully replaced by SQLite. The migration
 * functionality has been removed. Users with EclipseStore data should migrate using an earlier
 * version of Reaper before upgrading to this version.
 */
@Deprecated
public final class EclipseStoreToSqliteMigration {

  private static final Logger LOG = LoggerFactory.getLogger(EclipseStoreToSqliteMigration.class);

  private EclipseStoreToSqliteMigration() {
    // Utility class
  }

  /**
   * Check if migration from EclipseStore to SQLite is needed and perform it.
   *
   * <p>This method is now a no-op since EclipseStore has been completely removed. It will log a
   * warning if EclipseStore files are detected but cannot perform the migration.
   *
   * @param storageDir The directory where persistent storage files are located
   * @param sqliteConnection The SQLite database connection
   * @return false always (no migration performed)
   */
  public static boolean migrateIfNeeded(File storageDir, Connection sqliteConnection) {
    // Check if EclipseStore data exists
    File eclipseStoreMarker = new File(storageDir, "channel_0");
    if (eclipseStoreMarker.exists()) {
      LOG.warn(
          "EclipseStore data detected at {} but migration is no longer supported. "
              + "Please use an earlier version of Reaper to migrate your data first.",
          storageDir.getAbsolutePath());
      LOG.warn(
          "If you have already migrated, you can safely delete the old EclipseStore files "
              + "(channel_* and *.bin files) from {}",
          storageDir.getAbsolutePath());
    }
    return false;
  }
}
