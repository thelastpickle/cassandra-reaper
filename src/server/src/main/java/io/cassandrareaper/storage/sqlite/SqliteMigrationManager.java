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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Manages SQLite schema initialization and migrations for the memory storage backend. */
public final class SqliteMigrationManager {

  private static final Logger LOG = LoggerFactory.getLogger(SqliteMigrationManager.class);
  private static final int LATEST_VERSION = 1;

  private SqliteMigrationManager() {
    throw new UnsupportedOperationException("Utility class");
  }

  /**
   * Initialize or migrate the SQLite schema to the latest version.
   *
   * @param connection The SQLite database connection
   * @throws SQLException if schema initialization fails
   */
  public static void initializeSchema(Connection connection) throws SQLException {
    // Enable foreign key support in SQLite
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("PRAGMA foreign_keys = ON");
    }

    int currentVersion = getCurrentVersion(connection);

    if (currentVersion == 0) {
      LOG.info("Initializing new SQLite schema version {}", LATEST_VERSION);
      runMigration(connection, 1);
      LOG.info("SQLite schema initialization complete");
    } else if (currentVersion < LATEST_VERSION) {
      LOG.info("Upgrading SQLite schema from version {} to {}", currentVersion, LATEST_VERSION);
      for (int version = currentVersion + 1; version <= LATEST_VERSION; version++) {
        runMigration(connection, version);
      }
      LOG.info("SQLite schema upgrade complete");
    } else {
      LOG.debug("SQLite schema is up to date (version {})", currentVersion);
    }
  }

  /**
   * Get the current schema version from the database.
   *
   * @param connection The database connection
   * @return The current schema version, or 0 if not yet initialized
   */
  private static int getCurrentVersion(Connection connection) throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      // Check if schema_version table exists
      ResultSet tables = connection.getMetaData().getTables(null, null, "schema_version", null);
      if (!tables.next()) {
        return 0; // Schema not yet initialized
      }

      // Get the latest version
      ResultSet rs = stmt.executeQuery("SELECT MAX(version) as version FROM schema_version");
      if (rs.next()) {
        return rs.getInt("version");
      }
      return 0;
    }
  }

  /**
   * Run a specific migration script.
   *
   * @param connection The database connection
   * @param version The migration version to run
   */
  private static void runMigration(Connection connection, int version) throws SQLException {
    String migrationFile = String.format("/db/sqlite/%03d_init_schema.sql", version);
    LOG.info("Running migration: {}", migrationFile);

    String sql = loadMigrationSql(migrationFile);
    if (sql == null) {
      throw new SQLException("Migration file not found: " + migrationFile);
    }

    // Execute the migration SQL
    try (Statement stmt = connection.createStatement()) {
      // Split by semicolon but keep multi-line statements together
      String[] statements = sql.split(";");
      int statementCount = 0;
      for (String sqlStatement : statements) {
        // Remove comment lines from the statement
        String[] lines = sqlStatement.split("\n");
        StringBuilder cleanedStatement = new StringBuilder();
        for (String line : lines) {
          String trimmedLine = line.trim();
          if (!trimmedLine.isEmpty() && !trimmedLine.startsWith("--")) {
            cleanedStatement.append(line).append("\n");
          }
        }
        String trimmed = cleanedStatement.toString().trim();

        if (!trimmed.isEmpty()) {
          statementCount++;
          LOG.debug(
              "Executing SQL statement: {}", trimmed.substring(0, Math.min(60, trimmed.length())));
          try {
            stmt.execute(trimmed);
          } catch (SQLException e) {
            LOG.error("Failed to execute SQL statement: {}", trimmed, e);
            throw e;
          }
        }
      }
      LOG.debug("Executed {} SQL statements successfully", statementCount);
    }

    // Record the migration version
    try (Statement stmt = connection.createStatement()) {
      stmt.execute(
          String.format(
              "INSERT INTO schema_version (version, applied_at) VALUES (%d, %d)",
              version, System.currentTimeMillis()));
    }

    LOG.info("Migration {} completed successfully", version);
  }

  /**
   * Load migration SQL from resources.
   *
   * @param resourcePath The path to the SQL file in resources
   * @return The SQL content, or null if not found
   */
  private static String loadMigrationSql(String resourcePath) {
    try (InputStream is = SqliteMigrationManager.class.getResourceAsStream(resourcePath)) {
      if (is == null) {
        return null;
      }
      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
        return reader.lines().collect(Collectors.joining("\n"));
      }
    } catch (IOException e) {
      LOG.error("Failed to load migration SQL from {}", resourcePath, e);
      return null;
    }
  }
}
