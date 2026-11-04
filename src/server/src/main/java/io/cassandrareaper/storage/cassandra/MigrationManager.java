/*
 * Copyright 2016-2017 Spotify AB Copyright 2016-2019 The Last Pickle Ltd Copyright 2020-2020
 * DataStax, Inc.
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

package io.cassandrareaper.storage.cassandra;

import io.cassandrareaper.AppContext;
import io.cassandrareaper.ReaperApplicationConfiguration;
import io.cassandrareaper.storage.cassandra.migrations.Migration016;
import io.cassandrareaper.storage.cassandra.migrations.Migration021;
import io.cassandrareaper.storage.cassandra.migrations.Migration024;
import io.cassandrareaper.storage.cassandra.migrations.Migration025;
import io.cassandrareaper.storage.cassandra.migrations.Migration034;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import brave.Tracing;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.google.common.base.Preconditions;
import io.dropwizard.cassandra.CassandraFactory;
import io.dropwizard.core.setup.Environment;
import org.apache.commons.lang3.StringUtils;
import org.cognitor.cassandra.migration.Database;
import org.cognitor.cassandra.migration.MigrationRepository;
import org.cognitor.cassandra.migration.MigrationTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class MigrationManager {

  private static final Logger LOG = LoggerFactory.getLogger(MigrationManager.class);

  /** Maximum time to wait for schema agreement after DDL statements. */
  private static final int SCHEMA_AGREEMENT_WAIT_MS = 60_000;

  /** Poll interval when waiting for schema agreement. */
  private static final int SCHEMA_AGREEMENT_POLL_MS = 1000;

  private MigrationManager() {
    throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
  }

  static void initializeAndUpgradeSchema(
      CassandraFactory cassandraFactory,
      Environment environment,
      ReaperApplicationConfiguration config,
      Version version,
      CassandraStorageFacade.CassandraMode mode) {

    initializeCassandraSchema(cassandraFactory, environment, config, version);
  }

  static void initializeCassandraSchema(
      CassandraFactory cassandraFactory,
      Environment environment,
      ReaperApplicationConfiguration config,
      Version version) {
    Preconditions.checkState(
        0 >= Version.parse("2.1").compareTo(version),
        "All Cassandra nodes in Reaper's backend storage must be running version 2.1+");

    cassandraFactory.setSessionName("migration-" + Uuids.random());
    CqlSession cassandra =
        cassandraFactory.build(
            environment.metrics(),
            environment.lifecycle(),
            environment.healthChecks(),
            Tracing.newBuilder().build());

    try (Database database =
        new Database(cassandra, config.getCassandraFactory().getSessionKeyspaceName())) {

      // The Database constructor creates schema_migration and schema_migration_leader tables
      // (via CREATE TABLE IF NOT EXISTS). When multiple Reaper instances start concurrently,
      // these DDL statements can cause schema disagreements across the cluster, which then
      // break the LWT-based leader election that follows. Wait for schema agreement before
      // proceeding.
      waitForSchemaAgreement(cassandra);

      int currentVersion = database.getVersion();
      Preconditions.checkState(
          currentVersion == 0 || currentVersion >= 15,
          "You need to upgrade from Reaper 1.2.2 at least in order to run this version. "
              + "Please upgrade to 1.2.2, or greater, before performing this upgrade.");

      MigrationRepository migrationRepo = new MigrationRepository("db/cassandra");
      if (currentVersion < migrationRepo.getLatestVersion()) {
        LOG.warn(
            "Starting db migration from {} to {}…",
            currentVersion,
            migrationRepo.getLatestVersion());

        if (15 <= currentVersion) {
          List<String> otherRunningReapers =
              cassandra.execute("SELECT reaper_instance_host FROM running_reapers").all().stream()
                  .map((row) -> row.getString("reaper_instance_host"))
                  .filter(
                      (reaperInstanceHost) ->
                          !AppContext.REAPER_INSTANCE_ADDRESS.equals(reaperInstanceHost))
                  .collect(Collectors.toList());

          LOG.warn(
              "Database migration is happenning with other reaper instances possibly running. Found {}",
              StringUtils.join(otherRunningReapers));
        }

        // We now only support migrations starting at version 15 (Reaper 1.2.2)
        int startVersion = database.getVersion() == 0 ? 15 : database.getVersion();
        migrate(
            startVersion,
            migrationRepo,
            cassandraFactory,
            environment,
            CassandraStorageFacade.CassandraMode.CASSANDRA,
            config.getCassandraFactory().getSessionKeyspaceName());
        // some migration steps depend on the Cassandra version, so must be rerun every startup
        Migration016.migrate(cassandra, config.getCassandraFactory().getSessionKeyspaceName());
        // Switch metrics table to TWCS if possible, this is intentionally executed every startup
        Migration021.migrate(cassandra, config.getCassandraFactory().getSessionKeyspaceName());
        // Switch metrics table to TWCS if possible, this is intentionally executed every startup
        Migration024.migrate(cassandra, config.getCassandraFactory().getSessionKeyspaceName());
        if (database.getVersion() == 25) {
          Migration025.migrate(cassandra, config.getCassandraFactory().getSessionKeyspaceName());
        }
        if (database.getVersion() == 34) {
          Migration034.migrate(cassandra, config.getCassandraFactory().getSessionKeyspaceName());
        }
      } else {
        LOG.info(
            String.format(
                "Keyspace %s already at schema version %d",
                cassandra.getKeyspace(), currentVersion));
      }
    } finally {
      environment.healthChecks().unregister(cassandra.getName());
    }
  }

  /**
   * Waits for all Cassandra nodes to agree on the schema. This is critical after DDL statements
   * (CREATE TABLE IF NOT EXISTS) that the cognitor migration library issues during Database
   * initialization. Without schema agreement, subsequent LWT operations (used for leader election
   * in the migration library) can fail or produce incorrect results.
   *
   * @throws IllegalStateException if interrupted while waiting
   */
  static void waitForSchemaAgreement(CqlSession session) {
    long deadline = System.currentTimeMillis() + SCHEMA_AGREEMENT_WAIT_MS;

    while (System.currentTimeMillis() < deadline) {
      if (session.checkSchemaAgreement()) {
        LOG.info("Schema agreement reached across all nodes.");
        return;
      }
      LOG.debug("Waiting for schema agreement across Cassandra nodes…");
      try {
        Thread.sleep(SCHEMA_AGREEMENT_POLL_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Interrupted while waiting for schema agreement", e);
      }
    }

    LOG.warn(
        "Schema agreement was not reached within {} ms. Proceeding with migration anyway — "
            + "this may cause issues if multiple Reaper instances are starting concurrently.",
        SCHEMA_AGREEMENT_WAIT_MS);
  }

  static void migrate(
      int dbVersion,
      MigrationRepository repository,
      CassandraFactory cassandraFactory,
      Environment environment,
      CassandraStorageFacade.CassandraMode mode,
      String keyspaceName) {
    Preconditions.checkState(dbVersion < repository.getLatestVersion());

    for (int i = dbVersion + 1; i <= repository.getLatestVersion(); ++i) {
      cassandraFactory.setSessionName("migration" + Uuids.random());
      CqlSession cassandra =
          cassandraFactory.build(
              environment.metrics(),
              environment.lifecycle(),
              environment.healthChecks(),
              Tracing.newBuilder().build());
      final int nextVersion = i;
      String migrationRepoPath = "db/cassandra";
      // perform the migrations one at a time, so the MigrationXXX classes can be executed alongside
      // the scripts
      MigrationRepository migrationRepo =
          new MigrationRepository(migrationRepoPath) {
            @Override
            public int getLatestVersion() {
              return nextVersion;
            }

            @Override
            public List getMigrationsSinceVersion(int version) {
              return Collections.singletonList(
                  (Object) super.getMigrationsSinceVersion(nextVersion - 1).get(0));
            }
          };

      try (Database database = new Database(cassandra, cassandraFactory.getSessionKeyspaceName())) {
        // Wait for schema agreement after Database constructor creates/checks tables.
        // This prevents schema disagreements from breaking the LWT leader election
        // that MigrationTask uses internally when withConsensus=true.
        waitForSchemaAgreement(cassandra);

        MigrationTask migration = new MigrationTask(database, migrationRepo, true);
        migration.migrate();
        // after the script execute any MigrationXXX class that exists with the same version number
        Class.forName(
                "io.cassandrareaper.storage.cassandra.Migration"
                    + String.format("%03d", nextVersion))
            .getDeclaredMethod("migrate", CqlSession.class)
            .invoke(null, cassandra, keyspaceName);

        LOG.info("executed Migration" + String.format("%03d", nextVersion));
      } catch (ReflectiveOperationException ignore) {
      } finally {
        environment.healthChecks().unregister(cassandra.getName());
      }
      LOG.info(String.format("Migrated keyspace %s to version %d", keyspaceName, nextVersion));
    }
  }
}
