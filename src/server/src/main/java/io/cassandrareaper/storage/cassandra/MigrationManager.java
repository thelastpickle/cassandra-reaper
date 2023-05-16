/*
 * Copyright 2016-2017 Spotify AB
 * Copyright 2016-2019 The Last Pickle Ltd
 * Copyright 2020-2020 DataStax, Inc.
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

package io.cassandrareaper.storage.cassandra;

import io.cassandrareaper.AppContext;
import io.cassandrareaper.ReaperApplicationConfiguration;
import io.cassandrareaper.storage.cassandra.migrations.Migration016;
import io.cassandrareaper.storage.cassandra.migrations.Migration021;
import io.cassandrareaper.storage.cassandra.migrations.Migration024;
import io.cassandrareaper.storage.cassandra.migrations.Migration025;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.VersionNumber;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.cognitor.cassandra.migration.Database;
import org.cognitor.cassandra.migration.MigrationRepository;
import org.cognitor.cassandra.migration.MigrationTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class MigrationManager {

  private static final Logger LOG = LoggerFactory.getLogger(MigrationManager.class);

  private MigrationManager() {
    throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
  }

  static void initializeAndUpgradeSchema(
      Cluster cassandra,
      Session session,
      ReaperApplicationConfiguration config,
      VersionNumber version,
      CassandraStorage.CassandraMode mode) {

    if (mode.equals(CassandraStorage.CassandraMode.CASSANDRA)) {
      initializeCassandraSchema(cassandra, session, config, version);
    } else if (mode.equals(CassandraStorage.CassandraMode.ASTRA)) {
      initializeAstraSchema(cassandra, session, config, version);
    }
  }

  static void initializeCassandraSchema(
      Cluster cassandra,
      Session session,
      ReaperApplicationConfiguration config,
      VersionNumber version
  ) {
    Preconditions.checkState(
        0 >= VersionNumber.parse("2.1").compareTo(version),
        "All Cassandra nodes in Reaper's backend storage must be running version 2.1+");

    try (Database database = new Database(cassandra, config.getCassandraFactory().getKeyspace())) {

      int currentVersion = database.getVersion();
      Preconditions.checkState(
          currentVersion == 0 || currentVersion >= 15,
          "You need to upgrade from Reaper 1.2.2 at least in order to run this version. "
              + "Please upgrade to 1.2.2, or greater, before performing this upgrade.");

      MigrationRepository migrationRepo = new MigrationRepository("db/cassandra");
      if (currentVersion < migrationRepo.getLatestVersion()) {
        LOG.warn("Starting db migration from {} to {}…", currentVersion, migrationRepo.getLatestVersion());

        if (15 <= currentVersion) {
          List<String> otherRunningReapers = session.execute("SELECT reaper_instance_host FROM running_reapers").all()
              .stream()
              .map((row) -> row.getString("reaper_instance_host"))
              .filter((reaperInstanceHost) -> !AppContext.REAPER_INSTANCE_ADDRESS.equals(reaperInstanceHost))
              .collect(Collectors.toList());

          LOG.warn(
              "Database migration is happenning with other reaper instances possibly running. Found {}",
              StringUtils.join(otherRunningReapers));
        }

        // We now only support migrations starting at version 15 (Reaper 1.2.2)
        int startVersion = database.getVersion() == 0 ? 15 : database.getVersion();
        migrate(startVersion, migrationRepo, session, CassandraStorage.CassandraMode.CASSANDRA);
        // some migration steps depend on the Cassandra version, so must be rerun every startup
        Migration016.migrate(session, config.getCassandraFactory().getKeyspace());
        // Switch metrics table to TWCS if possible, this is intentionally executed every startup
        Migration021.migrate(session, config.getCassandraFactory().getKeyspace());
        // Switch metrics table to TWCS if possible, this is intentionally executed every startup
        Migration024.migrate(session, config.getCassandraFactory().getKeyspace());
        if (database.getVersion() == 25) {
          Migration025.migrate(session, config.getCassandraFactory().getKeyspace());
        }
      } else {
        LOG.info(
            String.format("Keyspace %s already at schema version %d", session.getLoggedKeyspace(), currentVersion));
      }
    }
  }

  static void initializeAstraSchema(
      Cluster cassandra,
      Session session,
      ReaperApplicationConfiguration config,
      VersionNumber version
  ) {
    Preconditions.checkState(
        0 >= VersionNumber.parse("2.1").compareTo(version),
        "All Cassandra nodes in Reaper's backend storage must be running version 2.1+");

    try (Database database = new Database(cassandra, config.getCassandraFactory().getKeyspace())) {

      int currentVersion = database.getVersion();

      MigrationRepository migrationRepo = new MigrationRepository("db/astra");
      if (currentVersion < migrationRepo.getLatestVersion()) {
        LOG.warn("Starting db migration from {} to {}…", currentVersion, migrationRepo.getLatestVersion());

        int startVersion = database.getVersion();
        migrate(startVersion, migrationRepo, session, CassandraStorage.CassandraMode.ASTRA);
      } else {
        LOG.info(
            String.format("Keyspace %s already at schema version %d", session.getLoggedKeyspace(), currentVersion));
      }
    }
  }

  static void migrate(
      int dbVersion,
      MigrationRepository repository,
      Session session,
      CassandraStorage.CassandraMode mode) {
    Preconditions.checkState(dbVersion < repository.getLatestVersion());

    for (int i = dbVersion + 1; i <= repository.getLatestVersion(); ++i) {
      final int nextVersion = i;
      String migrationRepoPath = mode.equals(CassandraStorage.CassandraMode.CASSANDRA) ? "db/cassandra" : "db/astra";
      // perform the migrations one at a time, so the MigrationXXX classes can be executed alongside the scripts
      MigrationRepository migrationRepo = new MigrationRepository(migrationRepoPath) {
        @Override
        public int getLatestVersion() {
          return nextVersion;
        }

        @Override
        public List getMigrationsSinceVersion(int version) {
          return Collections.singletonList((Object) super.getMigrationsSinceVersion(nextVersion - 1).get(0));
        }
      };

      try (Database database = new Database(session.getCluster(), session.getLoggedKeyspace())) {
        MigrationTask migration = new MigrationTask(database, migrationRepo, true);
        migration.migrate();
        // after the script execute any MigrationXXX class that exists with the same version number
        Class.forName("io.cassandrareaper.storage.cassandra.Migration" + String.format("%03d", nextVersion))
            .getDeclaredMethod("migrate", Session.class)
            .invoke(null, session);

        LOG.info("executed Migration" + String.format("%03d", nextVersion));
      } catch (ReflectiveOperationException ignore) {
      }
      LOG.info(String.format("Migrated keyspace %s to version %d", session.getLoggedKeyspace(), nextVersion));
    }
  }
}