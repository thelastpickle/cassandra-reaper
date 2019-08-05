/*
 * Copyright 2019-2019 The Last Pickle Ltd
 *
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

import io.cassandrareaper.ReaperApplicationConfiguration;

import java.util.UUID;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.VersionNumber;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SchemaMigrationLock implements AutoCloseable {

  private static final UUID SCHEMA_MIGRATION_LOCK_ID = UUIDs.startOf(1);
  private static final Logger LOG = LoggerFactory.getLogger(SchemaMigrationLock.class);
  private final Session session;
  private final boolean locked;

  public SchemaMigrationLock(VersionNumber version, Session session, ReaperApplicationConfiguration config) {

    Preconditions.checkArgument(
        !config.isInSidecarMode() || config.getEnableConcurrentMigrations(),
        "Running in sidecar mode and concurrent migrations are not enabled.");

    this.session = session;
    this.locked = migrationConsensusAvailable(config, version) && lockSchemaMigration();
  }

  @Override
  public void close() {
    if (locked) {
      unlockSchemaMigration();
    }
  }

  private boolean lockSchemaMigration() {
    while (true) {
      LOG.debug("Trying to take lead on schema migrations");

      ResultSet lwtResult = session.execute(
          "INSERT INTO system_distributed.parent_repair_history (parent_id, started_at) VALUES ("
            + SCHEMA_MIGRATION_LOCK_ID + ", dateOf(now())) IF NOT EXISTS USING TTL 300");

      if (lwtResult.wasApplied()) {
        LOG.debug("Took lead on schema migrations");
        return true;
      }

      LOG.info("Cannot grab the lock on schema migration. Waiting for it to be released...");
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  private synchronized boolean unlockSchemaMigration() {
    LOG.debug("Trying to take lead on schema migrations");

    ResultSet lwtResult = session.execute(
        "DELETE FROM system_distributed.parent_repair_history WHERE parent_id = " + SCHEMA_MIGRATION_LOCK_ID
        + " IF EXISTS");

    if (lwtResult.wasApplied()) {
      LOG.debug("Released lead on schema migrations");
      return true;
    }
    // Another instance took the lead on the segment
    LOG.error("Could not release lead on schema migrations");
    return false;
  }

  private static boolean migrationConsensusAvailable(ReaperApplicationConfiguration config, VersionNumber version) {
    return config.getEnableConcurrentMigrations() && VersionNumber.parse("2.2").compareTo(version) <= 0;
  }
}
