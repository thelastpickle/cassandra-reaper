/*
 * Copyright 2020-2020 The Last Pickle Ltd
 *
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

package io.cassandrareaper.storage.cassandra.migrations;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.Version;

public final class Migration034 {

  private static final Logger LOG = LoggerFactory.getLogger(Migration024.class);
  private static final String REPAIR_UNIT_V1_TABLE = "repair_unit_v1";

  private Migration034() {}

  /** Add the subrange_incremental field to the repair_unit_v1 table if needed. */
  public static void migrate(CqlSession session, String keyspace) {
    try {
      if (!hasSubrangeIncrementalField(session, keyspace)) {
        LOG.info("Altering {} to add the subrange_incremental field...", REPAIR_UNIT_V1_TABLE);
        session
            .execute("ALTER TABLE " + REPAIR_UNIT_V1_TABLE + " ADD subrange_incremental boolean");
        LOG.info("{} was successfully altered to add the subrange_incremental field.",
            REPAIR_UNIT_V1_TABLE);
      } else {
        LOG.info("{} already has the subrange_incremental field.", REPAIR_UNIT_V1_TABLE);
      }
    } catch (RuntimeException e) {
      LOG.error("Failed altering {} to add the subrange_incremental field", REPAIR_UNIT_V1_TABLE,
          e);
    }
  }

  private static boolean hasSubrangeIncrementalField(CqlSession session, String keyspace) {
    return session.getMetadata().getKeyspace(keyspace).get().getTable(REPAIR_UNIT_V1_TABLE).get()
        .getColumns().entrySet().stream()
        .anyMatch(entry -> entry.getKey().asCql(false).equals("\"subrange_incremental\""));
  }
}
