/*
 * Copyright 2018-2018 The Last Pickle Ltd
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

package io.cassandrareaper.storage.cassandra.migrations;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Migration016 {

  private static final Logger LOG = LoggerFactory.getLogger(Migration016.class);

  private Migration016() {
  }

  /**
   * if Cassandra is running version less than 4.0
   * alter every table to set `dclocal_read_repair_chance` to zero
   */
  public static void migrate(CqlSession session, String keyspace) {

    Version highestNodeVersion = session.getMetadata().getNodes().entrySet()
        .stream()
        .map(host -> host.getValue().getCassandraVersion())
        .max(Version::compareTo)
        .get();

    if (0 < Version.parse("4.0").compareTo(highestNodeVersion)) {
      LOG.warn("altering every table to set `dclocal_read_repair_chance` to zero…");
      session.getMetadata().getKeyspace(keyspace).get().getTables().entrySet()
          .stream()
          .filter(table -> !table.getValue().getName().equals("repair_schedule")
            && !table.getValue().getName().equals("repair_unit"))
          .forEach(tbl -> session.executeAsync(
              "ALTER TABLE " + tbl.getValue().getName() + " WITH dclocal_read_repair_chance = 0"));

      LOG.warn("alter every table to set `dclocal_read_repair_chance` to zero completed.");
    }

  }
}
