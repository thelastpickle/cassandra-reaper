/*
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


import com.datastax.driver.core.Session;
import com.datastax.driver.core.VersionNumber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Migration014 {

  private static final Logger LOG = LoggerFactory.getLogger(Migration014.class);

  private Migration014() {
  }

  /**
   * if Cassandra is running version less than 4.0
   *  alter every table to set `dclocal_read_repair_chance` to zero
   */
  public static void migrate(Session session, String keyspace) {

    VersionNumber highestNodeVersion = session.getCluster().getMetadata().getAllHosts()
        .stream()
        .map(host -> host.getCassandraVersion())
        .max(VersionNumber::compareTo)
        .get();

    if (0 < VersionNumber.parse("4.0").compareTo(highestNodeVersion)) {
      LOG.warn("altering every table to set `dclocal_read_repair_chance` to zero…");
      session.getCluster().getMetadata().getKeyspace(keyspace).getTables()
          .stream()
          .filter(table -> !table.getName().equals("repair_schedule") && !table.getName().equals("repair_unit"))
          .forEach(tbl -> session.executeAsync(
              "ALTER TABLE " + tbl.getName() + " WITH dclocal_read_repair_chance = 0"));

      LOG.warn("alter every table to set `dclocal_read_repair_chance` to zero completed.");
    }

  }
}
