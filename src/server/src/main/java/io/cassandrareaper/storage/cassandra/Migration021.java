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

import com.datastax.driver.core.Session;
import com.datastax.driver.core.VersionNumber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Migration021 {

  private static final Logger LOG = LoggerFactory.getLogger(Migration021.class);
  private static final String METRICS_V1_TABLE = "node_metrics_v1";
  private static final String OPERATIONS_TABLE = "node_operations";

  private Migration021() {
  }

  /**
   * Apply TWCS for metrics tables if the Cassandra version allows it.
   */
  public static void migrate(Session session, String keyspace) {

    VersionNumber lowestNodeVersion = session.getCluster().getMetadata().getAllHosts()
        .stream()
        .map(host -> host.getCassandraVersion())
        .min(VersionNumber::compareTo)
        .get();

    if ((VersionNumber.parse("3.0.8").compareTo(lowestNodeVersion) <= 0
        && VersionNumber.parse("3.0.99").compareTo(lowestNodeVersion) >= 0)
        || VersionNumber.parse("3.8").compareTo(lowestNodeVersion) <= 0) {
      try {
        if (!isUsingTwcs(session, keyspace)) {
          LOG.info("Altering {} to use TWCS...", METRICS_V1_TABLE);
          session.execute(
                  "ALTER TABLE " + METRICS_V1_TABLE + " WITH compaction = {'class': 'TimeWindowCompactionStrategy', "
                      + "'unchecked_tombstone_compaction': 'true', "
                      + "'compaction_window_size': '2', "
                      + "'compaction_window_unit': 'MINUTES'}");

          LOG.info("Altering {} to use TWCS...", OPERATIONS_TABLE);
          session.execute(
                  "ALTER TABLE " + OPERATIONS_TABLE + " WITH compaction = {'class': 'TimeWindowCompactionStrategy', "
                      + "'unchecked_tombstone_compaction': 'true', "
                      + "'compaction_window_size': '30', "
                      + "'compaction_window_unit': 'MINUTES'}");

          LOG.info("{} was successfully altered to use TWCS.", OPERATIONS_TABLE);
        }
      } catch (RuntimeException e) {
        LOG.error("Failed altering metrics tables to TWCS", e);
      }
    }

  }

  private static boolean isUsingTwcs(Session session, String keyspace) {
    return session
        .getCluster()
        .getMetadata()
        .getKeyspace(keyspace)
        .getTable(METRICS_V1_TABLE)
        .getOptions()
        .getCompaction()
        .get("class")
        .contains("TimeWindowCompactionStrategy");
  }
}
