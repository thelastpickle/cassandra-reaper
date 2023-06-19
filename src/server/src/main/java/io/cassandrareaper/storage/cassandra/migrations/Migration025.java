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

package io.cassandrareaper.storage.cassandra.migrations;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Migration025 {

  private static final Logger LOG = LoggerFactory.getLogger(Migration025.class);
  private static final String V1_TABLE = "repair_run_by_cluster";
  private static final String V2_TABLE = "repair_run_by_cluster_v2";
  private static PreparedStatement v2_insert;

  private Migration025() {
  }

  /**
   * Switch to v2 of repair_run_by_cluster
   */
  public static void migrate(Session session, String keyspace) {

    try {
      if (session.getCluster().getMetadata().getKeyspace(keyspace).getTable(V1_TABLE) != null) {
        v2_insert = session.prepare(
            "INSERT INTO " + V2_TABLE + "(cluster_name, id, repair_run_state) values (?, ?, ?)");
        LOG.info("Converting {} table...", V1_TABLE);
        ResultSet results = session.execute("SELECT * FROM " + V1_TABLE);
        for (Row row : results) {
          ResultSet runResults = session.execute(
              "SELECT distinct state from repair_run where id = " + row.getUUID("id"));
          for (Row runRow : runResults) {
            String state = runRow.getString("state");
            session.execute(v2_insert.bind(row.getString("cluster_name"), row.getUUID("id"), state));
          }
        }
        session.execute("DROP TABLE " + V1_TABLE);
      }
    } catch (RuntimeException e) {
      LOG.error("Failed transferring rows to " + V2_TABLE, e);
    }
  }
}
