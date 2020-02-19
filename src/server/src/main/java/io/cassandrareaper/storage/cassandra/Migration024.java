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


import io.cassandrareaper.core.RepairSegment;

import java.util.UUID;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public final class Migration024 {

  private static final String REPAIR_RUN_TABLE = "repair_run";

  private Migration024() {
  }

  /**
   * Populate the segment_active column of the repair_run table based on the value
   * of the segment_state column.
   */
  public static void migrate(Session session, String keyspace) {
    PreparedStatement updateStatement = session.prepare(
        "UPDATE " + REPAIR_RUN_TABLE + " SET segment_active = true WHERE id = ? AND segment_id = ?")
        .setConsistencyLevel(ConsistencyLevel.EACH_QUORUM);
    ResultSet resultSet
        = session.execute(
            "SELECT id, segment_id, segment_state FROM " + REPAIR_RUN_TABLE,
            ConsistencyLevel.QUORUM);
    for (Row row : resultSet) {
      UUID runId = row.getUUID("id");
      UUID segmentId = row.getUUID("segment_id");
      boolean segmentActive;
      try {
        int segmentState = row.getInt("segment_state");
        segmentActive = segmentState == RepairSegment.State.STARTED.ordinal()
            || segmentState == RepairSegment.State.RUNNING.ordinal();
      } catch (NullPointerException e) {
        // A NPE might happen if the the segment_state column is null for some
        // reason.
        continue;
      }
      // We do not really care whether segment_active is null or false, so we
      // only update it if it has to be true.
      if (segmentActive) {
        session.execute(updateStatement.bind(runId, segmentId));
      }
    }
  }

}
