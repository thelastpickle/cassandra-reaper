/*
 * Copyright 2017-2017 Spotify AB
 * Copyright 2017-2019 The Last Pickle Ltd
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

import java.util.Date;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class FixRepairSegmentTimestamps {

  private static final Logger LOG = LoggerFactory.getLogger(FixRepairSegmentTimestamps.class);

  private FixRepairSegmentTimestamps() {
  }

  /**
   * fix nulls in the repair_run table
   */
  public static void migrate(Session session) {
    LOG.warn("Removing NULLs in the repair_run table. This may take some minutes…");

    Statement getRepairSegmentsPrepStmt
        = new SimpleStatement("SELECT id,segment_id,segment_state,segment_start_time,segment_end_time FROM repair_run")
            .setConsistencyLevel(ConsistencyLevel.QUORUM);

    PreparedStatement updateRepairSegmentPrepStmt = session
        .prepare("INSERT INTO repair_run (id,segment_id,segment_start_time,segment_end_time)  VALUES(?, ?, ?, ?)")
        .setConsistencyLevel(ConsistencyLevel.EACH_QUORUM);

    ResultSet resultSet = session.execute(getRepairSegmentsPrepStmt);
    int rowsRead = 0;
    for (Row row : resultSet) {
      resultSet.fetchMoreResults();
      boolean update = false;
      RepairSegment.State state = RepairSegment.State.values()[row.getInt("segment_state")];
      Date startTime = row.getTimestamp("segment_start_time");
      Date endTime = row.getTimestamp("segment_end_time");

      // startTime can only be unset if segment is NOT_STARTED
      if (RepairSegment.State.NOT_STARTED != state && null == startTime) {
        update = true;
        startTime = new Date(0);
      }

      // endTime can only be set if segment is DONE
      if (RepairSegment.State.DONE != state && null != endTime) {
        update = true;
        endTime = null;
      }

      // endTime must be set if segment is DONE
      if (RepairSegment.State.DONE == state && null == endTime) {
        update = true;
        endTime = startTime;
      }

      if (update) {
        session.executeAsync(
            updateRepairSegmentPrepStmt.bind(row.getUUID("id"), row.getUUID("segment_id"), startTime, endTime));
      }
      ++rowsRead;
      if (0 == rowsRead % 1000) {
        LOG.warn("rows read: " + rowsRead);
      }
    }

    LOG.warn("Removal of NULLs in the repair_run table completed.");
  }
}
