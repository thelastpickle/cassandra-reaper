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

package io.cassandrareaper.storage.cassandra.migrations;

import io.cassandrareaper.core.RepairSegment;

import java.time.Instant;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class FixRepairSegmentTimestamps {

  private static final Logger LOG = LoggerFactory.getLogger(FixRepairSegmentTimestamps.class);

  private FixRepairSegmentTimestamps() {
  }

  /**
   * fix nulls in the repair_run table
   */
  public static void migrate(CqlSession session) {
    LOG.warn("Removing NULLs in the repair_run table. This may take some minutes…");

    SimpleStatement getRepairSegmentsPrepStmt
        = SimpleStatement.builder("SELECT id,segment_id,segment_state,segment_start_time,segment_end_time FROM "
        + "repair_run")
        .setConsistencyLevel(ConsistencyLevel.QUORUM).build();

    PreparedStatement updateRepairSegmentPrepStmt = session
        .prepare(SimpleStatement.builder("INSERT INTO repair_run "
            + "(id,segment_id,segment_start_time,segment_end_time)  VALUES(?, ?, ?, ?)")
        .setConsistencyLevel(ConsistencyLevel.EACH_QUORUM).build());

    ResultSet resultSet = session.execute(getRepairSegmentsPrepStmt);
    int rowsRead = 0;
    for (Row row : resultSet) {
      boolean update = false;
      RepairSegment.State state = RepairSegment.State.values()[row.getInt("segment_state")];
      Instant startTime = row.getInstant("segment_start_time");
      Instant endTime = row.getInstant("segment_end_time");

      // startTime can only be unset if segment is NOT_STARTED
      if (RepairSegment.State.NOT_STARTED != state && null == startTime) {
        update = true;
        startTime = Instant.EPOCH;
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
            updateRepairSegmentPrepStmt.bind(row.getUuid("id"), row.getUuid("segment_id"), startTime, endTime));
      }
      ++rowsRead;
      if (0 == rowsRead % 1000) {
        LOG.warn("rows read: " + rowsRead);
      }
    }

    LOG.warn("Removal of NULLs in the repair_run table completed.");
  }
}
