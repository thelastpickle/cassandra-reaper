/*
 *
 * Copyright 2019-2019 The Last Pickle Ltd
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

import io.cassandrareaper.core.RepairRun;

import java.time.Instant;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class FixRepairRunTimestamps {

  private static final Logger LOG = LoggerFactory.getLogger(FixRepairRunTimestamps.class);

  private FixRepairRunTimestamps() {}

  /** fix timestamps in the repair_run table */
  public static void migrate(CqlSession session) {
    LOG.warn("Correcting timestamps in the repair_run table. This may take some minutes…");

    SimpleStatement getRepairRunPrepStmt =
        SimpleStatement.builder("SELECT id,state,start_time,pause_time,end_time FROM repair_run")
            .setConsistencyLevel(ConsistencyLevel.QUORUM)
            .build();

    PreparedStatement updateRepairRunPrepStmt =
        session.prepare(
            SimpleStatement.builder(
                    "INSERT INTO repair_run (id,start_time,pause_time,end_time) "
                        + "VALUES(?, ?, ?, ?)")
                .setConsistencyLevel(ConsistencyLevel.EACH_QUORUM)
                .build());

    ResultSet resultSet = session.execute(getRepairRunPrepStmt);
    int rowsRead = 0;
    for (Row row : resultSet) {
      boolean update = false;
      RepairRun.RunState state = RepairRun.RunState.valueOf(row.getString("state"));

      // startTime must be null if repairRun is NOT_STARTED
      Instant startTime = row.getInstant("start_time");
      if (RepairRun.RunState.NOT_STARTED == state && null != startTime) {
        update = true;
        startTime = null;
      }

      // startTime must be set if repairRun is not NOT_STARTED
      if (RepairRun.RunState.NOT_STARTED != state && null == startTime) {
        update = true;
        startTime = Instant.EPOCH;
      }

      // pauseTime can only be set if repairRun is paused
      Instant pauseTime = row.getInstant("pause_time");
      if (RepairRun.RunState.PAUSED != state && null != pauseTime) {
        update = true;
        pauseTime = null;
      }

      // pauseTime must be set if repairRun is paused
      if (RepairRun.RunState.PAUSED == state && null == pauseTime) {
        update = true;
        pauseTime = startTime;
      }

      // endTime can only be set if repairRun is terminated
      Instant endTime = row.getInstant("end_time");
      if (!state.isTerminated() && null != endTime) {
        update = true;
        endTime = null;
      }

      // endTime must be set if repairRun is terminated
      if (state.isTerminated() && null == endTime) {
        update = true;
        endTime = startTime;
      }

      if (update) {
        session.executeAsync(
            updateRepairRunPrepStmt.bind(row.getUuid("id"), startTime, pauseTime, endTime));
      }
      ++rowsRead;
      if (0 == rowsRead % 1000) {
        LOG.warn("rows read: " + rowsRead);
      }
    }

    LOG.warn("Correction of timestamps in the repair_run table completed.");
  }
}
