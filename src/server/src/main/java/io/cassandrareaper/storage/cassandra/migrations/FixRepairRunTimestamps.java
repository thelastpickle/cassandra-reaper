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

public final class FixRepairRunTimestamps {

  private static final Logger LOG = LoggerFactory.getLogger(FixRepairRunTimestamps.class);

  private FixRepairRunTimestamps() {
  }

  /**
   * fix timestamps in the repair_run table
   */
  public static void migrate(Session session) {
    LOG.warn("Correcting timestamps in the repair_run table. This may take some minutes…");

    Statement getRepairRunPrepStmt
        = new SimpleStatement("SELECT id,state,start_time,pause_time,end_time FROM repair_run")
            .setConsistencyLevel(ConsistencyLevel.QUORUM);

    PreparedStatement updateRepairRunPrepStmt = session
        .prepare("INSERT INTO repair_run (id,start_time,pause_time,end_time) VALUES(?, ?, ?, ?)")
        .setConsistencyLevel(ConsistencyLevel.EACH_QUORUM);

    ResultSet resultSet = session.execute(getRepairRunPrepStmt);
    int rowsRead = 0;
    for (Row row : resultSet) {
      resultSet.fetchMoreResults();
      boolean update = false;
      RepairRun.RunState state = RepairRun.RunState.valueOf(row.getString("state"));

      // startTime must be null if repairRun is NOT_STARTED
      Date startTime = row.getTimestamp("start_time");
      if (RepairRun.RunState.NOT_STARTED == state && null != startTime) {
        update = true;
        startTime = null;
      }

      // startTime must be set if repairRun is not NOT_STARTED
      if (RepairRun.RunState.NOT_STARTED != state && null == startTime) {
        update = true;
        startTime = new Date(0);
      }

      // pauseTime can only be set if repairRun is paused
      Date pauseTime = row.getTimestamp("pause_time");
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
      Date endTime = row.getTimestamp("end_time");
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
            updateRepairRunPrepStmt.bind(row.getUUID("id"), startTime, pauseTime, endTime));
      }
      ++rowsRead;
      if (0 == rowsRead % 1000) {
        LOG.warn("rows read: " + rowsRead);
      }
    }

    LOG.warn("Correction of timestamps in the repair_run table completed.");
  }
}
