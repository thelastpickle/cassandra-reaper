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
package com.spotify.reaper.storage.postgresql;

import com.spotify.reaper.core.RepairRun;

import org.apache.cassandra.repair.RepairParallelism;
import org.joda.time.DateTime;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

public class RepairRunMapper implements ResultSetMapper<RepairRun> {

  static DateTime getDateTimeOrNull(ResultSet r, String dbColumnName) throws SQLException {
    Timestamp timestamp = r.getTimestamp(dbColumnName);
    DateTime result = null;
    if (null != timestamp) {
      result = new DateTime(timestamp);
    }
    return result;
  }

  public RepairRun map(int index, ResultSet r, StatementContext ctx) throws SQLException {
    RepairRun.RunState runState = RepairRun.RunState.valueOf(r.getString("state"));
    RepairParallelism repairParallelism =
        RepairParallelism.valueOf(r.getString("repair_parallelism"));
    RepairRun.Builder repairRunBuilder =
        new RepairRun.Builder(r.getString("cluster_name"),
                              r.getLong("repair_unit_id"),
                              getDateTimeOrNull(r, "creation_time"),
                              r.getFloat("intensity"),
                              r.getInt("segment_count"),
                              repairParallelism);
    return repairRunBuilder
        .runState(runState)
        .owner(r.getString("owner"))
        .cause(r.getString("cause"))
        .startTime(getDateTimeOrNull(r, "start_time"))
        .endTime(getDateTimeOrNull(r, "end_time"))
        .pauseTime(getDateTimeOrNull(r, "pause_time"))
        .lastEvent(r.getString("last_event"))
        .build(r.getLong("id"));
  }

}
