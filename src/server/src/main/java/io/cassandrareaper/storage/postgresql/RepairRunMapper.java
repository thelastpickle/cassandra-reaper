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

package io.cassandrareaper.storage.postgresql;

import io.cassandrareaper.core.RepairRun;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.apache.cassandra.repair.RepairParallelism;
import org.joda.time.DateTime;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

public final class RepairRunMapper implements ResultSetMapper<RepairRun> {

  static DateTime getDateTimeOrNull(ResultSet rs, String dbColumnName) throws SQLException {
    Timestamp timestamp = rs.getTimestamp(dbColumnName);
    DateTime result = null;
    if (null != timestamp) {
      result = new DateTime(timestamp);
    }
    return result;
  }

  @Override
  public RepairRun map(int index, ResultSet rs, StatementContext ctx) throws SQLException {
    RepairRun.RunState runState = RepairRun.RunState.valueOf(rs.getString("state"));
    RepairParallelism repairParallelism = RepairParallelism.fromName(
        rs.getString("repair_parallelism").toLowerCase().replace("datacenter_aware", "dc_parallel"));

    return RepairRun.builder(rs.getString("cluster_name"), UuidUtil.fromSequenceId(rs.getLong("repair_unit_id")))
        .creationTime(getDateTimeOrNull(rs, "creation_time"))
        .intensity(rs.getDouble("intensity"))
        .segmentCount(rs.getInt("segment_count"))
        .repairParallelism(repairParallelism)
        .runState(runState)
        .owner(rs.getString("owner"))
        .cause(rs.getString("cause"))
        .startTime(getDateTimeOrNull(rs, "start_time"))
        .endTime(getDateTimeOrNull(rs, "end_time"))
        .pauseTime(getDateTimeOrNull(rs, "pause_time"))
        .lastEvent(rs.getString("last_event"))
        .build(UuidUtil.fromSequenceId(rs.getLong("id")));
  }
}
