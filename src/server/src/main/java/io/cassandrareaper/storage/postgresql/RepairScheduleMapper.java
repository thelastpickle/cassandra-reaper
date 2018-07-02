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

import io.cassandrareaper.core.RepairSchedule;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.UUID;

import com.google.common.collect.ImmutableList;
import org.apache.cassandra.repair.RepairParallelism;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

public final class RepairScheduleMapper implements ResultSetMapper<RepairSchedule> {

  @Override
  public RepairSchedule map(int index, ResultSet rs, StatementContext ctx) throws SQLException {

    UUID[] runHistoryUuids = new UUID[0];

    Number[] runHistory = null;
    Array av = rs.getArray("run_history");
    if (null != av) {
      Object obj = av.getArray();
      if (obj instanceof Number[]) {
        runHistory = (Number[]) obj;
      } else if (obj instanceof Object[]) {
        Object[] ol = (Object[]) obj;
        runHistory = Arrays.copyOf(ol, ol.length, Number[].class);
      }

      if (null != runHistory && runHistory.length > 0) {
        runHistoryUuids = new UUID[runHistory.length];
        for (int i = 0; i < runHistory.length; i++) {
          runHistoryUuids[i] = UuidUtil.fromSequenceId(runHistory[i].longValue());
        }
      }
    }

    String stateStr = rs.getString("state");
    // For temporary backward compatibility reasons, supporting RUNNING state as ACTIVE.
    if ("RUNNING".equalsIgnoreCase(stateStr)) {
      stateStr = "ACTIVE";
    }

    RepairSchedule.State scheduleState = RepairSchedule.State.valueOf(stateStr);
    String parallelism = rs.getString("repair_parallelism").toLowerCase().replace("datacenter_aware", "dc_parallel");

    return RepairSchedule.builder(UuidUtil.fromSequenceId(rs.getLong("repair_unit_id")))
        .state(scheduleState)
        .daysBetween(rs.getInt("days_between"))
        .nextActivation(RepairRunMapper.getDateTimeOrNull(rs, "next_activation"))
        .runHistory(ImmutableList.copyOf(runHistoryUuids))
        .segmentCount(rs.getInt("segment_count"))
        .repairParallelism(RepairParallelism.fromName(parallelism))
        .intensity(rs.getDouble("intensity"))
        .creationTime(RepairRunMapper.getDateTimeOrNull(rs, "creation_time"))
        .segmentCountPerNode(rs.getInt("segment_count_per_node"))
        .owner(rs.getString("owner"))
        .pauseTime(RepairRunMapper.getDateTimeOrNull(rs, "pause_time"))
        .majorCompaction(rs.getBoolean("major_compaction"))
        .build(UuidUtil.fromSequenceId(rs.getLong("id")));
  }
}
