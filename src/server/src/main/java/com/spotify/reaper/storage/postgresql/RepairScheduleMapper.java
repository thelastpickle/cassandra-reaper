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

import com.google.common.collect.ImmutableList;
import com.spotify.reaper.scheduler.RepairSchedule;

import org.apache.cassandra.repair.RepairParallelism;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.UUID;

public class RepairScheduleMapper implements ResultSetMapper<RepairSchedule> {

  @Override
  public RepairSchedule map(int index, ResultSet r, StatementContext ctx) throws SQLException {
    
    UUID[] runHistoryUUIDs = new UUID[0];
    
    Number[] runHistory = null;
    Array av = r.getArray("run_history");
    if(null != av) {
      Object obj = av.getArray();
      if(obj instanceof Number[]) {
        runHistory = (Number[])obj;
      } else if(obj instanceof Object[]) {
        Object[] ol = (Object[])obj;
        runHistory = Arrays.copyOf(ol, ol.length, Number[].class);
      }
      
      if (null != runHistory && runHistory.length > 0) {
        runHistoryUUIDs = new UUID[runHistory.length];
        for (int i = 0; i < runHistory.length; i++) {
          runHistoryUUIDs[i] = UuidUtil.fromSequenceId(runHistory[i].longValue());
        }
      }
    }  
    
    String stateStr = r.getString("state");
    // For temporary backward compatibility reasons, supporting RUNNING state as ACTIVE.
    if ("RUNNING".equalsIgnoreCase(stateStr)) {
      stateStr = "ACTIVE";
    }

    RepairSchedule.State scheduleState = RepairSchedule.State.valueOf(stateStr);
    return new RepairSchedule.Builder(
        UuidUtil.fromSequenceId(r.getLong("repair_unit_id")),
        scheduleState,
        r.getInt("days_between"),
        RepairRunMapper.getDateTimeOrNull(r, "next_activation"),
        ImmutableList.copyOf(runHistoryUUIDs),
        r.getInt("segment_count"),
        RepairParallelism.fromName(r.getString("repair_parallelism").toLowerCase().replace("datacenter_aware", "dc_parallel")),
        r.getDouble("intensity"),
        RepairRunMapper.getDateTimeOrNull(r, "creation_time"))
        .owner(r.getString("owner"))
        .pauseTime(RepairRunMapper.getDateTimeOrNull(r, "pause_time"))
        .build(UuidUtil.fromSequenceId(r.getLong("id")));
  }
}
