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

import com.spotify.reaper.core.RepairSchedule;

import org.apache.cassandra.repair.RepairParallelism;
import org.apache.commons.lang.ArrayUtils;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class RepairScheduleMapper implements ResultSetMapper<RepairSchedule> {

  @Override
  public RepairSchedule map(int index, ResultSet r, StatementContext ctx) throws SQLException {
    return new RepairSchedule.Builder(
        r.getLong("repair_unit_id"),
        RepairSchedule.State.valueOf(r.getString("state")),
        r.getInt("days_between"),
        RepairRunMapper.getDateTimeOrNull(r, "next_activation"),
        ImmutableList.copyOf(ArrayUtils.toObject((long[]) r.getArray("run_history").getArray())),
        r.getInt("segment_count"),
        RepairParallelism.valueOf(r.getString("repair_parallelism")),
        r.getDouble("intensity"),
        RepairRunMapper.getDateTimeOrNull(r, "creation_time"))
        .owner(r.getString("owner"))
        .pauseTime(RepairRunMapper.getDateTimeOrNull(r, "pause_time"))
        .lastEvent(r.getString("last_event"))
        .build(r.getLong("id"));
  }
}
