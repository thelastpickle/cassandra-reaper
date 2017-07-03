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

import com.google.common.collect.ImmutableSet;

import com.spotify.reaper.core.RepairSchedule;
import com.spotify.reaper.core.RepairUnit;
import com.spotify.reaper.resources.view.RepairScheduleStatus;

import com.spotify.reaper.storage.PostgresStorage;
import org.apache.cassandra.repair.RepairParallelism;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class RepairScheduleStatusMapper implements ResultSetMapper<RepairScheduleStatus> {

  @Override
  public RepairScheduleStatus map(int index, ResultSet r, StatementContext ctx)
      throws SQLException {

    return new RepairScheduleStatus(
        PostgresStorage.fromSequenceId(r.getLong("id")),
        r.getString("owner"),
        r.getString("cluster_name"),
        r.getString("keyspace_name"),
        ImmutableSet.copyOf((String[]) r.getArray("column_families").getArray()),
        RepairSchedule.State.valueOf(r.getString("state")),
        RepairRunMapper.getDateTimeOrNull(r, "creation_time"),
        RepairRunMapper.getDateTimeOrNull(r, "next_activation"),
        RepairRunMapper.getDateTimeOrNull(r, "pause_time"),
        r.getDouble("intensity"),
        r.getBoolean("incremental_repair"),
        r.getInt("segment_count"),
        RepairParallelism.fromName(r.getString("repair_parallelism").toLowerCase().replace("datacenter_aware", "dc_parallel")),
        r.getInt("days_between")
    );
  }
}
