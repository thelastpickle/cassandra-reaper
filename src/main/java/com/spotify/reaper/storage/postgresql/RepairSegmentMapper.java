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

import com.datastax.driver.core.utils.UUIDs;
import com.spotify.reaper.core.RepairSegment;
import com.spotify.reaper.service.RingRange;

import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

public class RepairSegmentMapper implements ResultSetMapper<RepairSegment> {

  public RepairSegment map(int index, ResultSet r, StatementContext ctx) throws SQLException {
    RingRange range = new RingRange(r.getBigDecimal("start_token").toBigInteger(),
                                    r.getBigDecimal("end_token").toBigInteger());
    RepairSegment.Builder repairSegmentBuilder =
        new RepairSegment.Builder(fromSequenceId(r.getLong("run_id")), range, fromSequenceId(r.getLong("repair_unit_id")));
    return repairSegmentBuilder
        .state(RepairSegment.State.values()[r.getInt("state")])
        .coordinatorHost(r.getString("coordinator_host"))
        .startTime(RepairRunMapper.getDateTimeOrNull(r, "start_time"))
        .endTime(RepairRunMapper.getDateTimeOrNull(r, "end_time"))
        .failCount(r.getInt("fail_count"))
        .build(fromSequenceId(r.getLong("id")));
  }

  private static UUID fromSequenceId(long insertedId) {
    return new UUID(insertedId, UUIDs.timeBased().getLeastSignificantBits());
  }
}
