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

import io.cassandrareaper.core.RepairSegment;
import io.cassandrareaper.service.RingRange;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

public final class RepairSegmentMapper implements ResultSetMapper<RepairSegment> {

  @Override
  public RepairSegment map(int index, ResultSet rs, StatementContext ctx) throws SQLException {
    RingRange range
        = new RingRange(rs.getBigDecimal("start_token").toBigInteger(), rs.getBigDecimal("end_token").toBigInteger());

    RepairSegment.Builder builder = RepairSegment
        .builder(range, UuidUtil.fromSequenceId(rs.getLong("repair_unit_id")))
        .withRunId(UuidUtil.fromSequenceId(rs.getLong("run_id")))
        .state(RepairSegment.State.values()[rs.getInt("state")])
        .failCount(rs.getInt("fail_count"));

    if (null != rs.getString("coordinator_host")) {
      builder = builder.coordinatorHost(rs.getString("coordinator_host"));
    }
    if (null != RepairRunMapper.getDateTimeOrNull(rs, "start_time")) {
      builder = builder.startTime(RepairRunMapper.getDateTimeOrNull(rs, "start_time"));
    }
    if (null != RepairRunMapper.getDateTimeOrNull(rs, "end_time")) {
      builder = builder.endTime(RepairRunMapper.getDateTimeOrNull(rs, "end_time"));
    }
    return builder.build(UuidUtil.fromSequenceId(rs.getLong("id")));
  }
}
