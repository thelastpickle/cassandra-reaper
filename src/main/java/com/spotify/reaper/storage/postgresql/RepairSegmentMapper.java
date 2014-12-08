package com.spotify.reaper.storage.postgresql;

import com.spotify.reaper.core.RepairSegment;

import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class RepairSegmentMapper implements ResultSetMapper<RepairSegment> {

  public RepairSegment map(int index, ResultSet r, StatementContext ctx) throws SQLException {
    return new RepairSegment.Builder(r.getLong("run_id"),
                                     r.getBigDecimal("start_token").toBigInteger(),
                                     r.getBigDecimal("end_token").toBigInteger(),
                                     RepairSegment.State.values()[r.getInt("state")])
        .columnFamilyId(r.getLong("column_family_id"))
        .startTime(RepairRunMapper.getDateTimeOrNull(r, "start_time"))
        .endTime(RepairRunMapper.getDateTimeOrNull(r, "end_time"))
        .build(r.getLong("id"));
  }

}
