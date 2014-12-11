package com.spotify.reaper.storage.postgresql;

import com.spotify.reaper.core.RepairRun;

import org.joda.time.DateTime;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

public class RepairRunMapper implements ResultSetMapper<RepairRun> {

  public RepairRun map(int index, ResultSet r, StatementContext ctx) throws SQLException {
    RepairRun.RunState runState = RepairRun.RunState.valueOf(r.getString("state"));
    return new RepairRun.Builder(runState,
                                 getDateTimeOrNull(r, "creation_time"),
                                 r.getFloat("intensity"))
        .owner(r.getString("owner"))
        .cause(r.getString("cause"))
        .startTime(getDateTimeOrNull(r, "start_time"))
        .endTime(getDateTimeOrNull(r, "end_time"))
        .build(r.getLong("id"));
  }

  static final DateTime getDateTimeOrNull(ResultSet r, String columnName)
      throws SQLException {
    Timestamp timestamp = r.getTimestamp(columnName);
    DateTime result = null;
    if (null != timestamp) {
      result = new DateTime(timestamp);
    }
    return result;
  }

}
