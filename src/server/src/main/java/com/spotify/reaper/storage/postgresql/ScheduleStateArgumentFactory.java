package com.spotify.reaper.storage.postgresql;

import com.spotify.reaper.scheduler.RepairSchedule;

import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.Argument;
import org.skife.jdbi.v2.tweak.ArgumentFactory;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ScheduleStateArgumentFactory implements ArgumentFactory<RepairSchedule.State> {

  @Override
  public boolean accepts(Class<?> expectedType, Object value, StatementContext ctx) {
    return value instanceof RepairSchedule.State;
  }

  @Override
  public Argument build(Class<?> expectedType, final RepairSchedule.State value,
                        StatementContext ctx) {
    return new Argument() {
      public void apply(int position, PreparedStatement statement, StatementContext ctx)
          throws SQLException {
        statement.setString(position, value.toString());
      }
    };
  }
}
