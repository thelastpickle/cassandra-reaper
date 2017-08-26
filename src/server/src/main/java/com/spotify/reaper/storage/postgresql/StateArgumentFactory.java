package com.spotify.reaper.storage.postgresql;

import com.spotify.reaper.repair.segment.RepairSegment;

import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.Argument;
import org.skife.jdbi.v2.tweak.ArgumentFactory;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Provides JDBI a method to map State value to an INT value in database.
 */
public class StateArgumentFactory implements ArgumentFactory<RepairSegment.State> {

  @Override
  public boolean accepts(Class<?> expectedType, Object value, StatementContext ctx) {
    return value instanceof RepairSegment.State;
  }

  @Override
  public Argument build(Class<?> expectedType, final RepairSegment.State value,
                        StatementContext ctx) {
    return new Argument() {
      public void apply(int position, PreparedStatement statement, StatementContext ctx)
          throws SQLException {
        statement.setInt(position, value.ordinal());
      }
    };
  }
}
