package com.spotify.reaper.storage.postgresql;

import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.Argument;
import org.skife.jdbi.v2.tweak.ArgumentFactory;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;

/**
 * Required as we are using JDBI, and it cannot do Array binding otherwise, duh.
 */
public class PostgresArrayArgumentFactory implements ArgumentFactory<Collection<String>> {

  @Override
  public boolean accepts(Class<?> expectedType, Object value, StatementContext ctx) {
    return value instanceof Collection;
  }

  @Override
  public Argument build(Class<?> expectedType, final Collection<String> value,
                        StatementContext ctx) {
    return new Argument() {
      public void apply(int position,
                        PreparedStatement statement,
                        StatementContext ctx) throws SQLException {
        Array sqlArray = ctx.getConnection().createArrayOf("text", value.toArray());
        statement.setArray(position, sqlArray);
      }
    };
  }
}
