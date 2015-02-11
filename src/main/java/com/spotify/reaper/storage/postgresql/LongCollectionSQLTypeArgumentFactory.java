package com.spotify.reaper.storage.postgresql;

import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.Argument;
import org.skife.jdbi.v2.tweak.ArgumentFactory;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Provides JDBI a method to map our custom Long collection into an SQL Array type.
 *
 * NOTICE: this is very ugly due to not being able to have different generic types
 * except Strings when using Collections with JDBI here.
 */
public class LongCollectionSQLTypeArgumentFactory
    implements ArgumentFactory<LongCollectionSQLType> {

  @Override
  public boolean accepts(Class<?> expectedType, Object value, StatementContext ctx) {
    return value instanceof LongCollectionSQLType;
  }

  @Override
  public Argument build(Class<?> expectedType, final LongCollectionSQLType value,
                        StatementContext ctx) {
    return new Argument() {
      public void apply(int position, PreparedStatement statement, StatementContext ctx)
          throws SQLException {
        Array sqlArray = ctx.getConnection().createArrayOf("int", value.getValue().toArray());
        statement.setArray(position, sqlArray);
      }
    };
  }
}
