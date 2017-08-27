package com.spotify.reaper.storage.postgresql;

import com.spotify.reaper.core.RepairSchedule.LongCollectionSQLType;
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
public final class LongCollectionSQLTypeArgumentFactory implements ArgumentFactory<LongCollectionSQLType> {

  @Override
  public boolean accepts(Class<?> expectedType, Object value, StatementContext ctx) {
    return value instanceof LongCollectionSQLType;
  }

  @Override
  public Argument build(Class<?> expectedType, final LongCollectionSQLType value, StatementContext ctx) {
    return (int position, PreparedStatement statement, StatementContext ctx1) -> {
        try {
            Array sqlArray = ctx1.getConnection().createArrayOf("int", value.getValue().toArray());
            statement.setArray(position, sqlArray);
        }catch(SQLException e) {
            // H2 DB feature not supported: "createArray" error
            if(e.getErrorCode() != 50100) {
                throw e;
            }
            statement.setObject(position, value.getValue().toArray());
        }
    };
  }
}
