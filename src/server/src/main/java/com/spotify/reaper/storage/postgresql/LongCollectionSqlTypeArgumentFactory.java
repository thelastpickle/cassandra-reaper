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

import com.spotify.reaper.core.RepairSchedule.LongCollectionSqlType;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.Argument;
import org.skife.jdbi.v2.tweak.ArgumentFactory;

/**
 * Provides JDBI a method to map our custom Long collection into an SQL Array type.
 *
 * <p>
 * NOTICE: this is very ugly due to not being able to have different generic types except Strings when using
 * Collections with JDBI here.
 */
public final class LongCollectionSqlTypeArgumentFactory implements ArgumentFactory<LongCollectionSqlType> {

  @Override
  public boolean accepts(Class<?> expectedType, Object value, StatementContext ctx) {
    return value instanceof LongCollectionSqlType;
  }

  @Override
  public Argument build(Class<?> expectedType, final LongCollectionSqlType value, StatementContext ctx) {
    return (int position, PreparedStatement statement, StatementContext ctx1) -> {
      try {
        Array sqlArray = ctx1.getConnection().createArrayOf("int", value.getValue().toArray());
        statement.setArray(position, sqlArray);
      } catch (SQLException e) {
        // H2 DB feature not supported: "createArray" error
        if (e.getErrorCode() != 50100) {
          throw e;
        }
        statement.setObject(position, value.getValue().toArray());
      }
    };
  }
}
