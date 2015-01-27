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

import com.spotify.reaper.core.RepairUnit;

import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.Argument;
import org.skife.jdbi.v2.tweak.ArgumentFactory;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Provides JDBI a method to map RepairParallelism value to a TEXT value in database.
 */
public class RepairParallelismArgumentFactory
    implements ArgumentFactory<RepairUnit.RepairParallelism> {

  @Override
  public boolean accepts(Class<?> expectedType, Object value, StatementContext ctx) {
    return value instanceof RepairUnit.RepairParallelism;
  }

  @Override
  public Argument build(Class<?> expectedType, final RepairUnit.RepairParallelism value,
                        StatementContext ctx) {
    return new Argument() {
      public void apply(int position, PreparedStatement statement, StatementContext ctx)
          throws SQLException {
        statement.setString(position, value.toString());
      }
    };
  }
}
