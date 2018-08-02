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

package io.cassandrareaper;

import java.io.File;
import java.io.IOException;

import org.flywaydb.core.Flyway;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.Test;

public final class ReaperApplicationTest {

  @Test
  public void testFlywayOnH2() throws IOException {
    Flyway flyway = new Flyway();
    flyway.setLocations("/db/h2");
    JdbcDataSource ds = new JdbcDataSource();
    File db = File.createTempFile("cassandra-reaper", "h2");
    try {
      ds.setUrl("jdbc:h2:" + db.getAbsolutePath() + ";MODE=PostgreSQL");
      flyway.setDataSource(ds);
      flyway.migrate();
    } finally {
      db.delete();
    }
  }
}
