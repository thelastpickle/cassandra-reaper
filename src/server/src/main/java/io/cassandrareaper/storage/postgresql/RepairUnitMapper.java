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

package io.cassandrareaper.storage.postgresql;

import io.cassandrareaper.core.RepairUnit;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

import com.google.common.collect.Sets;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

public final class RepairUnitMapper implements ResultSetMapper<RepairUnit> {

  @Override
  public RepairUnit map(int index, ResultSet rs, StatementContext ctx) throws SQLException {

    String[] columnFamilies = parseStringArray(rs.getArray("column_families").getArray());
    String[] nodes =
        rs.getArray("nodes") == null
            ? new String[] {}
            : parseStringArray(rs.getArray("nodes").getArray());
    String[] datacenters =
        rs.getArray("datacenters") == null
            ? new String[] {}
            : parseStringArray(rs.getArray("datacenters").getArray());
    String[] blacklistedTables =
        rs.getArray("blacklisted_tables") == null
            ? new String[] {}
            : parseStringArray(rs.getArray("blacklisted_tables").getArray());

    RepairUnit.Builder builder =
        new RepairUnit.Builder(
            rs.getString("cluster_name"),
            rs.getString("keyspace_name"),
            Sets.newHashSet(columnFamilies),
            rs.getBoolean("incremental_repair"),
            Sets.newHashSet(nodes),
            Sets.newHashSet(datacenters),
            Sets.newHashSet(blacklistedTables));

    return builder.build(UuidUtil.fromSequenceId(rs.getLong("id")));
  }

  private String[] parseStringArray(Object obj) {
    String[] values = null;
    if (obj instanceof String[]) {
      values = (String[]) obj;
    } else if (obj instanceof Object[]) {
      Object[] ocf = (Object[]) obj;
      values = Arrays.copyOf(ocf, ocf.length, String[].class);
    }

    return values;
  }
}
