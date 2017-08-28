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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import com.google.common.collect.Sets;
import com.spotify.reaper.core.RepairUnit;

public class RepairUnitMapper implements ResultSetMapper<RepairUnit> {

  @Override
  public RepairUnit map(int index, ResultSet r, StatementContext ctx) throws SQLException {

    String[] columnFamilies = parseStringArray(r.getArray("column_families").getArray());
    String[] nodes = parseStringArray(r.getArray("nodes").getArray());
    String[] datacenters = parseStringArray(r.getArray("datacenters").getArray());


    RepairUnit.Builder builder = new RepairUnit.Builder(r.getString("cluster_name"), r.getString("keyspace_name"),
        Sets.newHashSet(columnFamilies), r.getBoolean("incremental_repair"), Sets.newHashSet(nodes),
        Sets.newHashSet(datacenters));
    return builder.build(UuidUtil.fromSequenceId(r.getLong("id")));
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
