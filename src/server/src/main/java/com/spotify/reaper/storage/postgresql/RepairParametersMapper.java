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

import com.google.common.collect.Sets;

import com.spotify.reaper.service.RepairParameters;
import com.spotify.reaper.service.RingRange;

import org.apache.cassandra.repair.RepairParallelism;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

public class RepairParametersMapper implements ResultSetMapper<RepairParameters> {
  @Override
  public RepairParameters map(int index, ResultSet r, StatementContext ctx) throws SQLException {
    RingRange range = new RingRange(r.getBigDecimal("start_token").toBigInteger(),
                                    r.getBigDecimal("end_token").toBigInteger());
    Object columnFamiliesObj = r.getArray("column_families").getArray();
    String[] columnFamilies;
    if (columnFamiliesObj instanceof String[]) {
        columnFamilies = (String[]) columnFamiliesObj;
    } else {
        Object[] objArray = (Object[]) columnFamiliesObj;
        columnFamilies = Arrays.copyOf(objArray, objArray.length, String[].class);
    }

    String repairParallelismStr = r.getString("repair_parallelism");
    if (repairParallelismStr != null)
    {
      repairParallelismStr = repairParallelismStr.toUpperCase();
    }
    RepairParallelism repairParallelism = RepairParallelism.fromName(repairParallelismStr);

    return new RepairParameters(range,
                                r.getString("keyspace_name"),
                                Sets.newHashSet(columnFamilies),
                                repairParallelism);
  }
}
