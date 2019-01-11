/*
 * Copyright 2014-2017 Spotify AB
 * Copyright 2016-2018 The Last Pickle Ltd
 *
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

import io.cassandrareaper.core.Segment;
import io.cassandrareaper.service.RepairParameters;
import io.cassandrareaper.service.RingRange;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

import com.google.common.collect.Sets;
import org.apache.cassandra.repair.RepairParallelism;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

public final class RepairParametersMapper implements ResultSetMapper<RepairParameters> {

  @Override
  public RepairParameters map(int index, ResultSet rs, StatementContext ctx) throws SQLException {

    RingRange range
        = new RingRange(rs.getBigDecimal("start_token").toBigInteger(), rs.getBigDecimal("end_token").toBigInteger());

    Object columnFamiliesObj = rs.getArray("column_families").getArray();
    String[] columnFamilies;
    if (columnFamiliesObj instanceof String[]) {
      columnFamilies = (String[]) columnFamiliesObj;
    } else {
      Object[] objArray = (Object[]) columnFamiliesObj;
      columnFamilies = Arrays.copyOf(objArray, objArray.length, String[].class);
    }

    String repairParallelismStr = rs.getString("repair_parallelism");
    if (repairParallelismStr != null) {
      repairParallelismStr = repairParallelismStr.toUpperCase();
    }
    RepairParallelism repairParallelism = RepairParallelism.fromName(repairParallelismStr);

    return new RepairParameters(
        Segment.builder().withTokenRange(range)
            .withActiveTime(rs.getString("active_time"))
            .withInactiveTime(rs.getString("inactive_time"))
            .build(),
        rs.getString("keyspace_name"),
        Sets.newHashSet(columnFamilies),
        repairParallelism);
  }
}
