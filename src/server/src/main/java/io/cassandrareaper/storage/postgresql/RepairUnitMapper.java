/*
 * Copyright 2014-2017 Spotify AB
 * Copyright 2016-2019 The Last Pickle Ltd
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

import io.cassandrareaper.core.RepairUnit;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.google.common.collect.Sets;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

public final class RepairUnitMapper implements ResultSetMapper<RepairUnit> {

  @Override
  public RepairUnit map(int index, ResultSet rs, StatementContext ctx) throws SQLException {

    String[] columnFamilies = IStoragePostgreSql.parseStringArray(rs.getArray("column_families").getArray());

    String[] nodes = rs.getArray("nodes") == null
            ? new String[] {}
            : IStoragePostgreSql.parseStringArray(rs.getArray("nodes").getArray());

    String[] datacenters = rs.getArray("datacenters") == null
            ? new String[] {}
            : IStoragePostgreSql.parseStringArray(rs.getArray("datacenters").getArray());

    String[] blacklistedTables = rs.getArray("blacklisted_tables") == null
            ? new String[] {}
            : IStoragePostgreSql.parseStringArray(rs.getArray("blacklisted_tables").getArray());

    int segmentTimeout = rs.getInt("timeout") == 0
            ? 30
            : rs.getInt("timeout");

    RepairUnit.Builder builder = RepairUnit.builder()
            .clusterName(rs.getString("cluster_name"))
            .keyspaceName(rs.getString("keyspace_name"))
            .columnFamilies(Sets.newHashSet(columnFamilies))
            .incrementalRepair(rs.getBoolean("incremental_repair"))
            .nodes(Sets.newHashSet(nodes))
            .datacenters(Sets.newHashSet(datacenters))
            .blacklistedTables(Sets.newHashSet(blacklistedTables))
            .repairThreadCount(rs.getInt("repair_thread_count"))
            .timeout(segmentTimeout);

    return builder.build(UuidUtil.fromSequenceId(rs.getLong("id")));
  }
}
