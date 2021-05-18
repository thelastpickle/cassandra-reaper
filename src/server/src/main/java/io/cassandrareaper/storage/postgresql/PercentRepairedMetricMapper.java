/*
 * Copyright 2015-2017 Spotify AB
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

import io.cassandrareaper.core.PercentRepairedMetric;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

public final class PercentRepairedMetricMapper implements ResultSetMapper<PercentRepairedMetric> {

  @Override
  public PercentRepairedMetric map(int index, ResultSet rs, StatementContext ctx) throws SQLException {
    return PercentRepairedMetric.builder()
        .withCluster(rs.getString("cluster"))
        .withRepairScheduleId(UuidUtil.fromSequenceId(rs.getLong("repair_schedule_id")))
        .withNode(rs.getString("node"))
        .withKeyspaceName(rs.getString("keyspace_name"))
        .withTableName(rs.getString("table_name"))
        .withPercentRepaired(rs.getInt("percent_repaired"))
        .build();
  }
}