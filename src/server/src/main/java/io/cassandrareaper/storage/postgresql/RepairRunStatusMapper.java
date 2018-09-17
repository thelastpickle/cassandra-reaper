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

import io.cassandrareaper.core.RepairRun;
import io.cassandrareaper.resources.view.RepairRunStatus;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.cassandra.repair.RepairParallelism;
import org.joda.time.DateTime;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;


public final class RepairRunStatusMapper implements ResultSetMapper<RepairRunStatus> {

  @Override
  public RepairRunStatus map(int index, ResultSet rs, StatementContext ctx) throws SQLException {
    long runId = rs.getLong("id");
    String clusterName = rs.getString("cluster_name");
    String keyspaceName = rs.getString("keyspace_name");

    Collection<String> columnFamilies
        = ImmutableSet.copyOf(getStringArray(rs.getArray("column_families").getArray()));

    int segmentsRepaired = rs.getInt("segments_repaired");
    int totalSegments = rs.getInt("segments_total");
    RepairRun.RunState state = RepairRun.RunState.valueOf(rs.getString("state"));
    DateTime startTime = RepairRunMapper.getDateTimeOrNull(rs, "start_time");
    DateTime endTime = RepairRunMapper.getDateTimeOrNull(rs, "end_time");
    String cause = rs.getString("cause");
    String owner = rs.getString("owner");
    String lastEvent = rs.getString("last_event");
    DateTime creationTime = RepairRunMapper.getDateTimeOrNull(rs, "creation_time");
    DateTime pauseTime = RepairRunMapper.getDateTimeOrNull(rs, "pause_time");
    Double intensity = rs.getDouble("intensity");
    Boolean incrementalRepair = rs.getBoolean("incremental_repair");
    RepairParallelism repairParallelism = RepairParallelism.fromName(
        rs.getString("repair_parallelism").toLowerCase().replace("datacenter_aware", "dc_parallel"));

    Collection<String> nodes = ImmutableSet.copyOf(
            rs.getArray("nodes") == null
                ? new String[] {}
                : getStringArray(rs.getArray("nodes").getArray()));

    Collection<String> datacenters = ImmutableSet.copyOf(
            getStringArray(
                rs.getArray("datacenters") == null
                    ? new String[] {}
                    : rs.getArray("datacenters").getArray()));

    Collection<String> blacklistedTables = ImmutableSet.copyOf(
            getStringArray(
                rs.getArray("blacklisted_tables") == null
                    ? new String[] {}
                    : rs.getArray("blacklisted_tables").getArray()));

    int repairThreadCount = rs.getInt("repair_thread_count");

    return new RepairRunStatus(
        UuidUtil.fromSequenceId(runId),
        clusterName,
        keyspaceName,
        columnFamilies,
        segmentsRepaired,
        totalSegments,
        state,
        startTime,
        endTime,
        cause,
        owner,
        lastEvent,
        creationTime,
        pauseTime,
        intensity,
        incrementalRepair,
        repairParallelism,
        nodes,
        datacenters,
        blacklistedTables,
        repairThreadCount);
  }

  private String[] getStringArray(Object array) {
    String[] stringArray = new String[((Object[]) array).length];
    return Lists.newArrayList(((Object[]) array))
        .stream()
        .map(element -> (String) element)
        .collect(Collectors.toList())
        .toArray(stringArray);
  }
}
