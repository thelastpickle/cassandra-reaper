package com.spotify.reaper.storage.postgresql;

import com.google.common.collect.ImmutableSet;

import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.resources.view.RepairRunStatus;

import com.spotify.reaper.storage.PostgresStorage;
import org.apache.cassandra.repair.RepairParallelism;
import org.joda.time.DateTime;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

public class RepairRunStatusMapper implements ResultSetMapper<RepairRunStatus> {

  @Override
  public RepairRunStatus map(int index, ResultSet r, StatementContext ctx) throws SQLException {
    long runId = r.getLong("id");
    String clusterName = r.getString("cluster_name");
    String keyspaceName = r.getString("keyspace_name");
    Collection<String> columnFamilies =
        ImmutableSet.copyOf((String[]) r.getArray("column_families").getArray());
    int segmentsRepaired = r.getInt("segments_repaired");
    int totalSegments = r.getInt("segments_total");
    RepairRun.RunState state = RepairRun.RunState.valueOf(r.getString("state"));
    DateTime startTime = RepairRunMapper.getDateTimeOrNull(r, "start_time");
    DateTime endTime = RepairRunMapper.getDateTimeOrNull(r, "end_time");
    String cause = r.getString("cause");
    String owner = r.getString("owner");
    String lastEvent = r.getString("last_event");
    DateTime creationTime = RepairRunMapper.getDateTimeOrNull(r, "creation_time");
    DateTime pauseTime = RepairRunMapper.getDateTimeOrNull(r, "pause_time");
    Double intensity = r.getDouble("intensity");
    Boolean incrementalRepair = r.getBoolean("incremental_repair");
    RepairParallelism repairParallelism =
        RepairParallelism.fromName(r.getString("repair_parallelism").toLowerCase().replace("datacenter_aware", "dc_parallel"));

    return new RepairRunStatus(PostgresStorage.fromSequenceId(runId), clusterName, keyspaceName, columnFamilies, segmentsRepaired,
        totalSegments, state, startTime, endTime, cause, owner, lastEvent,
        creationTime, pauseTime, intensity, incrementalRepair, repairParallelism);
  }
}
