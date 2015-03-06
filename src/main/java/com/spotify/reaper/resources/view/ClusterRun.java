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
package com.spotify.reaper.resources.view;

import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.resources.CommonTools;
import com.spotify.reaper.storage.postgresql.RepairRunMapper;

import org.apache.commons.lang.time.DurationFormatUtils;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ClusterRun {

  public final int runId;
  public final String cluserName;
  public final String keyspaceName;
  public final String[] columnFamilies;
  public final int segmentsRepaired;
  public final int totalSegments;
  public final RepairRun.RunState state;
  private final DateTime startTime;
  public String getStartTime() {
    return CommonTools.dateTimeToISO8601(startTime);
  }
  private final Duration duration;
  public String getDuration() {
    if (null == duration) {
      return null;
    }
    return DurationFormatUtils.formatDurationWords(duration.getMillis(), false, false);
  }
  public final String cause;
  public final String owner;
  public final String lastEvent;
  private final DateTime estimatedTimeOfArrival;
  public String getEstimatedTimeOfArrival() {
    return CommonTools.dateTimeToISO8601(estimatedTimeOfArrival);
  }

  public ClusterRun(int runId, String clusterName, String keyspaceName, String[] columnFamilies,
      int segmentsRepaired, int totalSegments, RepairRun.RunState state, DateTime startTime,
      DateTime endTime, String cause, String owner, String lastEvent) {
    this.runId = runId;
    this.cluserName = clusterName;
    this.keyspaceName = keyspaceName;
    this.columnFamilies = columnFamilies;
    this.segmentsRepaired = segmentsRepaired;
    this.totalSegments = totalSegments;
    this.state = state;
    this.startTime = startTime;
    this.cause = cause;
    this.owner = owner;
    this.lastEvent = lastEvent;

    if (startTime == null) {
      estimatedTimeOfArrival = null;
      duration = null;
    } else if (endTime == null) {
      duration = null;
      if (state == RepairRun.RunState.ERROR || state == RepairRun.RunState.DELETED) {
        estimatedTimeOfArrival = null;
      } else {
        long now = DateTime.now().getMillis();
        estimatedTimeOfArrival = new DateTime(
            now + (now - startTime.getMillis()) /
                segmentsRepaired * (totalSegments - segmentsRepaired));
      }
    } else {
      estimatedTimeOfArrival = null;
      duration = new Duration(startTime.toInstant(), endTime.toInstant());
    }
  }

  public static class Mapper implements ResultSetMapper<ClusterRun> {

    @Override
    public ClusterRun map(int index, ResultSet r, StatementContext ctx) throws SQLException {
      int runId = r.getInt("id");
      String clusterName = r.getString("cluster_name");
      String keyspaceName = r.getString("keyspace_name");
      String[] columnFamilies = (String[]) r.getArray("column_families").getArray();
      int segmentsRepaired = (int)r.getLong("count");
      int totalSegments = r.getInt("segment_count");
      RepairRun.RunState state = RepairRun.RunState.valueOf(r.getString("state"));
      DateTime startTime = RepairRunMapper.getDateTimeOrNull(r, "start_time");
      DateTime endTime = RepairRunMapper.getDateTimeOrNull(r, "end_time");
      String cause = r.getString("cause");
      String owner = r.getString("owner");
      String lastEvent = r.getString("last_event");
      return new ClusterRun(runId, clusterName, keyspaceName, columnFamilies, segmentsRepaired,
          totalSegments, state, startTime, endTime, cause, owner, lastEvent);
    }
  }
}
