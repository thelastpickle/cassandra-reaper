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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.reaper.core.ColumnFamily;
import com.spotify.reaper.core.RepairRun;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * Contains the data to be shown when querying repair run status.
 */
public class RepairRunStatus {

  public static final String TIMESTAMP_ISO8601_YODA_TEMPLATE = "YYYY-MM-dd'T'HH:mm:ss'Z'";

  private final String cause;

  private final String owner;

  @JsonProperty("cluster_name")
  private final String clusterName;

  @JsonProperty("table_name")
  private final String columnFamilyName;

  @JsonProperty("keyspace_name")
  private final String keyspaceName;

  @JsonProperty("run_state")
  private final String runState;

  @JsonIgnore
  private final DateTime creationTime;

  @JsonIgnore
  private final DateTime startTime;

  @JsonIgnore
  private final DateTime endTime;

  private final double intensity;

  @JsonProperty("segment_count")
  private final int segmentCount;

  @JsonProperty("segments_repaired")
  private int segmentsRepaired = 0;

  public RepairRunStatus(RepairRun repairRun, ColumnFamily columnFamily) {
    this.cause = repairRun.getCause();
    this.owner = repairRun.getOwner();
    this.clusterName = repairRun.getClusterName();
    this.columnFamilyName = columnFamily.getName();
    this.keyspaceName = columnFamily.getKeyspaceName();
    this.runState = repairRun.getRunState().name();
    this.creationTime = repairRun.getCreationTime();
    this.startTime = repairRun.getStartTime();
    this.endTime = repairRun.getEndTime();
    this.intensity = repairRun.getIntensity();
    this.segmentCount = columnFamily.getSegmentCount();
  }

  @JsonProperty("creation_time")
  public String getCreationTimeISO8601() {
    if (creationTime == null) {
      return null;
    }
    return creationTime.toDateTime(DateTimeZone.UTC).toString(TIMESTAMP_ISO8601_YODA_TEMPLATE);
  }

  @JsonProperty("start_time")
  public String getStartTimeISO8601() {
    if (startTime == null) {
      return null;
    }
    return startTime.toDateTime(DateTimeZone.UTC).toString(TIMESTAMP_ISO8601_YODA_TEMPLATE);
  }

  @JsonProperty("end_time")
  public String getEndTimeISO8601() {
    if (endTime == null) {
      return null;
    }
    return endTime.toDateTime(DateTimeZone.UTC).toString(TIMESTAMP_ISO8601_YODA_TEMPLATE);
  }

  public void setSegmentsRepaired(int segmentsRepaired) {
    this.segmentsRepaired = segmentsRepaired;
  }
}
