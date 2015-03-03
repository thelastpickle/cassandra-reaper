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
import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.core.RepairSegment;
import com.spotify.reaper.core.RepairUnit;
import com.spotify.reaper.resources.CommonTools;

import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;

import java.util.Collection;

/**
 * Contains the data to be shown when querying repair run status.
 */
public class RepairRunStatus {

  @JsonProperty
  private String cause;

  @JsonProperty
  private String owner;

  @JsonProperty
  private long id;

  @JsonProperty("cluster_name")
  private String clusterName;

  @JsonProperty("column_families")
  private Collection<String> columnFamilies;

  @JsonProperty("keyspace_name")
  private String keyspaceName;

  @JsonProperty("run_state")
  private String runState;

  @JsonIgnore
  private DateTime creationTime;

  @JsonIgnore
  private DateTime startTime;

  @JsonIgnore
  private DateTime endTime;

  @JsonIgnore
  private DateTime pauseTime;

  @JsonProperty
  private double intensity;

  @JsonProperty("segment_count")
  private int segmentCount;

  @JsonProperty("repair_parallelism")
  private String repairParallelism;

  @JsonProperty("segments_repaired")
  private int segmentsRepaired;

  @JsonProperty("last_event")
  private String lastEvent;

  /**
   * Default public constructor Required for Jackson JSON parsing.
   */
  public RepairRunStatus() {
  }

  public RepairRunStatus(RepairRun repairRun, RepairUnit repairUnit, int segmentsRepaired) {
    this.id = repairRun.getId();
    this.cause = repairRun.getCause();
    this.owner = repairRun.getOwner();
    this.clusterName = repairRun.getClusterName();
    this.columnFamilies = repairUnit.getColumnFamilies();
    this.keyspaceName = repairUnit.getKeyspaceName();
    this.runState = repairRun.getRunState().name();
    this.creationTime = repairRun.getCreationTime();
    this.startTime = repairRun.getStartTime();
    this.endTime = repairRun.getEndTime();
    this.pauseTime = repairRun.getPauseTime();
    this.intensity = CommonTools.roundDoubleNicely(repairRun.getIntensity());
    this.segmentCount = repairRun.getSegmentCount();
    this.repairParallelism = repairRun.getRepairParallelism().name().toLowerCase();
    this.segmentsRepaired = segmentsRepaired;
    this.lastEvent = repairRun.getLastEvent();
  }

  @JsonProperty("creation_time")
  public String getCreationTimeISO8601() {
    if (creationTime == null) {
      return null;
    }
    return CommonTools.dateTimeToISO8601(creationTime);
  }

  @JsonProperty("creation_time")
  public void setCreationTimeISO8601(String dateStr) {
    if (null != dateStr) {
      creationTime = ISODateTimeFormat.dateTimeNoMillis().parseDateTime(dateStr);
    }
  }

  @JsonProperty("start_time")
  public String getStartTimeISO8601() {
    if (startTime == null) {
      return null;
    }
    return CommonTools.dateTimeToISO8601(startTime);
  }

  @JsonProperty("start_time")
  public void setStartTimeISO8601(String dateStr) {
    if (null != dateStr) {
      startTime = ISODateTimeFormat.dateTimeNoMillis().parseDateTime(dateStr);
    }
  }

  @JsonProperty("end_time")
  public String getEndTimeISO8601() {
    if (endTime == null) {
      return null;
    }
    return CommonTools.dateTimeToISO8601(endTime);
  }

  @JsonProperty("end_time")
  public void setEndTimeISO8601(String dateStr) {
    if (null != dateStr) {
      endTime = ISODateTimeFormat.dateTimeNoMillis().parseDateTime(dateStr);
    }
  }

  @JsonProperty("pause_time")
  public String getPauseTimeISO8601() {
    if (pauseTime == null) {
      return null;
    }
    return CommonTools.dateTimeToISO8601(pauseTime);
  }

  @JsonProperty("pause_time")
  public void setPauseTimeISO8601(String dateStr) {
    if (null != dateStr) {
      pauseTime = ISODateTimeFormat.dateTimeNoMillis().parseDateTime(dateStr);
    }
  }

  public String getCause() {
    return cause;
  }

  public void setCause(String cause) {
    this.cause = cause;
  }

  public String getOwner() {
    return owner;
  }

  public void setOwner(String owner) {
    this.owner = owner;
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public String getClusterName() {
    return clusterName;
  }

  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }

  public Collection<String> getColumnFamilies() {
    return columnFamilies;
  }

  public void setColumnFamilies(Collection<String> columnFamilies) {
    this.columnFamilies = columnFamilies;
  }

  public String getKeyspaceName() {
    return keyspaceName;
  }

  public void setKeyspaceName(String keyspaceName) {
    this.keyspaceName = keyspaceName;
  }

  public String getRunState() {
    return runState;
  }

  public void setRunState(String runState) {
    this.runState = runState;
  }

  public DateTime getCreationTime() {
    return creationTime;
  }

  public void setCreationTime(DateTime creationTime) {
    this.creationTime = creationTime;
  }

  public DateTime getStartTime() {
    return startTime;
  }

  public void setStartTime(DateTime startTime) {
    this.startTime = startTime;
  }

  public DateTime getEndTime() {
    return endTime;
  }

  public void setEndTime(DateTime endTime) {
    this.endTime = endTime;
  }

  public DateTime getPauseTime() {
    return pauseTime;
  }

  public void setPauseTime(DateTime pauseTime) {
    this.pauseTime = pauseTime;
  }

  public double getIntensity() {
    return intensity;
  }

  public void setIntensity(double intensity) {
    this.intensity = intensity;
  }

  public int getSegmentCount() {
    return segmentCount;
  }

  public void setSegmentCount(int segmentCount) {
    this.segmentCount = segmentCount;
  }

  public String getRepairParallelism() {
    return repairParallelism;
  }

  public void setRepairParallelism(String repairParallelism) {
    this.repairParallelism = repairParallelism;
  }

  public int getSegmentsRepaired() {
    return segmentsRepaired;
  }

  public void setSegmentsRepaired(int segmentsRepaired) {
    this.segmentsRepaired = segmentsRepaired;
  }

  public String getLastEvent() {
    return lastEvent;
  }

  public void setLastEvent(String lastEvent) {
    this.lastEvent = lastEvent;
  }
}
