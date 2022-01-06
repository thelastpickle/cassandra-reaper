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

package io.cassandrareaper.resources.view;

import io.cassandrareaper.core.RepairRun;
import io.cassandrareaper.core.RepairUnit;

import java.util.Collection;
import java.util.UUID;

import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.format.ISODateTimeFormat;

/**
 * Contains the data to be shown when querying repair run status.
 */
public final class RepairRunStatus {

  @JsonProperty
  private String cause;

  @JsonProperty
  private String owner;

  @JsonProperty
  private UUID id;

  @JsonProperty("cluster_name")
  private String clusterName;

  @JsonProperty("column_families")
  private Collection<String> columnFamilies;

  @JsonProperty("keyspace_name")
  private String keyspaceName;

  @JsonProperty
  private RepairRun.RunState state;

  @JsonIgnore
  private DateTime creationTime;

  @JsonIgnore
  private DateTime startTime;

  @JsonIgnore
  private DateTime endTime;

  @JsonIgnore
  private DateTime pauseTime;

  @JsonIgnore private DateTime currentTime;

  @JsonProperty
  private double intensity;

  @JsonProperty("incremental_repair")
  private boolean incrementalRepair;

  @JsonProperty("total_segments")
  private int totalSegments;

  @JsonProperty("repair_parallelism")
  private RepairParallelism repairParallelism;

  @JsonProperty("segments_repaired")
  private int segmentsRepaired;

  @JsonProperty("last_event")
  private String lastEvent;

  @JsonProperty
  private String duration;

  @JsonIgnore
  private DateTime estimatedTimeOfArrival;

  @JsonProperty("nodes")
  private Collection<String> nodes;

  @JsonProperty("datacenters")
  private Collection<String> datacenters;

  @JsonProperty("blacklisted_tables")
  private Collection<String> blacklistedTables;

  @JsonProperty("repair_thread_count")
  private int repairThreadCount;

  @JsonProperty("repair_unit_id")
  private UUID repairUnitId;

  @JsonProperty("segment_timeout")
  private int segmentTimeout;

  @JsonProperty("adaptive_schedule")
  private boolean adaptiveSchedule;


  /**
   * Default public constructor Required for Jackson JSON parsing.
   */
  public RepairRunStatus() {
  }

  public RepairRunStatus(
      UUID runId,
      String clusterName,
      String keyspaceName,
      Collection<String> columnFamilies,
      int segmentsRepaired,
      int totalSegments,
      RepairRun.RunState state,
      DateTime startTime,
      DateTime endTime,
      String cause,
      String owner,
      String lastEvent,
      DateTime creationTime,
      DateTime pauseTime,
      double intensity,
      boolean incrementalRepair,
      RepairParallelism repairParallelism,
      Collection<String> nodes,
      Collection<String> datacenters,
      Collection<String> blacklistedTables,
      int repairThreadCount,
      UUID repairUnitId,
      int segmentTimeout,
      boolean adaptiveSchedule) {

    this.id = runId;
    this.cause = cause;
    this.owner = owner;
    this.clusterName = clusterName;
    this.columnFamilies = columnFamilies;
    this.keyspaceName = keyspaceName;
    this.state = state;
    this.creationTime = creationTime;
    this.startTime = startTime;
    this.endTime = endTime;
    this.pauseTime = pauseTime;
    this.currentTime = DateTime.now();
    this.intensity = roundDoubleNicely(intensity);
    this.incrementalRepair = incrementalRepair;
    this.totalSegments = totalSegments;
    this.repairParallelism = repairParallelism;
    this.segmentsRepaired = segmentsRepaired;
    this.lastEvent = lastEvent;

    this.nodes = nodes;
    this.datacenters = datacenters;
    this.blacklistedTables = blacklistedTables;
    this.repairThreadCount = repairThreadCount;
    this.segmentTimeout = segmentTimeout;
    this.adaptiveSchedule = adaptiveSchedule;

    if (startTime == null) {
      duration = null;
    } else {
      if (state == RepairRun.RunState.RUNNING || state == RepairRun.RunState.PAUSED) {
        duration = DurationFormatUtils.formatDurationWords(
                new Duration(startTime.toInstant(), currentTime.toInstant()).getMillis(),
                true,
                false);
      } else if (endTime != null) {
        duration = DurationFormatUtils.formatDurationWords(
                new Duration(startTime.toInstant(), endTime.toInstant()).getMillis(), true, false);
      } else {
        duration = null;
      }
    }

    if (startTime == null) {
      estimatedTimeOfArrival = null;
    } else {
      if (state == RepairRun.RunState.ERROR
          || state == RepairRun.RunState.DELETED
          || state == RepairRun.RunState.ABORTED
          || segmentsRepaired == 0) {
        estimatedTimeOfArrival = null;
      } else {
        long now = DateTime.now().getMillis();
        long currentDuration = now - startTime.getMillis();
        long millisecondsPerSegment = currentDuration / segmentsRepaired;
        int segmentsLeft = totalSegments - segmentsRepaired;
        estimatedTimeOfArrival = new DateTime(now + millisecondsPerSegment * segmentsLeft);
      }
    }

    this.repairUnitId = repairUnitId;
  }

  public RepairRunStatus(RepairRun repairRun, RepairUnit repairUnit, int segmentsRepaired) {
    this(
        repairRun.getId(),
        repairRun.getClusterName(),
        repairUnit.getKeyspaceName(),
        repairRun.getTables(),
        segmentsRepaired,
        repairRun.getSegmentCount(),
        repairRun.getRunState(),
        repairRun.getStartTime(),
        repairRun.getEndTime(),
        repairRun.getCause(),
        repairRun.getOwner(),
        repairRun.getLastEvent(),
        repairRun.getCreationTime(),
        repairRun.getPauseTime(),
        repairRun.getIntensity(),
        repairUnit.getIncrementalRepair(),
        repairRun.getRepairParallelism(),
        repairUnit.getNodes(),
        repairUnit.getDatacenters(),
        repairUnit.getBlacklistedTables(),
        repairUnit.getRepairThreadCount(),
        repairRun.getRepairUnitId(),
        repairUnit.getTimeout(),
        repairRun.getAdaptiveSchedule());
  }

  @JsonProperty("creation_time")
  public String getCreationTimeIso8601() {
    return dateTimeToIso8601(creationTime);
  }

  @JsonProperty("creation_time")
  public void setCreationTimeIso8601(String dateStr) {
    if (null != dateStr) {
      creationTime = ISODateTimeFormat.dateTimeNoMillis().parseDateTime(dateStr);
    }
  }

  @JsonProperty("start_time")
  public String getStartTimeIso8601() {
    return dateTimeToIso8601(startTime);
  }

  @JsonProperty("start_time")
  public void setStartTimeIso8601(String dateStr) {
    if (null != dateStr) {
      startTime = ISODateTimeFormat.dateTimeNoMillis().parseDateTime(dateStr);
    }
  }

  @JsonProperty("end_time")
  public String getEndTimeIso8601() {
    return dateTimeToIso8601(endTime);
  }

  @JsonProperty("end_time")
  public void setEndTimeIso8601(String dateStr) {
    if (null != dateStr) {
      endTime = ISODateTimeFormat.dateTimeNoMillis().parseDateTime(dateStr);
    }
  }

  @JsonProperty("pause_time")
  public String getPauseTimeIso8601() {
    return dateTimeToIso8601(pauseTime);
  }

  @JsonProperty("pause_time")
  public void setPauseTimeIso8601(String dateStr) {
    if (null != dateStr) {
      pauseTime = ISODateTimeFormat.dateTimeNoMillis().parseDateTime(dateStr);
    }
  }

  @JsonProperty("current_time")
  public void setCurrentTimeIso8601(String dateStr) {
    if (null != dateStr) {
      currentTime = ISODateTimeFormat.dateTimeNoMillis().parseDateTime(dateStr);
    }
  }

  @JsonProperty("current_time")
  public String getCurrentTimeIso8601() {
    return dateTimeToIso8601(currentTime);
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

  public UUID getId() {
    return id;
  }

  public void setId(UUID id) {
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

  public RepairRun.RunState getState() {
    return state;
  }

  public void setState(RepairRun.RunState runState) {
    this.state = runState;
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

  public DateTime getCurrentTime() {
    return currentTime;
  }

  public void setCurrentTime(DateTime currentTime) {
    this.currentTime = currentTime;
  }

  public double getIntensity() {
    return intensity;
  }

  public void setIntensity(double intensity) {
    this.intensity = intensity;
  }

  public boolean getIncrementalRepair() {
    return incrementalRepair;
  }

  public void setIncrementalRepair(boolean incrementalRepair) {
    this.incrementalRepair = incrementalRepair;
  }

  public int getTotalSegments() {
    return totalSegments;
  }

  public void setTotalSegments(int segmentCount) {
    this.totalSegments = segmentCount;
  }

  public RepairParallelism getRepairParallelism() {
    return repairParallelism;
  }

  public void setRepairParallelism(RepairParallelism repairParallelism) {
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

  public String getDuration() {
    return duration;
  }

  public void setDuration(String duration) {
    this.duration = duration;
  }

  @JsonProperty("estimated_time_of_arrival")
  public String getEstimatedTimeOfArrivalIso8601() {
    return dateTimeToIso8601(estimatedTimeOfArrival);
  }

  @JsonProperty("estimated_time_of_arrival")
  public void setEstimatedTimeOfArrivalIso8601(String dateStr) {
    if (null != dateStr) {
      estimatedTimeOfArrival = ISODateTimeFormat.dateTimeNoMillis().parseDateTime(dateStr);
    }
  }

  public Collection<String> getNodes() {
    return nodes;
  }

  public void setNodes(Collection<String> nodes) {
    this.nodes = nodes;
  }

  public Collection<String> getDatacenters() {
    return datacenters;
  }

  public void setDatacenters(Collection<String> datacenters) {
    this.datacenters = datacenters;
  }

  @JsonProperty("blacklisted_tables")
  public Collection<String> getBlacklistedTables() {
    return blacklistedTables;
  }

  public void setBlacklistedTables(Collection<String> blacklistedTables) {
    this.blacklistedTables = blacklistedTables;
  }


  public int getRepairThreadCount() {
    return repairThreadCount;
  }

  public void setRepairThreadCount(int repairThreadCount) {
    this.repairThreadCount = repairThreadCount;
  }

  static double roundDoubleNicely(double intensity) {
    return Math.round(intensity * 10000f) / 10000f;
  }

  @Nullable
  public static String dateTimeToIso8601(@Nullable DateTime dateTime) {
    return null != dateTime ? ISODateTimeFormat.dateTimeNoMillis().print(dateTime) : null;
  }

  public UUID getRepairUnitId() {
    return repairUnitId;
  }

  public void setRepairUnitId(UUID repairUnitId) {
    this.repairUnitId = repairUnitId;
  }

  public boolean getAdaptiveSchedule() {
    return adaptiveSchedule;
  }

  public void setAdaptiveSchedule(boolean adaptiveSchedule) {
    this.adaptiveSchedule = adaptiveSchedule;
  }
}
