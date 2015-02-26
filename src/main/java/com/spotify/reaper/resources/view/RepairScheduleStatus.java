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
import com.spotify.reaper.core.RepairSchedule;
import com.spotify.reaper.core.RepairUnit;
import com.spotify.reaper.resources.CommonTools;

import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;

import java.util.Collection;

public class RepairScheduleStatus {

  @JsonProperty()
  private long id;

  @JsonProperty()
  private String owner;

  @JsonProperty("cluster_name")
  private String clusterName;

  @JsonProperty("column_families")
  private Collection<String> columnFamilies;

  @JsonProperty("keyspace_name")
  private String keyspaceName;

  @JsonProperty()
  private String state;

  @JsonIgnore
  private DateTime creationTime;

  @JsonIgnore
  private DateTime nextActivation;

  @JsonIgnore
  private DateTime pauseTime;

  @JsonProperty()
  private double intensity;

  @JsonProperty("segment_count")
  private int segmentCount;

  @JsonProperty("repair_parallelism")
  private String repairParallelism;

  @JsonProperty("scheduled_days_between")
  private int daysBetween;

  /**
   * Default public constructor Required for Jackson JSON parsing.
   */
  public RepairScheduleStatus() {
  }

  public RepairScheduleStatus(RepairSchedule repairSchedule, RepairUnit repairUnit) {
    this.id = repairSchedule.getId();
    this.state = repairSchedule.getState().name();
    this.owner = repairSchedule.getOwner();
    this.clusterName = repairUnit.getClusterName();
    this.columnFamilies = repairUnit.getColumnFamilies();
    this.keyspaceName = repairUnit.getKeyspaceName();
    this.creationTime = repairSchedule.getCreationTime();
    this.nextActivation = repairSchedule.getNextActivation();
    this.pauseTime = repairSchedule.getPauseTime();
    this.intensity = CommonTools.roundDoubleNicely(repairSchedule.getIntensity());
    this.segmentCount = repairSchedule.getSegmentCount();
    this.repairParallelism = repairSchedule.getRepairParallelism().name().toLowerCase();
    this.daysBetween = repairSchedule.getDaysBetween();
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public String getOwner() {
    return owner;
  }

  public void setOwner(String owner) {
    this.owner = owner;
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

  public String getState() {
    return state;
  }

  public void setState(String state) {
    this.state = state;
  }

  public DateTime getCreationTime() {
    return creationTime;
  }

  public void setCreationTime(DateTime creationTime) {
    this.creationTime = creationTime;
  }

  public DateTime getNextActivation() {
    return nextActivation;
  }

  public void setNextActivation(DateTime nextActivation) {
    this.nextActivation = nextActivation;
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

  public int getDaysBetween() {
    return daysBetween;
  }

  public void setDaysBetween(int daysBetween) {
    this.daysBetween = daysBetween;
  }

  @JsonProperty("creation_time")
  public String getCreationTimeISO8601() {
    return CommonTools.dateTimeToISO8601(creationTime);
  }

  @JsonProperty("creation_time")
  public void setCreationTimeISO8601(String dateStr) {
    if (null != dateStr) {
      creationTime = ISODateTimeFormat.dateTimeNoMillis().parseDateTime(dateStr);
    }
  }

  @JsonProperty("next_activation")
  public String getNextActivationISO8601() {
    return CommonTools.dateTimeToISO8601(nextActivation);
  }

  @JsonProperty("next_activation")
  public void setNextActivationISO8601(String dateStr) {
    if (null != dateStr) {
      nextActivation = ISODateTimeFormat.dateTimeNoMillis().parseDateTime(dateStr);
    }
  }

  @JsonProperty("pause_time")
  public String getPauseTimeISO8601() {
    return CommonTools.dateTimeToISO8601(pauseTime);
  }

  @JsonProperty("pause_time")
  public void setPauseTimeISO8601(String dateStr) {
    if (null != dateStr) {
      pauseTime = ISODateTimeFormat.dateTimeNoMillis().parseDateTime(dateStr);
    }
  }

}
