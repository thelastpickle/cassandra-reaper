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

import com.google.common.annotations.VisibleForTesting;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.reaper.core.RepairSchedule;
import com.spotify.reaper.core.RepairUnit;
import com.spotify.reaper.resources.CommonTools;

import org.joda.time.DateTime;

import java.util.Collection;

public class RepairScheduleStatus {

  @JsonProperty
  private final long id;

  @JsonProperty
  private final String owner;

  @JsonProperty("cluster_name")
  private final String clusterName;

  @JsonProperty("column_families")
  private final Collection<String> columnFamilies;

  @JsonProperty("keyspace_name")
  private final String keyspaceName;

  @JsonProperty("state")
  private final String state;

  @JsonIgnore
  private final DateTime creationTime;

  @JsonIgnore
  private final DateTime nextActivation;

  @JsonIgnore
  private final DateTime pauseTime;

  @JsonProperty
  private final double intensity;

  @JsonProperty("segment_count")
  private final int segmentCount;

  @JsonProperty("repair_parallelism")
  private final String repairParallelism;

  @JsonProperty("schedule_days_between")
  private final int daysBetween;

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
    this.intensity = roundIntensity(repairSchedule.getIntensity());
    this.segmentCount = repairSchedule.getSegmentCount();
    this.repairParallelism = repairSchedule.getRepairParallelism().name().toLowerCase();
    this.daysBetween = repairSchedule.getDaysBetween();
  }

  @VisibleForTesting
  protected static double roundIntensity(double intensity) {
    return Math.round(intensity * 10000f) / 10000f;
  }

  @JsonProperty("creation_time")
  public String getCreationTimeISO8601() {
    if (creationTime == null) {
      return null;
    }
    return CommonTools.dateTimeToISO8601(creationTime);
  }

  @JsonProperty("next_activation")
  public String getNextActivationISO8601() {
    if (nextActivation == null) {
      return null;
    }
    return CommonTools.dateTimeToISO8601(nextActivation);
  }

  @JsonProperty("pause_time")
  public String getPauseTimeISO8601() {
    if (pauseTime == null) {
      return null;
    }
    return CommonTools.dateTimeToISO8601(pauseTime);
  }

  public long getId() {
    return this.id;
  }

  public String getState() {
    return this.state;
  }

}
