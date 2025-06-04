/*
 * Copyright 2021-2021 DataStax, Inc.
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

package io.cassandrareaper.core;

import io.cassandrareaper.validators.NullOrNotBlank;
import io.cassandrareaper.validators.ValidEditableRepairSchedule;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.repair.RepairParallelism;

@ValidEditableRepairSchedule
public class EditableRepairSchedule {
  @NullOrNotBlank protected String owner;

  @JsonProperty(value = "repair_parallelism")
  protected RepairParallelism repairParallelism;

  @Min(value = 0)
  @Max(value = 1)
  protected Double intensity;

  @JsonProperty(value = "scheduled_days_between")
  @Min(value = 0)
  @Max(value = 31)
  protected Integer daysBetween;

  @JsonProperty(value = "segment_count_per_node")
  @Min(value = 0)
  @Max(value = 1000)
  protected Integer segmentCountPerNode;

  @JsonProperty(value = "percent_unrepaired_threshold")
  @Min(value = -1)
  @Max(value = 99)
  protected Integer percentUnrepairedThreshold;

  protected Boolean adaptive;

  public EditableRepairSchedule() {
    this.owner = null;
    this.repairParallelism = null;
    this.intensity = null;
    this.daysBetween = null;
    this.segmentCountPerNode = null;
    this.percentUnrepairedThreshold = null;
    this.adaptive = null;
  }

  public String getOwner() {
    return owner;
  }

  public void setOwner(String owner) {
    this.owner = owner;
  }

  public RepairParallelism getRepairParallelism() {
    return repairParallelism;
  }

  public void setRepairParallelism(RepairParallelism repairParallelism) {
    this.repairParallelism = repairParallelism;
  }

  public Double getIntensity() {
    return intensity;
  }

  public void setIntensity(Double intensity) {
    this.intensity = intensity;
  }

  public Integer getDaysBetween() {
    return daysBetween;
  }

  public void setDaysBetween(Integer daysBetween) {
    this.daysBetween = daysBetween;
  }

  public Integer getSegmentCountPerNode() {
    return segmentCountPerNode;
  }

  public void setSegmentCountPerNode(Integer segmentCountPerNode) {
    this.segmentCountPerNode = segmentCountPerNode;
  }

  public Integer getPercentUnrepairedThreshold() {
    return percentUnrepairedThreshold == null ? -1 : percentUnrepairedThreshold;
  }

  public void setPercentUnrepairedThreshold(Integer percentUnrepairedThreshold) {
    this.percentUnrepairedThreshold = percentUnrepairedThreshold;
  }

  public Boolean getAdaptive() {
    return adaptive;
  }

  public void setAdaptive(boolean adaptive) {
    this.adaptive = adaptive;
  }
}
