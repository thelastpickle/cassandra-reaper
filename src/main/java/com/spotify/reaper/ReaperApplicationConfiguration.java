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
package com.spotify.reaper;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import io.dropwizard.Configuration;
import io.dropwizard.db.DataSourceFactory;

public class ReaperApplicationConfiguration extends Configuration {

  private int segmentCount;

  private boolean snapshotRepair;

  private double repairIntensity;

  private int repairRunThreadCount;

  private int hangingRepairTimeoutMins;

  @NotEmpty
  private String storageType;

  @Valid
  @NotNull
  @JsonProperty
  private DataSourceFactory database = new DataSourceFactory();

  @JsonProperty
  public int getSegmentCount() {
    return segmentCount;
  }

  @JsonProperty
  public void setSegmentCount(int segmentCount) {
    this.segmentCount = segmentCount;
  }

  @JsonProperty
  public boolean getSnapshotRepair() {
    return snapshotRepair;
  }

  @JsonProperty
  public void setSnapshotRepair(boolean snapshotRepair) {
    this.snapshotRepair = snapshotRepair;
  }


  @JsonProperty
  public double getRepairIntensity() {
    return repairIntensity;
  }

  @JsonProperty
  public void setRepairIntensity(double repairIntensity) {
    this.repairIntensity = repairIntensity;
  }

  @JsonProperty
  public int getRepairRunThreadCount() {
    return repairRunThreadCount;
  }

  @JsonProperty
  public void setRepairRunThreadCount(int repairRunThreadCount) {
    this.repairRunThreadCount = repairRunThreadCount;
  }

  @JsonProperty
  public void setStorageType(String storageType) {
    this.storageType = storageType;
  }

  @JsonProperty
  public String getStorageType() {
    return storageType;
  }

  @JsonProperty
  public DataSourceFactory getDataSourceFactory() {
    return database;
  }

  @JsonProperty
  public int getHangingRepairTimeoutMins() {
    return hangingRepairTimeoutMins;
  }

  @JsonProperty
  public void setHangingRepairTimeoutMins(int hangingRepairTimeoutMins) {
    this.hangingRepairTimeoutMins = hangingRepairTimeoutMins;
  }
}
