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

import org.apache.cassandra.repair.RepairParallelism;
import org.hibernate.validator.constraints.NotEmpty;

import java.util.Map;

import javax.validation.Valid;
import javax.validation.constraints.DecimalMin;
import javax.validation.constraints.Max;
import javax.validation.constraints.NotNull;

import io.dropwizard.Configuration;
import io.dropwizard.db.DataSourceFactory;

public class ReaperApplicationConfiguration extends Configuration {

  @JsonProperty
  @NotNull
  private Integer segmentCount;

  @JsonProperty
  @NotNull
  private RepairParallelism repairParallelism;

  @JsonProperty
  @NotNull
  @DecimalMin(value = "0", inclusive=false)
  @Max(1)
  private Double repairIntensity;

  @JsonProperty
  @NotNull
  private Integer repairRunThreadCount;

  @JsonProperty
  @NotNull
  private Integer hangingRepairTimeoutMins;

  @NotEmpty
  private String storageType;

  @Valid
  @NotNull
  @JsonProperty
  private DataSourceFactory database = new DataSourceFactory();

  @JsonProperty
  private Map<String, Integer> jmxPorts;


  public int getSegmentCount() {
    return segmentCount;
  }

  public void setSegmentCount(int segmentCount) {
    this.segmentCount = segmentCount;
  }

  public RepairParallelism getRepairParallelism() {
    return repairParallelism;
  }

  public void setRepairParallelism(RepairParallelism repairParallelism) {
    this.repairParallelism = repairParallelism;
  }

  public double getRepairIntensity() {
    return repairIntensity;
  }

  public void setRepairIntensity(double repairIntensity) {
    this.repairIntensity = repairIntensity;
  }

  public int getRepairRunThreadCount() {
    return repairRunThreadCount;
  }

  public void setRepairRunThreadCount(int repairRunThreadCount) {
    this.repairRunThreadCount = repairRunThreadCount;
  }

  public String getStorageType() {
    return storageType;
  }

  public void setStorageType(String storageType) {
    this.storageType = storageType;
  }

  public DataSourceFactory getDataSourceFactory() {
    return database;
  }

  public void setDataSourceFactory(DataSourceFactory database) {
    this.database = database;
  }

  public int getHangingRepairTimeoutMins() {
    return hangingRepairTimeoutMins;
  }

  @JsonProperty
  public void setHangingRepairTimeoutMins(int hangingRepairTimeoutMins) {
    this.hangingRepairTimeoutMins = hangingRepairTimeoutMins;
  }

  public Map<String, Integer> getJmxPorts() {
    return jmxPorts;
  }

  public void setJmxPorts(Map<String, Integer> jmxPorts) {
    this.jmxPorts = jmxPorts;
  }

}
