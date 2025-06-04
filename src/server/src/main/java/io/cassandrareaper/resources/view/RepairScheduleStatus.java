/*
 * Copyright 2014-2017 Spotify AB
 * Copyright 2016-2018 The Last Pickle Ltd
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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.cassandrareaper.core.RepairSchedule;
import io.cassandrareaper.core.RepairUnit;
import java.util.Collection;
import java.util.UUID;
import org.apache.cassandra.repair.RepairParallelism;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;

public final class RepairScheduleStatus {

  @JsonProperty private UUID id;

  @JsonProperty private String owner;

  @JsonProperty("cluster_name")
  private String clusterName;

  @JsonProperty("keyspace_name")
  private String keyspaceName;

  @JsonProperty("column_families")
  private Collection<String> columnFamilies;

  @JsonProperty private RepairSchedule.State state;

  @JsonIgnore private DateTime creationTime;

  @JsonIgnore private DateTime nextActivation;

  @JsonIgnore private DateTime pauseTime;

  @JsonProperty private double intensity;

  @JsonProperty("incremental_repair")
  private boolean incrementalRepair;

  @JsonProperty("subrange_incremental_repair")
  private boolean subrangeIncrementalRepair;

  @JsonProperty("repair_parallelism")
  private RepairParallelism repairParallelism;

  @JsonProperty("scheduled_days_between")
  private int daysBetween;

  @JsonProperty("nodes")
  private Collection<String> nodes;

  @JsonProperty("datacenters")
  private Collection<String> datacenters;

  @JsonProperty("blacklisted_tables")
  private Collection<String> blacklistedTables;

  @JsonProperty("segment_count_per_node")
  private int segmentCountPerNode;

  @JsonProperty("repair_thread_count")
  private int repairThreadCount;

  @JsonProperty("repair_unit_id")
  private UUID repairUnitId;

  @JsonProperty("segment_timeout")
  private int segmentTimeout;

  @JsonProperty("adaptive")
  private boolean adaptive;

  @JsonProperty("percent_unrepaired_threshold")
  private int percentUnrepairedThreshold;

  /** Default public constructor Required for Jackson JSON parsing. */
  public RepairScheduleStatus() {}

  public RepairScheduleStatus(
      UUID id,
      String owner,
      String clusterName,
      String keyspaceName,
      Collection<String> columnFamilies,
      RepairSchedule.State state,
      DateTime creationTime,
      DateTime nextActivation,
      DateTime pauseTime,
      double intensity,
      boolean incrementalRepair,
      boolean subrangeIncrementalRepair,
      RepairParallelism repairParallelism,
      int daysBetween,
      Collection<String> nodes,
      Collection<String> datacenters,
      Collection<String> blacklistedTables,
      int segmentCountPerNode,
      int repairThreadCount,
      UUID repairUnitId,
      int segmentTimeout,
      boolean adaptive,
      int percentUnrepairedThreshold) {

    this.id = id;
    this.owner = owner;
    this.clusterName = clusterName;
    this.keyspaceName = keyspaceName;
    this.columnFamilies = columnFamilies;
    this.state = state;
    this.creationTime = creationTime;
    this.nextActivation = nextActivation;
    this.pauseTime = pauseTime;
    this.intensity = RepairRunStatus.roundDoubleNicely(intensity);
    this.incrementalRepair = incrementalRepair;
    this.subrangeIncrementalRepair = subrangeIncrementalRepair;
    this.repairParallelism = repairParallelism;
    this.daysBetween = daysBetween;
    this.nodes = nodes;
    this.datacenters = datacenters;
    this.blacklistedTables = blacklistedTables;
    this.segmentCountPerNode = segmentCountPerNode;
    this.repairThreadCount = repairThreadCount;
    this.repairUnitId = repairUnitId;
    this.segmentTimeout = segmentTimeout;
    this.adaptive = adaptive;
    this.percentUnrepairedThreshold = percentUnrepairedThreshold;
  }

  public RepairScheduleStatus(RepairSchedule repairSchedule, RepairUnit repairUnit) {
    this(
        repairSchedule.getId(),
        repairSchedule.getOwner(),
        repairUnit.getClusterName(),
        repairUnit.getKeyspaceName(),
        repairUnit.getColumnFamilies(),
        repairSchedule.getState(),
        repairSchedule.getCreationTime(),
        repairSchedule.getNextActivation(),
        repairSchedule.getPauseTime(),
        repairSchedule.getIntensity(),
        repairUnit.getIncrementalRepair(),
        repairUnit.getSubrangeIncrementalRepair(),
        repairSchedule.getRepairParallelism(),
        repairSchedule.getDaysBetween(),
        repairUnit.getNodes(),
        repairUnit.getDatacenters(),
        repairUnit.getBlacklistedTables(),
        repairSchedule.getSegmentCountPerNode(),
        repairUnit.getRepairThreadCount(),
        repairUnit.getId(),
        repairUnit.getTimeout(),
        repairSchedule.getAdaptive(),
        repairSchedule.getPercentUnrepairedThreshold() == null
            ? -1
            : repairSchedule.getPercentUnrepairedThreshold());
  }

  public UUID getId() {
    return id;
  }

  public void setId(UUID id) {
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

  public RepairSchedule.State getState() {
    return state;
  }

  public void setState(RepairSchedule.State state) {
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

  public boolean getIncrementalRepair() {
    return incrementalRepair;
  }

  public void setIncrementalRepair(boolean incrementalRepair) {
    this.incrementalRepair = incrementalRepair;
  }

  public boolean getSubrangeIncrementalRepair() {
    return subrangeIncrementalRepair;
  }

  public void setSubrangeIncrementalRepair(boolean subrangeIncrementalRepair) {
    this.subrangeIncrementalRepair = subrangeIncrementalRepair;
  }

  public RepairParallelism getRepairParallelism() {
    return repairParallelism;
  }

  public void setRepairParallelism(RepairParallelism repairParallelism) {
    this.repairParallelism = repairParallelism;
  }

  public int getDaysBetween() {
    return daysBetween;
  }

  public void setDaysBetween(int daysBetween) {
    this.daysBetween = daysBetween;
  }

  @JsonProperty("creation_time")
  public String getCreationTimeIso8601() {
    return RepairRunStatus.dateTimeToIso8601(creationTime);
  }

  @JsonProperty("creation_time")
  public void setCreationTimeIso8601(String dateStr) {
    if (null != dateStr) {
      creationTime = ISODateTimeFormat.dateTimeNoMillis().parseDateTime(dateStr);
    }
  }

  @JsonProperty("next_activation")
  public String getNextActivationIso8601() {
    return RepairRunStatus.dateTimeToIso8601(nextActivation);
  }

  @JsonProperty("next_activation")
  public void setNextActivationIso8601(String dateStr) {
    if (null != dateStr) {
      nextActivation = ISODateTimeFormat.dateTimeNoMillis().parseDateTime(dateStr);
    }
  }

  @JsonProperty("pause_time")
  public String getPauseTimeIso8601() {
    return RepairRunStatus.dateTimeToIso8601(pauseTime);
  }

  @JsonProperty("pause_time")
  public void setPauseTimeIso8601(String dateStr) {
    if (null != dateStr) {
      pauseTime = ISODateTimeFormat.dateTimeNoMillis().parseDateTime(dateStr);
    }
  }

  @JsonProperty("nodes")
  public Collection<String> getNodes() {
    return nodes;
  }

  @JsonProperty("datacenters")
  public Collection<String> getDatacenters() {
    return datacenters;
  }

  @JsonProperty("nodes")
  public void setNodes(Collection<String> nodes) {
    this.nodes = nodes;
  }

  @JsonProperty("datacenters")
  public void setDatacenters(Collection<String> datacenters) {
    this.datacenters = datacenters;
  }

  public void setBlacklistedTables(Collection<String> blacklistedTables) {
    this.blacklistedTables = blacklistedTables;
  }

  @JsonProperty("blacklisted_tables")
  public Collection<String> getBlacklistedTables() {
    return this.blacklistedTables;
  }

  @JsonProperty("segment_count_per_node")
  public int getSegmentCountPerNode() {
    return segmentCountPerNode;
  }

  public void setSegmentCountPerNode(int segmentCountPerNode) {
    this.segmentCountPerNode = segmentCountPerNode;
  }

  @JsonProperty("repair_thread_count")
  public int getRepairThreadCount() {
    return repairThreadCount;
  }

  public void setRepairThreadCount(int repairThreadCount) {
    this.repairThreadCount = repairThreadCount;
  }

  @JsonProperty("repair_unit_id")
  public UUID getRepairUnitId() {
    return repairUnitId;
  }

  public void setRepairUnitId(UUID repairUnitId) {
    this.repairUnitId = repairUnitId;
  }

  public boolean getAdaptive() {
    return adaptive;
  }

  public void setAdaptive(boolean adaptive) {
    this.adaptive = adaptive;
  }

  public int getPercentUnrepairedThreshold() {
    return percentUnrepairedThreshold;
  }

  public void setPercentUnrepairedThreshold(int percentUnrepairedThreshold) {
    this.percentUnrepairedThreshold = percentUnrepairedThreshold;
  }
}
