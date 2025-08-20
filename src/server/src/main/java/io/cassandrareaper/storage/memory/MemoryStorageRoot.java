/*
 * Copyright 2024-2024 DataStax, Inc.
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

package io.cassandrareaper.storage.memory;

import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.RepairRun;
import io.cassandrareaper.core.RepairSchedule;
import io.cassandrareaper.core.RepairSegment;
import io.cassandrareaper.core.RepairUnit;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class MemoryStorageRoot {
  private final ConcurrentMap<UUID, RepairSegment> repairSegments = new ConcurrentHashMap<>();
  private final ConcurrentMap<UUID, RepairUnit> repairUnits = new ConcurrentHashMap<>();
  private final ConcurrentMap<RepairUnit.Builder, RepairUnit> repairUnitsByKey =
      new ConcurrentHashMap<>();
  private final ConcurrentMap<UUID, RepairRun> repairRuns = new ConcurrentHashMap<>();
  private final ConcurrentMap<UUID, RepairSchedule> repairSchedules = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Cluster> clusters = new ConcurrentHashMap<>();

  public MemoryStorageRoot() {
    super();
  }

  // Cluster operations
  public Map<String, Cluster> getClusters() {
    return clusters;
  }

  public Cluster addCluster(Cluster cluster) {
    return clusters.put(cluster.getName(), cluster);
  }

  public Cluster removeCluster(String clusterName) {
    return clusters.remove(clusterName);
  }

  // Repair Schedule operations
  public Map<UUID, RepairSchedule> getRepairSchedules() {
    return this.repairSchedules;
  }

  public RepairSchedule getRepairScheduleById(UUID id) {
    return this.repairSchedules.get(id);
  }

  public RepairSchedule addRepairSchedule(RepairSchedule schedule) {
    return repairSchedules.put(schedule.getId(), schedule);
  }

  public RepairSchedule removeRepairSchedule(UUID id) {
    return repairSchedules.remove(id);
  }

  // RepairSegment operations
  public Map<UUID, RepairSegment> getRepairSegments() {
    return this.repairSegments;
  }

  public RepairSegment addRepairSegment(RepairSegment segment) {
    return this.repairSegments.put(segment.getId(), segment);
  }

  public RepairSegment removeRepairSegment(UUID id) {
    return this.repairSegments.remove(id);
  }

  public RepairSegment getRepairSegmentById(UUID id) {
    return this.repairSegments.get(id);
  }

  // RepairUnit operations
  public Map<UUID, RepairUnit> getRepairUnits() {
    return this.repairUnits;
  }

  public Map<RepairUnit.Builder, RepairUnit> getRepairUnitsByKey() {
    return this.repairUnitsByKey;
  }

  public RepairUnit addRepairUnit(RepairUnit.Builder key, RepairUnit unit) {
    RepairUnit newUnit = this.repairUnits.put(unit.getId(), unit);
    if (key != null) {
      this.repairUnitsByKey.put(key, unit);
    }
    return newUnit;
  }

  public RepairUnit removeRepairUnit(RepairUnit.Builder key, UUID id) {
    RepairUnit unit = this.repairUnits.remove(id);
    if (key != null) {
      this.repairUnitsByKey.remove(key);
    }
    return unit;
  }

  public RepairUnit getrRepairUnitById(UUID id) {
    return this.repairUnits.get(id);
  }

  public RepairUnit getRepairUnitByKey(RepairUnit.Builder key) {
    return this.repairUnitsByKey.get(key);
  }

  // RepairRun operations
  public Map<UUID, RepairRun> getRepairRuns() {
    return this.repairRuns;
  }

  public RepairRun addRepairRun(RepairRun run) {
    return this.repairRuns.put(run.getId(), run);
  }

  public RepairRun removeRepairRun(UUID id) {
    return this.repairRuns.remove(id);
  }

  public RepairRun getRepairRunById(UUID id) {
    return this.repairRuns.get(id);
  }

  public static String toString(RepairSegment segment) {
    StringBuilder buf = new StringBuilder();
    buf.append("RepairSegment ID: ")
        .append(segment.getId())
        .append(", Token range: ")
        .append(segment.getTokenRange());
    return buf.toString();
  }
}
