/*
 * Copyright 2017-2017 Spotify AB
 * Copyright 2017-2018 The Last Pickle Ltd
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

package io.cassandrareaper.service;

import io.cassandrareaper.AppContext;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.RepairRun;
import io.cassandrareaper.core.RepairSchedule;
import io.cassandrareaper.core.RepairUnit;

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Preconditions;
import org.apache.cassandra.repair.RepairParallelism;
import org.joda.time.DateTime;

import static io.cassandrareaper.metrics.MetricNameUtils.cleanId;
import static io.cassandrareaper.metrics.MetricNameUtils.cleanName;

public final class RepairScheduleService {

  private final AppContext context;
  private final RepairUnitService repairUnitService;

  private RepairScheduleService(AppContext context) {
    this.context = context;
    this.repairUnitService = RepairUnitService.create(context);
    registerRepairScheduleMetrics(context.storage.getAllRepairSchedules());
  }

  public static RepairScheduleService create(AppContext context) {
    return new RepairScheduleService(context);
  }

  public Optional<RepairSchedule> conflictingRepairSchedule(Cluster cluster, RepairUnit.Builder repairUnit) {

    Collection<RepairSchedule> repairSchedules = context.storage
        .getRepairSchedulesForClusterAndKeyspace(repairUnit.clusterName, repairUnit.keyspaceName);

    for (RepairSchedule sched : repairSchedules) {
      RepairUnit repairUnitForSched = context.storage.getRepairUnit(sched.getRepairUnitId());
      Preconditions.checkState(repairUnitForSched.getClusterName().equals(repairUnit.clusterName));
      Preconditions.checkState(repairUnitForSched.getKeyspaceName().equals(repairUnit.keyspaceName));

      if (repairUnitService.conflictingUnits(cluster, repairUnitForSched, repairUnit)) {
        return Optional.of(sched);
      }
    }
    return Optional.empty();
  }

  public Optional<RepairSchedule> identicalRepairUnit(Cluster cluster, RepairUnit.Builder repairUnit) {

    Collection<RepairSchedule> repairSchedules = context.storage
        .getRepairSchedulesForClusterAndKeyspace(repairUnit.clusterName, repairUnit.keyspaceName);

    for (RepairSchedule sched : repairSchedules) {
      RepairUnit repairUnitForSched = context.storage.getRepairUnit(sched.getRepairUnitId());
      Preconditions.checkState(repairUnitForSched.getClusterName().equals(repairUnit.clusterName));
      Preconditions.checkState(repairUnitForSched.getKeyspaceName().equals(repairUnit.keyspaceName));

      // if the schedule is identical, return immediately
      if (repairUnitService.identicalUnits(cluster, repairUnitForSched, repairUnit)) {
        return Optional.of(sched);
      }
    }
    return Optional.empty();
  }

  /**
   * Instantiates a RepairSchedule and stores it in the storage backend.
   *
   *<p>
   * Expected to have called first  conflictingRepairSchedule(Cluster, RepairUnit)
   *
   * @return the new, just stored RepairSchedule instance
   */
  public RepairSchedule storeNewRepairSchedule(
      Cluster cluster,
      RepairUnit repairUnit,
      int daysBetween,
      DateTime nextActivation,
      String owner,
      int segmentCountPerNode,
      RepairParallelism repairParallelism,
      Double intensity,
      boolean force,
      boolean adaptive,
      int percentUnrepairedThreshold) {

    Preconditions.checkArgument(
        force || !conflictingRepairSchedule(cluster, repairUnit.with()).isPresent(),
        "A repair schedule already exists for cluster \"%s\", keyspace \"%s\", and column families: %s",
        cluster.getName(),
        repairUnit.getKeyspaceName(),
        repairUnit.getColumnFamilies());

    RepairSchedule.Builder scheduleBuilder = RepairSchedule.builder(repairUnit.getId())
        .daysBetween(daysBetween)
        .nextActivation(nextActivation)
        .repairParallelism(repairParallelism)
        .intensity(intensity)
        .segmentCountPerNode(segmentCountPerNode)
        .owner(owner)
        .adaptive(adaptive)
        .percentUnrepairedThreshold(percentUnrepairedThreshold);

    RepairSchedule repairSchedule = context.storage.addRepairSchedule(scheduleBuilder);
    registerScheduleMetrics(repairSchedule.getId());
    return repairSchedule;
  }

  public void deleteRepairSchedule(UUID repairScheduleId) {
    unregisterScheduleMetrics(repairScheduleId);
    context.storage.deleteRepairSchedule(repairScheduleId);
  }

  private void registerRepairScheduleMetrics(Collection<RepairSchedule> allRepairSchedules) {
    allRepairSchedules.forEach(schedule -> registerScheduleMetrics(schedule.getId()));
  }


  private void registerScheduleMetrics(UUID repairScheduleId) {
    RepairSchedule schedule = context.storage.getRepairSchedule(repairScheduleId).get();
    RepairUnit repairUnit = context.storage.getRepairUnit(schedule.getRepairUnitId());
    String metricName = metricName("millisSinceLastRepairForSchedule",
            repairUnit.getClusterName(),
            repairUnit.getKeyspaceName(),
            schedule.getId());

    if (!context.metricRegistry.getMetrics().containsKey(metricName)) {
      context.metricRegistry.register(metricName, getMillisSinceLastRepairForSchedule(schedule.getId()));
    }
  }

  private void unregisterScheduleMetrics(UUID repairScheduleId) {
    RepairSchedule schedule = context.storage.getRepairSchedule(repairScheduleId).get();
    RepairUnit repairUnit = context.storage.getRepairUnit(schedule.getRepairUnitId());
    String metricName = metricName("millisSinceLastRepairForSchedule",
        repairUnit.getClusterName(),
        repairUnit.getKeyspaceName(),
        schedule.getId());

    if (context.metricRegistry.getMetrics().containsKey(metricName)) {
      context.metricRegistry.remove(metricName);
    }
  }

  private Gauge<Long> getMillisSinceLastRepairForSchedule(UUID repairSchedule) {
    return () -> {
      Optional<RepairSchedule> schedule = context.storage.getRepairSchedule(repairSchedule);

      Optional<UUID> latestRepairUuid = Optional.ofNullable(schedule.orElseThrow(() ->
              new IllegalArgumentException("Repair schedule not found"))
          .getLastRun());

      Long millisSinceLastRepair = latestRepairUuid.map(uuid -> context.storage.getRepairRun(uuid))
          .filter(Optional::isPresent)
          .map(Optional::get)
          .map(RepairRun::getEndTime)
          .filter(Objects::nonNull)
          .map(dateTime -> DateTime.now().getMillis() - dateTime.getMillis())
          .orElse(DateTime.now().getMillis()); // Return epoch if no repairs from this schedule were completed
      return millisSinceLastRepair;
    };
  }

  private String metricName(String metric, String clusterName, String keyspaceName, UUID scheduleId) {
    return MetricRegistry.name(RepairScheduleService.class,
        metric, cleanName(clusterName), cleanName(keyspaceName), cleanId(scheduleId));

  }
}

