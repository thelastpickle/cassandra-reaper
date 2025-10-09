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
import io.cassandrareaper.storage.repairrun.IRepairRunDao;

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import static io.cassandrareaper.metrics.MetricNameUtils.cleanId;
import static io.cassandrareaper.metrics.MetricNameUtils.cleanName;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.cassandra.repair.RepairParallelism;
import org.joda.time.DateTime;

public final class RepairScheduleService {

  public static final String MILLIS_SINCE_LAST_REPAIR_METRIC_NAME =
      "millisSinceLastRepairForSchedule";
  public static final String UNFULFILLED_REPAIR_SCHEDULE_METRIC_NAME = "unfulfilledRepairSchedule";
  private final AppContext context;
  private final RepairUnitService repairUnitService;

  private final IRepairRunDao repairRunDao;

  private RepairScheduleService(AppContext context, IRepairRunDao repairRunDao) {
    this.context = context;
    this.repairUnitService = RepairUnitService.create(context);
    registerRepairScheduleMetrics(context.storage.getRepairScheduleDao().getAllRepairSchedules());
    this.repairRunDao = repairRunDao;
  }

  public static RepairScheduleService create(AppContext context, IRepairRunDao repairRunDao) {
    return new RepairScheduleService(context, repairRunDao);
  }

  public Optional<RepairSchedule> conflictingRepairSchedule(
      Cluster cluster, RepairUnit.Builder repairUnit) {

    Collection<RepairSchedule> repairSchedules =
        context
            .storage
            .getRepairScheduleDao()
            .getRepairSchedulesForClusterAndKeyspace(
                repairUnit.clusterName, repairUnit.keyspaceName);

    for (RepairSchedule sched : repairSchedules) {
      RepairUnit repairUnitForSched =
          context.storage.getRepairUnitDao().getRepairUnit(sched.getRepairUnitId());
      Preconditions.checkState(repairUnitForSched.getClusterName().equals(repairUnit.clusterName));
      Preconditions.checkState(
          repairUnitForSched.getKeyspaceName().equals(repairUnit.keyspaceName));

      if (repairUnitService.conflictingUnits(cluster, repairUnitForSched, repairUnit)) {
        return Optional.of(sched);
      }
    }
    return Optional.empty();
  }

  public Optional<RepairSchedule> identicalRepairUnit(
      Cluster cluster, RepairUnit.Builder repairUnit) {

    Collection<RepairSchedule> repairSchedules =
        context
            .storage
            .getRepairScheduleDao()
            .getRepairSchedulesForClusterAndKeyspace(
                repairUnit.clusterName, repairUnit.keyspaceName);

    for (RepairSchedule sched : repairSchedules) {
      RepairUnit repairUnitForSched =
          context.storage.getRepairUnitDao().getRepairUnit(sched.getRepairUnitId());
      Preconditions.checkState(repairUnitForSched.getClusterName().equals(repairUnit.clusterName));
      Preconditions.checkState(
          repairUnitForSched.getKeyspaceName().equals(repairUnit.keyspaceName));

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
   * <p>Expected to have called first conflictingRepairSchedule(Cluster, RepairUnit)
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

    RepairSchedule.Builder scheduleBuilder =
        RepairSchedule.builder(repairUnit.getId())
            .daysBetween(daysBetween)
            .nextActivation(nextActivation)
            .repairParallelism(repairParallelism)
            .intensity(intensity)
            .segmentCountPerNode(segmentCountPerNode)
            .owner(owner)
            .adaptive(adaptive)
            .percentUnrepairedThreshold(percentUnrepairedThreshold);

    RepairSchedule repairSchedule =
        context.storage.getRepairScheduleDao().addRepairSchedule(scheduleBuilder);
    registerScheduleMetrics(repairSchedule.getId());
    return repairSchedule;
  }

  public void deleteRepairSchedule(UUID repairScheduleId) {
    unregisterScheduleMetrics(repairScheduleId);
    context.storage.getRepairScheduleDao().deleteRepairSchedule(repairScheduleId);
  }

  private void registerRepairScheduleMetrics(Collection<RepairSchedule> allRepairSchedules) {
    allRepairSchedules.forEach(schedule -> registerScheduleMetrics(schedule.getId()));
  }

  private void registerScheduleMetrics(UUID repairScheduleId) {
    RepairSchedule schedule =
        context.storage.getRepairScheduleDao().getRepairSchedule(repairScheduleId).get();
    RepairUnit repairUnit =
        context.storage.getRepairUnitDao().getRepairUnit(schedule.getRepairUnitId());
    String metricName =
        metricName(
            MILLIS_SINCE_LAST_REPAIR_METRIC_NAME,
            repairUnit.getClusterName(),
            repairUnit.getKeyspaceName(),
            schedule.getId());

    if (!context.metricRegistry.getMetrics().containsKey(metricName)) {
      context.metricRegistry.register(
          metricName, getMillisSinceLastRepairForSchedule(schedule.getId()));
    }

    String unfulfilledRepairScheduleMetricName =
        metricName(
            UNFULFILLED_REPAIR_SCHEDULE_METRIC_NAME,
            repairUnit.getClusterName(),
            repairUnit.getKeyspaceName(),
            schedule.getId());

    if (!context.metricRegistry.getMetrics().containsKey(unfulfilledRepairScheduleMetricName)) {
      context.metricRegistry.register(
          unfulfilledRepairScheduleMetricName, getUnfulfilledRepairSchedule(schedule.getId()));
    }
  }

  private void unregisterScheduleMetrics(UUID repairScheduleId) {
    Optional<RepairSchedule> schedule =
        context.storage.getRepairScheduleDao().getRepairSchedule(repairScheduleId);
    schedule.ifPresent(
        sched -> {
          RepairUnit repairUnit =
              context.storage.getRepairUnitDao().getRepairUnit(sched.getRepairUnitId());
          String metricName =
              metricName(
                  MILLIS_SINCE_LAST_REPAIR_METRIC_NAME,
                  repairUnit.getClusterName(),
                  repairUnit.getKeyspaceName(),
                  sched.getId());

          if (context.metricRegistry.getMetrics().containsKey(metricName)) {
            context.metricRegistry.remove(metricName);
          }

          String unfulfilledRepairScheduleMetricName =
              metricName(
                  UNFULFILLED_REPAIR_SCHEDULE_METRIC_NAME,
                  repairUnit.getClusterName(),
                  repairUnit.getKeyspaceName(),
                  sched.getId());

          if (context
              .metricRegistry
              .getMetrics()
              .containsKey(unfulfilledRepairScheduleMetricName)) {
            context.metricRegistry.remove(unfulfilledRepairScheduleMetricName);
          }
        });
  }

  private Gauge<Long> getMillisSinceLastRepairForSchedule(UUID repairSchedule) {
    return () -> {
      Optional<RepairSchedule> schedule =
          context.storage.getRepairScheduleDao().getRepairSchedule(repairSchedule);

      Optional<UUID> latestRepairUuid =
          Optional.ofNullable(
              schedule
                  .orElseThrow(() -> new IllegalArgumentException("Repair schedule not found"))
                  .getLastRun());

      Long millisSinceLastRepair =
          latestRepairUuid
              .map(uuid -> repairRunDao.getRepairRun(uuid))
              .filter(Optional::isPresent)
              .map(Optional::get)
              .map(RepairRun::getEndTime)
              .filter(Objects::nonNull)
              .map(dateTime -> DateTime.now().getMillis() - dateTime.getMillis())
              .orElse(
                  DateTime.now()
                      .getMillis()); // Return epoch if no repairs from this schedule were completed
      return millisSinceLastRepair;
    };
  }

  @VisibleForTesting
  Gauge<Long> getUnfulfilledRepairSchedule(UUID repairSchedule) {
    return () -> {
      Optional<RepairSchedule> schedule =
          context.storage.getRepairScheduleDao().getRepairSchedule(repairSchedule);

      Optional<UUID> latestRepairUuid =
          Optional.ofNullable(
              schedule
                  .orElseThrow(() -> new IllegalArgumentException("Repair schedule not found"))
                  .getLastRun());

      // Cases where it is not relevant to check if the repair schedule is unfulfilled
      if (schedule.get().getDaysBetween() == 0
          || schedule.get().getState() == RepairSchedule.State.PAUSED
          || schedule.get().getState() == RepairSchedule.State.DELETED) {
        return 0l;
      }

      // Fail fast if we're past the next activation time by 10% of the cycle time
      if (DateTime.now().getMillis() - schedule.get().getNextActivation().getMillis()
          > schedule.get().getDaysBetween() * 24 * 60 * 60 * 1000 * 0.1) {
        return 1l;
      }

      Optional<Long> millisSinceLastRepair =
          latestRepairUuid
              .map(uuid -> repairRunDao.getRepairRun(uuid))
              .filter(Optional::isPresent)
              .map(Optional::get)
              .filter(repairRun -> repairRun.getRunState() == RepairRun.RunState.DONE)
              .map(RepairRun::getEndTime)
              .filter(Objects::nonNull)
              .map(dateTime -> DateTime.now().getMillis() - dateTime.getMillis());

      if (millisSinceLastRepair.isPresent()) {
        // we have a last repair that has finished successfully
        if (millisSinceLastRepair.get() > schedule.get().getDaysBetween() * 24 * 60 * 60 * 1000) {
          // Return 1 if the last completed repair was not within the repair schedule interval
          return 1l;
        }
        return 0l;
      }

      // Repair is still running or never started but the next activation time is still in the
      // future
      return 0l;
    };
  }

  private String metricName(
      String metric, String clusterName, String keyspaceName, UUID scheduleId) {
    return MetricRegistry.name(
        RepairScheduleService.class,
        metric,
        cleanName(clusterName),
        cleanName(keyspaceName),
        cleanId(scheduleId));
  }
}
