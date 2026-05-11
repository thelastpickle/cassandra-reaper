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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RepairScheduleService {

  private static final Logger LOG = LoggerFactory.getLogger(RepairScheduleService.class);

  public static final String MILLIS_SINCE_LAST_REPAIR_METRIC_NAME =
      "millisSinceLastRepairForSchedule";
  public static final String UNFULFILLED_REPAIR_SCHEDULE_METRIC_NAME = "unfulfilledRepairSchedule";
  private final AppContext context;
  private final RepairUnitService repairUnitService;

  private final IRepairRunDao repairRunDao;

  private RepairScheduleService(AppContext context, IRepairRunDao repairRunDao) {
    this.context = context;
    this.repairUnitService = RepairUnitService.create(context);
    this.repairRunDao = repairRunDao;
    registerRepairScheduleMetrics(context.storage.getRepairScheduleDao().getAllRepairSchedules());
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

    // Initialize lastRun from repair history if not set
    // This ensures metrics are accurate after restart
    LOG.debug("Last run for schedule " + schedule.getId() + " is " + schedule.getLastRun());
    if (schedule.getLastRun() == null) {
      Optional<UUID> lastCompletedRun = findLastCompletedRepairRun(repairUnit.getId());
      if (lastCompletedRun.isPresent()) {
        RepairSchedule updatedSchedule =
            schedule.with().lastRun(lastCompletedRun.get()).build(schedule.getId());
        context.storage.getRepairScheduleDao().updateRepairSchedule(updatedSchedule);
        schedule = updatedSchedule;
      }
    }

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

  private Long getMillisSinceLastRepairForScheduleAsLong(UUID repairSchedule) {
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
  }

  private Gauge<Long> getMillisSinceLastRepairForSchedule(UUID repairSchedule) {
    return () -> {
      return getMillisSinceLastRepairForScheduleAsLong(repairSchedule);
    };
  }

  @VisibleForTesting
  Gauge<Integer> getUnfulfilledRepairSchedule(UUID repairSchedule) {
    return () -> {
      RepairSchedule schedule =
          context
              .storage
              .getRepairScheduleDao()
              .getRepairSchedule(repairSchedule)
              .orElseThrow(() -> new IllegalArgumentException("Repair schedule not found"));

      // Cases where it is not relevant to check if the repair schedule is unfulfilled
      if (schedule.getDaysBetween() == 0
          || schedule.getState() == RepairSchedule.State.PAUSED
          || schedule.getState() == RepairSchedule.State.DELETED) {
        return 0;
      }

      long now = DateTime.now().getMillis();
      long intervalInMillis = schedule.getDaysBetween() * 24L * 60 * 60 * 1000;
      long graceInMillis = intervalInMillis / 10;
      long allowedDelayInMillis = intervalInMillis + graceInMillis;
      DateTime nextActivation = schedule.getNextActivation();
      Long millisSinceLastRepair = getMillisSinceLastRepairForScheduleAsLong(repairSchedule);

      // If the last repair was more than a year ago, we don't care about the cycle window, means it
      // didn't run at all
      if (millisSinceLastRepair < 365 * 24 * 60 * 60 * 1000) {
        boolean unfulfilled = millisSinceLastRepair > allowedDelayInMillis;
        if (unfulfilled) {
          LOG.debug(
              "Setting {}=1 for schedule {} because last completed repair is older than the allowed cycle window "
                  + "(state={}, daysBetween={}, intervalInMillis={}, graceInMillis={}, allowedDelayInMillis={}, "
                  + "now={},"
                  + "millisSinceLastRepair={}, nextActivation={}, millisPastNextActivation={})",
              UNFULFILLED_REPAIR_SCHEDULE_METRIC_NAME,
              schedule.getId(),
              schedule.getState(),
              schedule.getDaysBetween(),
              intervalInMillis,
              graceInMillis,
              allowedDelayInMillis,
              now,
              millisSinceLastRepair,
              nextActivation,
              nextActivation == null ? null : now - nextActivation.getMillis());
        }
        return unfulfilled ? 1 : 0;
      }

      if (nextActivation != null) {
        long millisPastNextActivation = now - nextActivation.getMillis();
        boolean unfulfilled = millisPastNextActivation > allowedDelayInMillis;
        if (unfulfilled) {
          LOG.debug(
              "Setting {}=1 for schedule {} because there is no completed repair in the schedule state and the "
                  + "next activation is older than the allowed cycle window "
                  + "(state={}, daysBetween={}, intervalInMillis={}, graceInMillis={}, allowedDelayInMillis={}, "
                  + "now={}, "
                  + "nextActivation={}, millisPastNextActivation={})",
              UNFULFILLED_REPAIR_SCHEDULE_METRIC_NAME,
              schedule.getId(),
              schedule.getState(),
              schedule.getDaysBetween(),
              intervalInMillis,
              graceInMillis,
              allowedDelayInMillis,
              now,
              nextActivation,
              millisPastNextActivation);
        }
        return unfulfilled ? 1 : 0;
      }

      return 0;
    };
  }

  /**
   * Finds the most recent completed repair run for a given repair unit. This is used to initialize
   * the lastRun field when it's null, ensuring metrics are accurate after restart.
   *
   * @param repairUnitId The repair unit ID to search for completed repairs
   * @return Optional containing the UUID of the most recent completed repair run, or empty if none
   *     found
   */
  private Optional<UUID> findLastCompletedRepairRun(UUID repairUnitId) {
    Collection<RepairRun> repairRuns = repairRunDao.getRepairRunsForUnit(repairUnitId);

    Optional<UUID> lastRepairRun =
        repairRuns.stream()
            .filter(run -> run.getRunState() == RepairRun.RunState.DONE)
            .filter(run -> run.getEndTime() != null)
            .min(RepairRun::compareTo)
            .map(RepairRun::getId);

    return lastRepairRun;
  }

  /**
   * Updates the schedule metrics after a repair run completes. This re-registers the metrics with
   * static gauges that capture the completion time, similar to how RepairRunner handles its
   * metrics. This is more efficient than querying the database on every metric read.
   *
   * @param scheduleId The UUID of the repair schedule whose metrics should be updated
   * @param repairRunCompleted The completion time of the repair run
   */
  public void updateScheduleMetricsAfterRepair(UUID scheduleId, DateTime repairRunCompleted) {
    Optional<RepairSchedule> schedule =
        context.storage.getRepairScheduleDao().getRepairSchedule(scheduleId);

    if (schedule.isPresent()) {
      RepairUnit repairUnit =
          context.storage.getRepairUnitDao().getRepairUnit(schedule.get().getRepairUnitId());

      String metricName =
          metricName(
              MILLIS_SINCE_LAST_REPAIR_METRIC_NAME,
              repairUnit.getClusterName(),
              repairUnit.getKeyspaceName(),
              scheduleId);

      // Remove the old dynamic gauge and register a new static one with the completion time
      if (context.metricRegistry.getMetrics().containsKey(metricName)) {
        context.metricRegistry.remove(metricName);
      }
      context.metricRegistry.register(
          metricName,
          (Gauge<Long>)
              () -> DateTime.now().getMillis() - repairRunCompleted.toInstant().getMillis());

      String unfulfilledRepairScheduleMetricName =
          metricName(
              UNFULFILLED_REPAIR_SCHEDULE_METRIC_NAME,
              repairUnit.getClusterName(),
              repairUnit.getKeyspaceName(),
              scheduleId);

      // Re-register the unfulfilled metric as well to reflect the latest state
      if (context.metricRegistry.getMetrics().containsKey(unfulfilledRepairScheduleMetricName)) {
        context.metricRegistry.remove(unfulfilledRepairScheduleMetricName);
      }
      context.metricRegistry.register(
          unfulfilledRepairScheduleMetricName, getUnfulfilledRepairSchedule(scheduleId));
    }
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
