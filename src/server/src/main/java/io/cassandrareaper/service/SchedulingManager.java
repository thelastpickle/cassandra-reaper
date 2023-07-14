/*
 * Copyright 2015-2017 Spotify AB
 * Copyright 2016-2019 The Last Pickle Ltd
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
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.PercentRepairedMetric;
import io.cassandrareaper.core.RepairRun;
import io.cassandrareaper.core.RepairSchedule;
import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.storage.IDistributedStorage;
import io.cassandrareaper.storage.repairrun.IRepairRunDao;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.DriverInternalError;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SchedulingManager extends TimerTask {

  private static final Logger LOG = LoggerFactory.getLogger(SchedulingManager.class);

  private final AppContext context;
  private final RepairRunService repairRunService;

  /* nextActivatedSchedule used for nicer logging only */
  private RepairSchedule nextActivatedSchedule;

  private IRepairRunDao repairRunDao;

  private SchedulingManager(AppContext context, Supplier<RepairRunService> repairRunServiceSupplier,
                            IRepairRunDao repairRunDao) {
    this.context = context;
    this.repairRunService = repairRunServiceSupplier.get();
    this.repairRunDao = repairRunDao;
  }

  public static SchedulingManager create(AppContext context, IRepairRunDao repairRunDao) {
    return new SchedulingManager(context, () -> RepairRunService.create(context, repairRunDao), repairRunDao);
  }

  @VisibleForTesting
  static SchedulingManager create(AppContext context, Supplier<RepairRunService> repairRunServiceSupplier,
                                  IRepairRunDao repairRunDao) {
    return new SchedulingManager(context, repairRunServiceSupplier, repairRunDao);
  }

  private static boolean repairRunComesFromSchedule(RepairRun repairRun, RepairSchedule schedule) {
    return repairRun.getRunState().isActive()
        || (RepairRun.RunState.NOT_STARTED == repairRun.getRunState()
        && repairRun.getCause().equals(getCauseName(schedule)));
  }

  private static String getCauseName(RepairSchedule schedule) {
    return "scheduled run (schedule id " + schedule.getId().toString() + ')';
  }

  public void start() {
    LOG.info("Starting new SchedulingManager instance");
    Timer timer = new Timer("SchedulingManagerTimer");

    timer.schedule(
        this,
        ThreadLocalRandom.current().nextLong(1000, 2000),
        1000L * Integer.getInteger(SchedulingManager.class.getName() + ".period_seconds", 60));
  }

  public RepairSchedule pauseRepairSchedule(RepairSchedule schedule) {
    RepairSchedule updatedSchedule
        = schedule.with().state(RepairSchedule.State.PAUSED).pauseTime(DateTime.now()).build(schedule.getId());

    if (!context.storage.getRepairScheduleDao().updateRepairSchedule(updatedSchedule)) {
      throw new IllegalStateException(String.format("failed updating repair schedule %s", updatedSchedule.getId()));
    }
    return updatedSchedule;
  }

  public RepairSchedule resumeRepairSchedule(RepairSchedule schedule) {
    RepairSchedule updatedSchedule
        = schedule.with().state(RepairSchedule.State.ACTIVE).pauseTime(null).build(schedule.getId());

    if (!context.storage.getRepairScheduleDao().updateRepairSchedule(updatedSchedule)) {
      throw new IllegalStateException(String.format("failed updating repair schedule %s", updatedSchedule.getId()));
    }
    return updatedSchedule;
  }

  /**
   * Called regularly, do not block!
   */
  @Override
  public void run() {
    if (context.isRunning.get()) {
      LOG.debug("Checking for repair schedules...");
      UUID lastId = null;
      try {
        Collection<RepairSchedule> schedules = context.storage.getRepairScheduleDao().getAllRepairSchedules();
        // Cleanup metric registry from deleted schedules
        cleanupMetricsRegistry(schedules);
        // Start repairs for schedules that require it
        if (currentReaperIsSchedulingLeader()) {
          boolean anyRunStarted = false;
          for (RepairSchedule schedule : schedules) {
            lastId = schedule.getId();
            anyRunStarted = manageSchedule(schedule) || anyRunStarted;
          }
          if (!anyRunStarted && nextActivatedSchedule != null) {
            LOG.debug(
                "not scheduling new repairs yet, next activation is '{}' for schedule id '{}'",
                nextActivatedSchedule.getNextActivation(),
                nextActivatedSchedule.getId());
          }
        }
      } catch (DriverInternalError expected) {
        LOG.debug("Driver connection closed, Reaper is shutting down.");
      } catch (DriverException e) {
        LOG.error("Error while scheduling repairs due to a connection problem with the database", e);
      } catch (Throwable ex) {
        if (lastId == null) {
          LOG.error("Failed managing repair schedules", ex);
        } else {
          LOG.error("Failed managing repair schedule with id '{}'", lastId, ex);
        }
        try {
          assert false : "if assertions are enabled then exit the jvm";
        } catch (AssertionError ae) {
          if (context.isRunning.get()) {
            LOG.error("SchedulingManager failed. Exiting JVM.");
            System.exit(1);
          }
        }
      }
    }
  }

  // Cleanup metric registry from deleted schedules
  // Such metrics are named after the following pattern:
  //   "millisSinceLastRepairForSchedule.<cluster>.<keyspace>.<schedule id>"
  @VisibleForTesting
  void cleanupMetricsRegistry(Collection<RepairSchedule> schedules) {
    // Cycle through the metrics registry and delete any metrics that are not in the current schedules
    context.metricRegistry.getMetrics().keySet().stream()
        .filter(key -> key.startsWith(RepairScheduleService.MILLIS_SINCE_LAST_REPAIR_METRIC_NAME))
        .filter(key -> !schedules.stream().anyMatch(
            schedule -> schedule.getId().toString().equals(key.split("\\.")[3])))
        .forEach(context.metricRegistry::remove);
  }

  /**
   * Manage, i.e. check whether a new repair run should be started with this schedule.
   *
   * @param schdle The schedule to be checked for activation.
   * @return boolean indicating whether a new RepairRun instance was created and started.
   */
  @VisibleForTesting
  boolean manageSchedule(RepairSchedule schdle) {
    RepairUnit unit = context.storage.getRepairUnitDao().getRepairUnit(schdle.getRepairUnitId());
    boolean overUnrepairedThreshold = false;
    if (unit.getIncrementalRepair() && schdle.getPercentUnrepairedThreshold() > 0) {
      List<PercentRepairedMetric> percentRepairedMetrics = context.storage.getPercentRepairedMetrics(
          unit.getClusterName(),
          schdle.getId(),
          DateTime.now().minusMinutes(context.config.getPercentRepairedCheckIntervalMinutes() + 1).getMillis());
      int maxUnrepairedPercent
          = 100 - percentRepairedMetrics.stream().mapToInt(PercentRepairedMetric::getPercentRepaired).max().orElse(100);
      LOG.debug(
          "Current unrepaired percent for schedule {} is {} and threshold is {}",
          schdle.getId(), maxUnrepairedPercent,
          schdle.getPercentUnrepairedThreshold());
      if (maxUnrepairedPercent >= schdle.getPercentUnrepairedThreshold()) {
        overUnrepairedThreshold = true;
      }
    }
    switch (schdle.getState()) {
      case ACTIVE:
        if (schdle.getNextActivation().isBeforeNow() || (overUnrepairedThreshold && lastRepairRunIsOldEnough(schdle))) {

          RepairSchedule schedule
              = schdle.with().nextActivation(schdle.getFollowingActivation()).build(schdle.getId());

          context.storage.getRepairScheduleDao().updateRepairSchedule(schedule);

          LOG.info(
              "repair unit '{}' should be repaired based on RepairSchedule with id '{}'",
              schedule.getRepairUnitId(),
              schedule.getId());

          if (repairRunAlreadyScheduled(schedule, unit)) {
            return false;
          }

          try {
            RepairRun newRepairRun = createNewRunForUnit(schedule, unit);
            context.repairManager.startRepairRun(newRepairRun);
            return true;
          } catch (ReaperException e) {
            LOG.error(e.getMessage(), e);
          }
        } else {
          if (nextActivatedSchedule == null
              || nextActivatedSchedule.getNextActivation().isAfter(schdle.getNextActivation())) {

            nextActivatedSchedule = schdle;
          }
        }
        break;
      case PAUSED:
        LOG.info("Repair schedule '{}' is paused", schdle.getId());
        return false;
      default:
        throw new AssertionError("illegal schedule state in call to manageSchedule(..): " + schdle.getState());
    }
    return false;
  }

  private boolean repairRunAlreadyScheduled(RepairSchedule schedule, RepairUnit repairUnit) {
    Collection<RepairRun> repairRuns = repairRunDao.getRepairRunsForUnit(schedule.getRepairUnitId());
    for (RepairRun repairRun : repairRuns) {
      if (repairRunComesFromSchedule(repairRun, schedule)) {
        LOG.info(
            "there is repair (id {}) in state '{}' for repair unit '{}', "
                + "postponing current schedule trigger until next scheduling",
            repairRun.getId(),
            repairRun.getRunState(),
            repairUnit.getId());
        return true;
      }
    }
    return false;
  }

  private RepairRun createNewRunForUnit(RepairSchedule schedule, RepairUnit repairUnit) throws ReaperException {

    return repairRunService.registerRepairRun(
        context.storage.getClusterDao().getCluster(repairUnit.getClusterName()),
        repairUnit,
        Optional.of(getCauseName(schedule)),
        schedule.getOwner(),
        schedule.getSegmentCountPerNode(),
        schedule.getRepairParallelism(),
        schedule.getIntensity(),
        schedule.getAdaptive());
  }

  /**
   * When multiple Reapers are running, only the older one can start schedules.
   * In non distributed modes, this method always returns true.
   *
   * @return true or false
   */
  @VisibleForTesting
  boolean currentReaperIsSchedulingLeader() {
    if (context.isDistributed.get()) {
      List<UUID> runningReapers = ((IDistributedStorage) context.storage).getRunningReapers();
      Collections.sort(runningReapers);
      if (runningReapers.isEmpty()) {
        // this should never happen, but if it does, we don't want to start a repair run
        LOG.warn("No running reapers found but running in distributed mode."
            + " No scheduling leader can be elected and no scheduled run will start.");
        return false;
      }
      return context.reaperInstanceId.equals(runningReapers.get(0));
    }

    return true;
  }

  /**
   * A schedule triggered by percent repaired metrics must wait for those metrics to refresh between runs.
   * We give two metrics refresh cycles before allowing a new run.
   *
   * @return true or false
   */
  @VisibleForTesting
  boolean lastRepairRunIsOldEnough(RepairSchedule schedule) {
    if (schedule.getPercentUnrepairedThreshold() > 0 && schedule.getLastRun() != null) {
      Optional<RepairRun> lastRun = repairRunDao.getRepairRun(schedule.getLastRun());
      if (lastRun.isPresent()) {
        DateTime lastRunEndTime = lastRun.get().getEndTime();
        DateTime nextAllowedRunTime
            = lastRunEndTime.plusMinutes(context.config.getPercentRepairedCheckIntervalMinutes() * 2);
        DateTime currentTime = DateTime.now();
        boolean canRun = currentTime.isAfter(nextAllowedRunTime);
        return canRun;
      }
    }
    return true;
  }

  public void maybeRegisterRepairRunCompleted(RepairRun repairRun) {
    Collection<RepairSchedule> repairSchedulesForCluster = context.storage.getRepairScheduleDao()
        .getRepairSchedulesForCluster(repairRun.getClusterName());

    repairSchedulesForCluster.stream().filter(schedule -> repairRunComesFromSchedule(repairRun, schedule))
        .findFirst()
        .ifPresent(schedule -> context.storage.getRepairScheduleDao().updateRepairSchedule(
            schedule.with().lastRun(repairRun.getId()).build(schedule.getId())));
  }
}