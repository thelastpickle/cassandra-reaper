/*
 * Copyright 2015-2017 Spotify AB
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

package io.cassandrareaper.service;

import io.cassandrareaper.AppContext;
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.RepairRun;
import io.cassandrareaper.core.RepairSchedule;
import io.cassandrareaper.core.RepairUnit;

import java.util.Collection;
import java.util.Optional;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SchedulingManager extends TimerTask {

  private static final Logger LOG = LoggerFactory.getLogger(SchedulingManager.class);

  private final AppContext context;
  private final RepairRunService repairRunService;

  /* nextActivatedSchedule used for nicer logging only */
  private RepairSchedule nextActivatedSchedule;

  private SchedulingManager(AppContext context) {
    this.context = context;
    this.repairRunService = RepairRunService.create(context);
  }

  public static SchedulingManager create(AppContext context) {
    return new SchedulingManager(context);
  }

  public void start() {
    LOG.info("Starting new SchedulingManager instance");
    Timer timer = new Timer("SchedulingManagerTimer");

    timer.schedule(
        this,
        1000L,
        1000L * Integer.getInteger(SchedulingManager.class.getName() + ".period_seconds", 60));
  }

  public RepairSchedule pauseRepairSchedule(RepairSchedule schedule) {
    RepairSchedule updatedSchedule
        = schedule.with().state(RepairSchedule.State.PAUSED).pauseTime(DateTime.now()).build(schedule.getId());

    if (!context.storage.updateRepairSchedule(updatedSchedule)) {
      throw new IllegalStateException(String.format("failed updating repair schedule %s", updatedSchedule.getId()));
    }
    return updatedSchedule;
  }

  public RepairSchedule resumeRepairSchedule(RepairSchedule schedule) {
    RepairSchedule updatedSchedule
        = schedule.with().state(RepairSchedule.State.ACTIVE).pauseTime(null).build(schedule.getId());

    if (!context.storage.updateRepairSchedule(updatedSchedule)) {
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
        Collection<RepairSchedule> schedules = context.storage.getAllRepairSchedules();
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
      } catch (Throwable ex) {
        LOG.error("failed managing schedule for run with id: {}", lastId);
        LOG.error("catch exception", ex);
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

  /**
   * Manage, i.e. check whether a new repair run should be started with this schedule.
   *
   * @param schdle The schedule to be checked for activation.
   * @return boolean indicating whether a new RepairRun instance was created and started.
   */
  private boolean manageSchedule(RepairSchedule schdle) {
    switch (schdle.getState()) {
      case ACTIVE:
        if (schdle.getNextActivation().isBeforeNow()) {

          RepairSchedule schedule
              = schdle.with().nextActivation(schdle.getFollowingActivation()).build(schdle.getId());

          context.storage.updateRepairSchedule(schedule);

          LOG.info(
              "repair unit '{}' should be repaired based on RepairSchedule with id '{}'",
              schedule.getRepairUnitId(),
              schedule.getId());

          RepairUnit repairUnit = context.storage.getRepairUnit(schedule.getRepairUnitId());
          if (repairRunAlreadyScheduled(schedule, repairUnit)) {
            return false;
          }

          try {
            RepairRun newRepairRun = createNewRunForUnit(schedule, repairUnit);

            ImmutableList<UUID> newRunHistory
                = new ImmutableList.Builder<UUID>()
                    .addAll(schedule.getRunHistory())
                    .add(newRepairRun.getId())
                    .build();

            RepairSchedule latestSchedule
                = context.storage.getRepairSchedule(schedule.getId()).get();

            if (equal(schedule, latestSchedule)) {

              // always overwrites and returns true. see following FIXMEs
              boolean result
                  = context.storage.updateRepairSchedule(
                      schedule.with().runHistory(newRunHistory).build(schedule.getId()));

              // FIXME – concurrency is broken unless we atomically add/remove run history items
              // boolean result = context.storage
              //        .addRepairRunToRepairSchedule(schedule.getId(), newRepairRun.getId());

              if (result) {
                context.repairManager.startRepairRun(newRepairRun);
                return true;
              }
              // this duplicated repair_run needs to be removed from the schedule's history
              // FIXME – concurrency is broken unless we atomically add/remove run history items
              // boolean result = context.storage
              //        .deleteRepairRunFromRepairSchedule(schedule.getId(), newRepairRun.getId());
            } else if (schedule.getRunHistory().size() < latestSchedule.getRunHistory().size()) {
              UUID latestRepairRun = latestSchedule.getRunHistory().get(latestSchedule.getRunHistory().size() - 1);
              LOG.info(
                  "schedule {} has already added a new repair run {}",
                  schedule.getId(),
                  latestRepairRun);
              // newRepairRun is identified as a duplicate (for this schedule and activation time)
              // so take the actual last repair run, and try start it. it's ok if already running.
              newRepairRun = context.storage.getRepairRun(latestRepairRun).get();
              context.repairManager.startRepairRun(newRepairRun);
            } else {
              LOG.warn(
                  "schedule {} has been altered by someone else. not running repair",
                  schedule.getId());
            }
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

  private static boolean equal(RepairSchedule s1, RepairSchedule s2) {
    Preconditions.checkArgument(s1.getId().equals(s2.getId()), "%s does not equal %s", s1.getId(), s2.getId());

    Preconditions.checkArgument(
        s1.getOwner().equals(s2.getOwner()),
        "%s does not equal %s",
        s1.getOwner(),
        s2.getOwner());

    Preconditions.checkArgument(
        s1.getDaysBetween() == s2.getDaysBetween(),
        "%s does not equal %s",
        s1.getDaysBetween(),
        s2.getDaysBetween());

    Preconditions.checkArgument(
        0.01d > Math.abs(s1.getIntensity() - s2.getIntensity()),
        "%s does not equal %s",
        s1.getIntensity(),
        s2.getIntensity());

    Preconditions.checkArgument(
        s1.getCreationTime().equals(s2.getCreationTime()),
        "%s does not equal %s",
        s1.getCreationTime(),
        s2.getCreationTime());

    Preconditions.checkArgument(
        s1.getNextActivation().equals(s2.getNextActivation()),
        "%s does not equal %s",
        s1.getNextActivation(),
        s2.getNextActivation());

    Preconditions.checkArgument(
        s1.getFollowingActivation().equals(s2.getFollowingActivation()),
        "%s does not equal %s",
        s1.getFollowingActivation(),
        s2.getFollowingActivation());

    boolean result = s1.getState().equals(s2.getState());
    result &= s1.getRunHistory().size() == s2.getRunHistory().size();

    for (int i = 0; result && i < s1.getRunHistory().size(); ++i) {
      result &= s1.getRunHistory().get(i).equals(s2.getRunHistory().get(i));
    }
    return result;
  }

  private boolean repairRunAlreadyScheduled(RepairSchedule schedule, RepairUnit repairUnit) {
    Collection<RepairRun> repairRuns = context.storage.getRepairRunsForUnit(schedule.getRepairUnitId());
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

  private static boolean repairRunComesFromSchedule(RepairRun repairRun, RepairSchedule schedule) {
    return repairRun.getRunState().isActive()
        || (RepairRun.RunState.NOT_STARTED == repairRun.getRunState()
        && repairRun.getCause().equals(getCauseName(schedule)));
  }

  private RepairRun createNewRunForUnit(RepairSchedule schedule, RepairUnit repairUnit) throws ReaperException {

    return repairRunService.registerRepairRun(
        context.storage.getCluster(repairUnit.getClusterName()).get(),
        repairUnit,
        Optional.of(getCauseName(schedule)),
        schedule.getOwner(),
        schedule.getSegmentCount(),
        schedule.getSegmentCountPerNode(),
        schedule.getRepairParallelism(),
        schedule.getIntensity(),
        schedule.getActiveTime(),
        schedule.getInactiveTime());
  }

  private static String getCauseName(RepairSchedule schedule) {
    return "scheduled run (schedule id " + schedule.getId().toString() + ')';
  }
}
