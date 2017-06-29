package com.spotify.reaper.service;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import com.spotify.reaper.AppContext;
import com.spotify.reaper.ReaperException;
import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.core.RepairSchedule;
import com.spotify.reaper.core.RepairUnit;
import com.spotify.reaper.resources.CommonTools;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;

public final class SchedulingManager extends TimerTask {

  private static final Logger LOG = LoggerFactory.getLogger(SchedulingManager.class);

  private static TimerTask schedulingManager;

  public static void start(AppContext context) {
    if (null == schedulingManager) {
      LOG.info("Starting new SchedulingManager instance");
      schedulingManager = new SchedulingManager(context);
      Timer timer = new Timer("SchedulingManagerTimer");
      timer.schedule(schedulingManager, 1000l, 1000l * 60);
    } else {
      LOG.warn("there is already one instance of SchedulingManager running, not starting new one");
    }
  }

  public static RepairSchedule pauseRepairSchedule(AppContext context, RepairSchedule schedule) {
    RepairSchedule updatedSchedule = schedule.with()
        .state(RepairSchedule.State.PAUSED)
        .pauseTime(DateTime.now())
        .build(schedule.getId());
    if (!context.storage.updateRepairSchedule(updatedSchedule)) {
      throw new IllegalStateException(String.format("failed updating repair schedule %s", updatedSchedule.getId()));
    }
    return updatedSchedule;
  }

  public static RepairSchedule resumeRepairSchedule(AppContext context, RepairSchedule schedule) {
    RepairSchedule updatedSchedule = schedule.with()
        .state(RepairSchedule.State.ACTIVE)
        .pauseTime(null)
        .build(schedule.getId());
    if (!context.storage.updateRepairSchedule(updatedSchedule)) {
      throw new IllegalStateException(String.format("failed updating repair schedule %s", updatedSchedule.getId()));
    }
    return updatedSchedule;
  }

  private final AppContext context;

  /* nextActivatedSchedule used for nicer logging only */
  private RepairSchedule nextActivatedSchedule;

  private SchedulingManager(AppContext context) {
    this.context = context;
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
            LOG.debug("not scheduling new repairs yet, next activation is '{}' for schedule id '{}'",
                      nextActivatedSchedule.getNextActivation(), nextActivatedSchedule.getId());
          }
        } catch (Exception ex) {
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
   * @param schedule The schedule to be checked for activation.
   * @return boolean indicating whether a new RepairRun instance was created and started.
   */
  private boolean manageSchedule(RepairSchedule schedule_) {
    switch (schedule_.getState()) {
        case ACTIVE:
            if (schedule_.getNextActivation().isBeforeNow()) {

              RepairSchedule schedule = schedule_
                      .with().nextActivation(schedule_.getFollowingActivation()).build(schedule_.getId());

              context.storage.updateRepairSchedule(schedule);

              LOG.info(
                      "repair unit '{}' should be repaired based on RepairSchedule with id '{}'",
                       schedule.getRepairUnitId(), schedule.getId());

              Optional<RepairUnit> fetchedUnit = context.storage.getRepairUnit(schedule.getRepairUnitId());
              if (!fetchedUnit.isPresent()) {
                LOG.warn("RepairUnit with id {} not found", schedule.getRepairUnitId());
                return false;
              }
              RepairUnit repairUnit = fetchedUnit.get();
              if (repairRunAlreadyScheduled(schedule, repairUnit)) {
                return false;
              }

              try {
                  RepairRun newRepairRun = createNewRunForUnit(schedule, repairUnit);

                  ImmutableList<UUID> newRunHistory = new ImmutableList.Builder<UUID>()
                      .addAll(schedule.getRunHistory()).add(newRepairRun.getId()).build();

                  RepairSchedule latestSchedule = context.storage.getRepairSchedule(schedule.getId()).get();

                  if (equal(schedule, latestSchedule)) {
                      
                    boolean result = context.storage.updateRepairSchedule(
                          schedule.with()
                            .runHistory(newRunHistory)
                            .build(schedule.getId()));
                    // FIXME – concurrency is broken unless we atomically add/remove run history items
                    //boolean result = context.storage
                    //        .addRepairRunToRepairSchedule(schedule.getId(), newRepairRun.getId());

                    if (result) {
                      context.repairManager.startRepairRun(context, newRepairRun);
                      return true;
                    }
                  } else if (schedule.getRunHistory().size() < latestSchedule.getRunHistory().size()) {
                      UUID newRepairRunId = latestSchedule.getRunHistory().get(latestSchedule.getRunHistory().size()-1);
                      LOG.info("schedule {} has already added a new repair run {}", schedule.getId(), newRepairRunId);
                      // this repair_run is identified as a duplicate (for this activation):
                      // so take the last repair run, and try start it. it's ok if already running.
                      newRepairRun = context.storage.getRepairRun(newRepairRunId).get();
                      context.repairManager.startRepairRun(context, newRepairRun);
                  } else {
                      LOG.warn("schedule {} has been altered by someone else. not running repair", schedule.getId());
                  }
                  // this duplicated repair_run needs to be removed from the schedule's history
                  // FIXME – concurrency is broken unless we atomically add/remove run history items
                  //boolean result = context.storage
                  //        .deleteRepairRunFromRepairSchedule(schedule.getId(), newRepairRun.getId());
              } catch (ReaperException e) {
                  LOG.error(e.getMessage(), e);
              }
            } else {
              if (nextActivatedSchedule == null
                      || nextActivatedSchedule.getNextActivation().isAfter(schedule_.getNextActivation())) {

                nextActivatedSchedule = schedule_;
              }
            }
            break;
        case PAUSED:
            LOG.info("Repair schedule '{}' is paused", schedule_.getId());
            return false;
        default:
            throw new AssertionError("illegal schedule state in call to manageSchedule(..): " + schedule_.getState());
    }
    return false;
  }

    private static boolean equal(RepairSchedule s1, RepairSchedule s2) {
        Preconditions.checkArgument(s1.getId().equals(s2.getId()));
        Preconditions.checkArgument(s1.getOwner().equals(s2.getOwner()));
        Preconditions.checkArgument(s1.getDaysBetween() == s2.getDaysBetween());
        Preconditions.checkArgument(s1.getIntensity() == s2.getIntensity());
        Preconditions.checkArgument(s1.getCreationTime().equals(s2.getCreationTime()));
        Preconditions.checkArgument(s1.getNextActivation().equals(s2.getNextActivation()));
        Preconditions.checkArgument(s1.getFollowingActivation().equals(s2.getFollowingActivation()));

        boolean result = s1.getState().equals(s2.getState());
        result &= s1.getRunHistory().size() == s2.getRunHistory().size();

        for (int i = 0 ; result && i < s1.getRunHistory().size() ; ++i) {
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
                        repairRun.getId(), repairRun.getRunState(), repairUnit.getId());
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

    return CommonTools.registerRepairRun(
            context,
            context.storage.getCluster(repairUnit.getClusterName()).get(),
            repairUnit,
            Optional.of(getCauseName(schedule)),
            schedule.getOwner(),
            schedule.getSegmentCount(),
            schedule.getRepairParallelism(),
            schedule.getIntensity());
  }

  private static String getCauseName(RepairSchedule schedule) {
      return "scheduled run (schedule id " + schedule.getId().toString() + ')';
  }

}
