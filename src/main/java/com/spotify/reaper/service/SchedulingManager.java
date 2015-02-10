package com.spotify.reaper.service;

import com.google.common.base.Optional;

import com.spotify.reaper.AppContext;
import com.spotify.reaper.ReaperException;
import com.spotify.reaper.core.Cluster;
import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.core.RepairSchedule;
import com.spotify.reaper.core.RepairUnit;
import com.spotify.reaper.resources.CommonTools;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Timer;
import java.util.TimerTask;

public class SchedulingManager extends TimerTask {

  private static final Logger LOG = LoggerFactory.getLogger(SchedulingManager.class);

  private static TimerTask schedulingManager;

  public static void start(AppContext context) {
    if (null == schedulingManager) {
      LOG.info("Starting new SchedulingManager instance");
      schedulingManager = new SchedulingManager(context);
      Timer timer = new Timer();
      timer.schedule(schedulingManager, 1000, 1000 * 60); // activate once per minute
    } else {
      LOG.warn("there is already one instance of SchedulingManager running, not starting new");
    }
  }

  private AppContext context;

  private SchedulingManager(AppContext context) {
    this.context = context;
  }

  /**
   * Called regularly, do not block!
   */
  @Override
  public void run() {
    LOG.debug("SchedulingManager activated");
    long lastId = -1;
    try {
      Collection<RepairSchedule> schedules = context.storage.getAllRepairSchedules();
      for (RepairSchedule schedule : schedules) {
        lastId = schedule.getId();
        manageSchedule(schedule);
      }
    } catch (Exception ex) {
      LOG.error("failed managing schedule for run with id: {}", lastId);
      LOG.error("catch exception", ex);
    }
  }

  private void manageSchedule(RepairSchedule schedule) throws ReaperException {
    if (schedule.getNextActivation().isBeforeNow()) {
      LOG.info("repair unit '{}' should be repaired based on RepairSchedule with id '{}'",
               schedule.getRepairUnitId(), schedule.getId());

      RepairUnit repairUnit = context.storage.getRepairUnit(schedule.getRepairUnitId()).get();
      Collection<RepairRun> repairRuns = context.storage.getRepairRunsForUnit(repairUnit);

      boolean canStartNewRun = true;
      for (RepairRun repairRun : repairRuns) {
        RepairRun.RunState state = repairRun.getRunState();
        if (state != RepairRun.RunState.DONE && state != RepairRun.RunState.NOT_STARTED) {
          LOG.info("there is repair (id {}) in state '{}' for repair unit '{}', "
                   + "postponing current schedule trigger until next scheduling",
                   repairRun.getId(), repairRun.getRunState(), repairUnit.getId());
          canStartNewRun = false;
        }
      }

      if (canStartNewRun) {
        startNewRunForUnit(schedule, repairUnit);
        context.storage.updateRepairSchedule(schedule.with()
                                                 .nextActivation(schedule.getFollowingActivation())
                                                 .build(schedule.getId()));
      } else {
        LOG.warn("skip scheduling, next activation for repair unit '{}' will be: {}",
                 repairUnit.getId(), schedule.getFollowingActivation());
        context.storage.updateRepairSchedule(schedule.with()
                                                 .nextActivation(schedule.getFollowingActivation())
                                                 .build(schedule.getId()));
      }
    } else {
      LOG.debug("not scheduling new repairs yet for repair unit '{}', next activation: {}",
                schedule.getRepairUnitId(), schedule.getNextActivation());
    }
  }

  private void startNewRunForUnit(RepairSchedule schedule, RepairUnit repairUnit)
      throws ReaperException {
    Cluster cluster = context.storage.getCluster(repairUnit.getClusterName()).get();
    RepairRun newRepairRun = CommonTools.registerRepairRun(
        context, cluster, repairUnit, Optional.of("scheduled run"),
        schedule.getOwner(), schedule.getSegmentCount(), schedule.getRepairParallelism(),
        schedule.getIntensity());
    RepairRunner.startRepairRun(context, newRepairRun);
  }

}
