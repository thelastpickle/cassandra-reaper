package com.spotify.reaper.service;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;

import com.spotify.reaper.AppContext;
import com.spotify.reaper.ReaperException;
import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.core.RepairSegment;

import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RepairManager {

  private static final Logger LOG = LoggerFactory.getLogger(RepairManager.class);

  private ScheduledExecutorService executor;
  private long repairTimeoutMillis;
  private long retryDelayMillis;

  public long getRepairTimeoutMillis() {
    return repairTimeoutMillis;
  }

  // Caching all active RepairRunners.
  @VisibleForTesting
  public Map<Long, RepairRunner> repairRunners = Maps.newConcurrentMap();

  public void initializeThreadPool(int threadAmount, long repairTimeout,
                                          TimeUnit repairTimeoutTimeUnit, long retryDelay,
                                          TimeUnit retryDelayTimeUnit) {
    executor = Executors
        .newScheduledThreadPool(threadAmount, new NamedThreadFactory("RepairRunner"));
    repairTimeoutMillis = repairTimeoutTimeUnit.toMillis(repairTimeout);
    retryDelayMillis = retryDelayTimeUnit.toMillis(retryDelay);
  }


  /**
   * Consult storage to see if any repairs are running, and resume those repair runs.
   *
   * @param context Reaper's application context.
   */
  public void resumeRunningRepairRuns(AppContext context) {
    Collection<RepairRun> running =
        context.storage.getRepairRunsWithState(RepairRun.RunState.RUNNING);
    for (RepairRun repairRun : running) {
      Collection<RepairSegment> runningSegments =
          context.storage.getSegmentsWithState(repairRun.getId(), RepairSegment.State.RUNNING);
      for (RepairSegment segment : runningSegments) {
        try {
          SegmentRunner.abort(context, segment,
                              context.jmxConnectionFactory.connect(segment.getCoordinatorHost()));
        } catch (ReaperException e) {
          LOG.debug("Tried to abort repair on segment {} marked as RUNNING, but the host was down"
                    + " (so abortion won't be needed)", segment.getId());
          SegmentRunner.postpone(context, segment);
        }
      }
      startRepairRun(context, repairRun);
    }
    Collection<RepairRun> paused =
        context.storage.getRepairRunsWithState(RepairRun.RunState.PAUSED);
    for (RepairRun pausedRepairRun : paused) {
      startRepairRun(context, pausedRepairRun);
    }
  }

  public void startRepairRun(AppContext context, RepairRun runToBeStarted) {
    assert null != executor : "you need to initialize the thread pool first";
    long runId = runToBeStarted.getId();
    LOG.info("Starting a run with id #{} with current state '{}'",
             runId, runToBeStarted.getRunState());
    if (runToBeStarted.getRunState() == RepairRun.RunState.NOT_STARTED) {
      RepairRun updatedRun = runToBeStarted.with()
          .runState(RepairRun.RunState.RUNNING)
          .startTime(DateTime.now())
          .build(runToBeStarted.getId());
      if (!context.storage.updateRepairRun(updatedRun)) {
        throw new RuntimeException("failed updating repair run " + updatedRun.getId());
      }
    } else if (runToBeStarted.getRunState() == RepairRun.RunState.PAUSED) {
      RepairRun updatedRun = runToBeStarted.with()
          .runState(RepairRun.RunState.RUNNING)
          .pauseTime(null)
          .build(runToBeStarted.getId());
      if (!context.storage.updateRepairRun(updatedRun)) {
        throw new RuntimeException("failed updating repair run " + updatedRun.getId());
      }
    } else if (runToBeStarted.getRunState() == RepairRun.RunState.RUNNING) {
      assert !repairRunners.containsKey(runId) :
          "trying to re-schedule run that is already running, with id " + runId;
      LOG.info("re-scheduling a running run after restart, with id " + runId);
    } else {
      throw new RuntimeException("cannot start run with state: " + runId);
    }
    if (!repairRunners.containsKey(runId)) {
      LOG.info("scheduling repair for repair run #{}", runId);
      try {
        RepairRunner newRunner = new RepairRunner(context, runId);
        repairRunners.put(runId, newRunner);
        executor.submit(newRunner);
      } catch (ReaperException e) {
        e.printStackTrace();
        LOG.warn("Failed to schedule repair for repair run #{}", runId);
      }
    } else {
      LOG.info("there is already a repair runner for run with id {}, so not scheduling new runner",
               runId);
      assert runToBeStarted.getRunState() == RepairRun.RunState.PAUSED :
          "only paused run should not be scheduled for run already...";
    }
  }

  public void pauseRepairRun(AppContext context, RepairRun runToBePaused) {
    RepairRun updatedRun = runToBePaused.with()
        .runState(RepairRun.RunState.PAUSED)
        .pauseTime(DateTime.now())
        .build(runToBePaused.getId());
    if (!context.storage.updateRepairRun(updatedRun)) {
      throw new RuntimeException("failed updating repair run " + updatedRun.getId());
    }
  }

  public void scheduleRetry(RepairRunner runner) {
    executor.schedule(runner, retryDelayMillis, TimeUnit.MILLISECONDS);
  }

  public void scheduleNextRun(RepairRunner runner, long delay) {
    executor.schedule(runner, delay, TimeUnit.MILLISECONDS);
  }

  public void removeRunner(RepairRunner runner) {
    repairRunners.remove(runner.getRepairRunId());
  }
}
