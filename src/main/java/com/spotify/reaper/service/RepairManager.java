package com.spotify.reaper.service;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.spotify.reaper.AppContext;
import com.spotify.reaper.ReaperException;
import com.spotify.reaper.cassandra.JmxProxy;
import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.core.RepairSegment;
import java.util.UUID;

public class RepairManager {

  private static final Logger LOG = LoggerFactory.getLogger(RepairManager.class);

  private ListeningScheduledExecutorService executor;
  private long repairTimeoutMillis;
  private long retryDelayMillis;

  public long getRepairTimeoutMillis() {
    return repairTimeoutMillis;
  }

  // Caching all active RepairRunners.
  @VisibleForTesting
  public Map<UUID, RepairRunner> repairRunners = Maps.newConcurrentMap();

  public void initializeThreadPool(int threadAmount, long repairTimeout,
      TimeUnit repairTimeoutTimeUnit, long retryDelay,
      TimeUnit retryDelayTimeUnit) {
    executor = MoreExecutors.listeningDecorator(Executors.newScheduledThreadPool(threadAmount,
        new NamedThreadFactory("RepairRunner")));
    repairTimeoutMillis = repairTimeoutTimeUnit.toMillis(repairTimeout);
    retryDelayMillis = retryDelayTimeUnit.toMillis(retryDelay);
  }


  /**
   * Consult storage to see if any repairs are running, and resume those repair runs.
   *
   * @param context Reaper's application context.
   * @throws ReaperException 
   */
  public void resumeRunningRepairRuns(AppContext context) throws ReaperException {
    context.storage.saveHeartbeat();
    Collection<RepairRun> running =
        context.storage.getRepairRunsWithState(RepairRun.RunState.RUNNING);
    for (RepairRun repairRun : running) {
      if(!repairRunners.containsKey(repairRun.getId())) {
        Collection<RepairSegment> runningSegments =
            context.storage.getSegmentsWithState(repairRun.getId(), RepairSegment.State.RUNNING);
        for (RepairSegment segment : runningSegments) {
          try (JmxProxy jmxProxy = context.jmxConnectionFactory
              .connect(segment.getCoordinatorHost())) {
            SegmentRunner.abort(context, segment, jmxProxy);
          } catch (ReaperException e) {
            LOG.debug("Tried to abort repair on segment {} marked as RUNNING, but the host was down"
                      + " (so abortion won't be needed)", segment.getId(), e);          
            SegmentRunner.postpone(context, segment, context.storage.getRepairUnit(repairRun.getId()));
          }
        }
        
        LOG.info("Restarting run id {} that has no runner", repairRun.getId());
        startRepairRun(context, repairRun);
      } 
    }
    Collection<RepairRun> paused =
        context.storage.getRepairRunsWithState(RepairRun.RunState.PAUSED);
    for (RepairRun pausedRepairRun : paused) {
      if(!repairRunners.containsKey(pausedRepairRun.getId())) {
        startRunner(context, pausedRepairRun.getId());
      }
    }
  }

  public RepairRun startRepairRun(AppContext context, RepairRun runToBeStarted) throws ReaperException {
    assert null != executor : "you need to initialize the thread pool first";
    UUID runId = runToBeStarted.getId();
    LOG.info("Starting a run with id #{} with current state '{}'",
        runId, runToBeStarted.getRunState());
    switch (runToBeStarted.getRunState()) {
      case NOT_STARTED: {
        RepairRun updatedRun = runToBeStarted.with()
            .runState(RepairRun.RunState.RUNNING)
            .startTime(DateTime.now())
            .build(runToBeStarted.getId());
        if (!context.storage.updateRepairRun(updatedRun)) {
          throw new ReaperException("failed updating repair run " + updatedRun.getId());
        }
        startRunner(context, runId);
        return updatedRun;
      }
      case PAUSED: {
        RepairRun updatedRun = runToBeStarted.with()
            .runState(RepairRun.RunState.RUNNING)
            .pauseTime(null)
            .build(runToBeStarted.getId());
        if (!context.storage.updateRepairRun(updatedRun)) {
          throw new ReaperException("failed updating repair run " + updatedRun.getId());
        }
        return updatedRun;
      }
      case RUNNING:
        Preconditions.checkState(!repairRunners.containsKey(runId),
            "trying to re-trigger run that is already running, with id " + runId);
        LOG.info("re-trigger a running run after restart, with id {}", runId);
        startRunner(context, runId);
        return runToBeStarted;
      case ERROR: {
        RepairRun updatedRun = runToBeStarted.with()
            .runState(RepairRun.RunState.RUNNING)
            .endTime(null)
            .build(runToBeStarted.getId());
        if (!context.storage.updateRepairRun(updatedRun)) {
          throw new ReaperException("failed updating repair run " + updatedRun.getId());
        }
        startRunner(context, runId);
        return updatedRun;
      }
      default:
        throw new ReaperException("cannot start run with state: " + runToBeStarted.getRunState());
    }
  }

  private void startRunner(AppContext context, UUID runId) {
    if (!repairRunners.containsKey(runId)) {
      LOG.info("scheduling repair for repair run #{}", runId);
      try {
        RepairRunner newRunner = new RepairRunner(context, runId);
        repairRunners.put(runId, newRunner);
        executor.submit(newRunner);
      } catch (ReaperException e) {
        LOG.warn("Failed to schedule repair for repair run #{}", runId, e);
      }
    } else {
      LOG.error(
          "there is already a repair runner for run with id {}, so not starting new runner. This "
          + "should not happen.", runId);
    }
  }

  public RepairRun pauseRepairRun(AppContext context, RepairRun runToBePaused) throws ReaperException {
    RepairRun updatedRun = runToBePaused.with()
        .runState(RepairRun.RunState.PAUSED)
        .pauseTime(DateTime.now())
        .build(runToBePaused.getId());
    if (!context.storage.updateRepairRun(updatedRun)) {
      throw new ReaperException("failed updating repair run " + updatedRun.getId());
    }
    return updatedRun;
  }

  public RepairRun abortRepairRun(AppContext context, RepairRun runToBeAborted) throws ReaperException {
    RepairRun updatedRun = runToBeAborted.with()
        .runState(RepairRun.RunState.ABORTED)
        .pauseTime(DateTime.now())
        .build(runToBeAborted.getId());
    if (!context.storage.updateRepairRun(updatedRun)) {
      throw new ReaperException("failed updating repair run " + updatedRun.getId());
    }
    return updatedRun;
  }

  public void scheduleRetry(RepairRunner runner) {
    executor.schedule(runner, retryDelayMillis, TimeUnit.MILLISECONDS);
  }

  public ListenableFuture<?> submitSegment(SegmentRunner runner) {
    return executor.submit(runner);
  }

  public void removeRunner(RepairRunner runner) {
    repairRunners.remove(runner.getRepairRunId());
  }
}
