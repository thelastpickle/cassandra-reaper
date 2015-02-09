/*
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
package com.spotify.reaper.service;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;

import com.spotify.reaper.AppContext;
import com.spotify.reaper.ReaperException;
import com.spotify.reaper.cassandra.JmxProxy;
import com.spotify.reaper.core.Cluster;
import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.core.RepairSegment;
import com.spotify.reaper.core.RepairUnit;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RepairRunner implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(RepairRunner.class);

  private static ScheduledExecutorService executor;
  private static long repairTimeoutMillis;
  private static long retryDelayMillis;
  private final AppContext context;
  private final long repairRunId;
  private JmxProxy jmxConnection;
  private Long currentlyRunningSegmentId;

  // Caching all active RepairRunners.
  @VisibleForTesting
  public static Map<Long, RepairRunner> repairRunners = Maps.newConcurrentMap();

  private RepairRunner(AppContext context, long repairRunId)
      throws ReaperException {
    this.context = context;
    this.repairRunId = repairRunId;
    Optional<RepairRun> repairRun = context.storage.getRepairRun(repairRunId);
    assert repairRun.isPresent() : "No RepairRun with ID " + repairRunId + " found from storage";
    Optional<Cluster> cluster = context.storage.getCluster(repairRun.get().getClusterName());
    assert cluster.isPresent() : "No Cluster with name " + repairRun.get().getClusterName()
                                 + " found from storage";
  }

  public static void initializeThreadPool(int threadAmount, long repairTimeout,
                                          TimeUnit repairTimeoutTimeUnit, long retryDelay,
                                          TimeUnit retryDelayTimeUnit) {
    executor = Executors.newScheduledThreadPool(threadAmount);
    repairTimeoutMillis = repairTimeoutTimeUnit.toMillis(repairTimeout);
    retryDelayMillis = retryDelayTimeUnit.toMillis(retryDelay);
  }

  /**
   * Consult storage to see if any repairs are running, and resume those repair runs.
   *
   * @param context Reaper's application context.
   */
  public static void resumeRunningRepairRuns(AppContext context) {
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
      RepairRunner.startRepairRun(context, repairRun);
    }
    Collection<RepairRun> paused =
        context.storage.getRepairRunsWithState(RepairRun.RunState.PAUSED);
    for (RepairRun pausedRepairRun : paused) {
      RepairRunner.startRepairRun(context, pausedRepairRun);
    }
  }

  public static void startRepairRun(AppContext context, RepairRun runToBeStarted) {
    assert null != executor : "you need to initialize the thread pool first";
    long runId = runToBeStarted.getId();
    LOG.info("changing state RUNNING for run with id #{} with current state '{}'",
             runId, runToBeStarted.getRunState());
    RepairRun updatedRun;
    if (runToBeStarted.getRunState() == RepairRun.RunState.NOT_STARTED) {
      updatedRun = runToBeStarted.with()
          .runState(RepairRun.RunState.RUNNING)
          .startTime(DateTime.now())
          .build(runToBeStarted.getId());
      if (!context.storage.updateRepairRun(updatedRun)) {
        throw new RuntimeException("failed updating repair run " + updatedRun.getId());
      }
    } else if (runToBeStarted.getRunState() == RepairRun.RunState.PAUSED) {
      updatedRun = runToBeStarted.with()
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

  public static void pauseRepairRun(AppContext context, RepairRun runToBePaused) {
    RepairRun updatedRun = runToBePaused.with()
        .runState(RepairRun.RunState.PAUSED)
        .pauseTime(DateTime.now())
        .build(runToBePaused.getId());
    if (!context.storage.updateRepairRun(updatedRun)) {
      throw new RuntimeException("failed updating repair run " + updatedRun.getId());
    }
  }

    @VisibleForTesting
  public Long getCurrentlyRunningSegmentId() {
    return currentlyRunningSegmentId;
  }

  /**
   * Starts/resumes a repair run that is supposed to run.
   */
  @Override
  public void run() {
    RepairRun repairRun = context.storage.getRepairRun(repairRunId).get();
    try {
      RepairRun.RunState state = repairRun.getRunState();
      LOG.debug("run() called for repair run #{} with run state {}", repairRunId, state);
      switch (state) {
        case NOT_STARTED:
          start();
          break;
        case RUNNING:
          startNextSegment();
          break;
        case PAUSED:
          executor.schedule(this, retryDelayMillis, TimeUnit.MILLISECONDS);
          break;
        case DONE:
          // We're done. Let go of thread.
          repairRunners.remove(repairRunId);
          break;
      }
    } catch (ReaperException | RuntimeException e) {
      LOG.error("RepairRun FAILURE");
      LOG.error(e.toString());
      LOG.error(Arrays.toString(e.getStackTrace()));
      e.printStackTrace();
      context.storage.updateRepairRun(repairRun.with()
                                          .runState(RepairRun.RunState.ERROR)
                                          .endTime(DateTime.now())
                                          .build(repairRun.getId()));
      repairRunners.remove(repairRunId);
    }
  }

  /**
   * Starts the repair run.
   */
  private void start() throws ReaperException {
    LOG.info("Repairs for repair run #{} starting", repairRunId);
    RepairRun repairRun = context.storage.getRepairRun(repairRunId).get();
    boolean success = context.storage.updateRepairRun(repairRun.with()
                                                          .runState(RepairRun.RunState.RUNNING)
                                                          .startTime(DateTime.now())
                                                          .build(repairRun.getId()));
    if (!success) {
      LOG.error("failed updating repair run " + repairRun.getId());
    }
    startNextSegment();
  }

  /**
   * Concludes the repair run.
   */
  private void end() {
    LOG.info("Repairs for repair run #{} done", repairRunId);
    RepairRun repairRun = context.storage.getRepairRun(repairRunId).get();
    boolean success = context.storage.updateRepairRun(repairRun.with()
                                                          .runState(RepairRun.RunState.DONE)
                                                          .endTime(DateTime.now())
                                                          .lastEvent("All done")
                                                          .build(repairRun.getId()));
    if (!success) {
      LOG.error("failed updating repair run " + repairRun.getId());
    }
  }

  /**
   * Get the next segment and repair it. If there is none, we're done.
   */
  private void startNextSegment() throws ReaperException {
    // Currently not allowing parallel repairs.
    assert
        context.storage.getSegmentAmountForRepairRun(repairRunId, RepairSegment.State.RUNNING) == 0;
    Optional<RepairSegment> nextSegment = context.storage.getNextFreeSegment(repairRunId);
    if (nextSegment.isPresent()) {
      repairSegment(nextSegment.get().getId(), nextSegment.get().getTokenRange());
    } else {
      end();
    }
  }

  /**
   * Start the repair of a segment.
   *
   * @param segmentId  id of the segment to repair.
   * @param tokenRange token range of the segment to repair.
   */
  private void repairSegment(long segmentId, RingRange tokenRange) throws ReaperException {
    RepairRun repairRun = context.storage.getRepairRun(repairRunId).get();
    RepairUnit repairUnit = context.storage.getRepairUnit(repairRun.getRepairUnitId()).get();
    String keyspace = repairUnit.getKeyspaceName();
    LOG.debug("preparing to repair segment {} on run with id {}", segmentId, repairRun.getId());

    if (jmxConnection == null || !jmxConnection.isConnectionAlive()) {
      try {
        LOG.debug("connecting JMX proxy for repair runner on run id: {}", repairRunId);
        Cluster cluster = context.storage.getCluster(repairUnit.getClusterName()).get();
        jmxConnection = context.jmxConnectionFactory.connectAny(cluster);
      } catch (ReaperException e) {
        e.printStackTrace();
        LOG.warn("Failed to reestablish JMX connection in runner #{}, reattempting in {} seconds",
                 repairRunId, retryDelayMillis);
        executor.schedule(this, retryDelayMillis, TimeUnit.MILLISECONDS);
        return;
      }
      LOG.info("successfully reestablished JMX proxy for repair runner on run id: {}", repairRunId);
    }

    List<String> potentialCoordinators = jmxConnection.tokenRangeToEndpoint(keyspace, tokenRange);
    if (potentialCoordinators == null) {
      // This segment has a faulty token range. Abort the entire repair run.
      boolean success = context.storage.updateRepairRun(repairRun.with()
                                                            .runState(RepairRun.RunState.ERROR)
                                                            .build(repairRun.getId()));
      if (!success) {
        LOG.error("failed updating repair run " + repairRun.getId());
      }
      return;
    }

    currentlyRunningSegmentId = segmentId;
    SegmentRunner.triggerRepair(context, segmentId, potentialCoordinators, repairTimeoutMillis);
    currentlyRunningSegmentId = null;

    handleResult(segmentId);
  }

  private void handleResult(long segmentId) {
    RepairSegment segment = context.storage.getRepairSegment(segmentId).get();
    RepairSegment.State state = segment.getState();
    LOG.debug("In repair run #{}, triggerRepair on segment {} ended with state {}",
              repairRunId, segmentId, state);
    switch (state) {
      case NOT_STARTED:
        // Repair timed out
        executor.schedule(this, retryDelayMillis, TimeUnit.MILLISECONDS);
        break;
      case DONE:
        // Successful repair
        long delay = intensityBasedDelayMillis(segment);
        executor.schedule(this, delay, TimeUnit.MILLISECONDS);
        String event = String.format("Waiting %ds because of intensity based delay", delay / 1000);
        RepairRun updatedRepairRun =
            context.storage.getRepairRun(repairRunId).get().with().lastEvent(event)
                .build(repairRunId);
        context.storage.updateRepairRun(updatedRepairRun);
        break;
      default:
        // Another thread has started a new repair on this segment already
        // Or maybe the same repair segment id should never be re-run in which case this is an error
        String msg = "handleResult called with a segment state (" + state + ") that it should not"
                     + " have after segmentRunner has tried a repair";
        LOG.error(msg);
        throw new RuntimeException(msg);
    }
  }

  /**
   * Calculate the delay that should be used before starting the next repair segment.
   *
   * @param repairSegment the last finished repair segment.
   * @return the delay in milliseconds.
   */
  long intensityBasedDelayMillis(RepairSegment repairSegment) {
    RepairRun repairRun = context.storage.getRepairRun(repairRunId).get();
    assert repairSegment.getEndTime() != null && repairSegment.getStartTime() != null;
    long repairEnd = repairSegment.getEndTime().getMillis();
    long repairStart = repairSegment.getStartTime().getMillis();
    long repairDuration = repairEnd - repairStart;
    long delay = (long) (repairDuration / repairRun.getIntensity() - repairDuration);
    LOG.debug("Scheduling next runner run() with delay {} ms", delay);
    return delay;
  }
}
