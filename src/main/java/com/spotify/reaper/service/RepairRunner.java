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
import java.util.List;

public class RepairRunner implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(RepairRunner.class);

  private final AppContext context;
  private final long repairRunId;
  private JmxProxy jmxConnection;
  private Long currentlyRunningSegmentId;

  public RepairRunner(AppContext context, long repairRunId)
      throws ReaperException {
    this.context = context;
    this.repairRunId = repairRunId;
    Optional<RepairRun> repairRun = context.storage.getRepairRun(repairRunId);
    assert repairRun.isPresent() : "No RepairRun with ID " + repairRunId + " found from storage";
    Optional<Cluster> cluster = context.storage.getCluster(repairRun.get().getClusterName());
    assert cluster.isPresent() : "No Cluster with name " + repairRun.get().getClusterName()
                                 + " found from storage";
  }

  public long getRepairRunId() {
    return repairRunId;
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
          context.repairManager.scheduleRetry(this);
          break;
        case DONE:
          // We're done. Let go of thread.
          context.repairManager.removeRunner(this);
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
      context.repairManager.removeRunner(this);
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
        LOG.warn("Failed to reestablish JMX connection in runner #{}, retrying", repairRunId);

        context.repairManager.scheduleRetry(this);
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
    SegmentRunner.triggerRepair(context, segmentId, potentialCoordinators,
                                context.repairManager.getRepairTimeoutMillis());
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
        // Unsuccessful repair
        context.repairManager.scheduleRetry(this);
        break;
      case DONE:
        // Successful repair
        long delay = intensityBasedDelayMillis(segment);
        context.repairManager.scheduleNextRun(this, delay);
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
    if (repairSegment.getEndTime() == null && repairSegment.getStartTime() == null) {
      return 0;
    }
    else if (repairSegment.getEndTime() != null && repairSegment.getStartTime() != null) {
      long repairEnd = repairSegment.getEndTime().getMillis();
      long repairStart = repairSegment.getStartTime().getMillis();
      long repairDuration = repairEnd - repairStart;
      RepairRun repairRun = context.storage.getRepairRun(repairRunId).get();
      long delay = (long) (repairDuration / repairRun.getIntensity() - repairDuration);
      LOG.debug("Scheduling next runner run() with delay {} ms", delay);
      return delay;
    } else
    {
      LOG.error("Segment {} returned with startTime {} and endTime {}. This should not happen."
              + "Intensity cannot apply, so next run will start immediately.",
          repairSegment.getId(), repairSegment.getStartTime(), repairSegment.getEndTime());
      return 0;
    }
  }
}
