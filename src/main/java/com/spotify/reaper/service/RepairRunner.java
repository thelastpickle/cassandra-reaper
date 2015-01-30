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

import com.spotify.reaper.ReaperException;
import com.spotify.reaper.cassandra.JmxConnectionFactory;
import com.spotify.reaper.cassandra.JmxProxy;
import com.spotify.reaper.core.Cluster;
import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.core.RepairSegment;
import com.spotify.reaper.core.RepairUnit;
import com.spotify.reaper.storage.IStorage;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RepairRunner implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(RepairRunner.class);

  private static ScheduledExecutorService executor = null;
  private static long repairTimeoutMillis;
  private static long retryDelayMillis;
  private final IStorage storage;
  private final long repairRunId;
  private final JmxConnectionFactory jmxConnectionFactory;
  private JmxProxy jmxConnection;
  private Long currentlyRunningSegmentId;

  // Caching all active RepairRunners.
  @VisibleForTesting
  public static Map<Long, RepairRunner> repairRunners = Maps.newConcurrentMap();

  private RepairRunner(IStorage storage, long repairRunId,
                       JmxConnectionFactory jmxConnectionFactory)
      throws ReaperException {
    this.storage = storage;
    this.repairRunId = repairRunId;
    this.jmxConnectionFactory = jmxConnectionFactory;
    Optional<RepairRun> repairRun = storage.getRepairRun(repairRunId);
    assert repairRun.isPresent() : "No RepairRun with ID " + repairRunId + " found from storage";
    jmxConnection = this.jmxConnectionFactory.connectAny(
        storage.getCluster(repairRun.get().getClusterName()).get());
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
   * @param storage Reaper's internal storage.
   */
  public static void resumeRunningRepairRuns(IStorage storage,
                                             JmxConnectionFactory jmxConnectionFactory) {
    for (RepairRun repairRun : storage.getRepairRunsWithState(RepairRun.RunState.RUNNING)) {
      Collection<RepairSegment> runningSegments =
          storage.getSegmentsWithState(repairRun.getId(), RepairSegment.State.RUNNING);
      for (RepairSegment segment : runningSegments) {
        try {
          SegmentRunner.abort(storage, segment,
                              jmxConnectionFactory.connect(segment.getCoordinatorHost()));
        } catch (ReaperException e) {
          LOG.debug("Tried to abort repair on segment {} marked as RUNNING, but the host was down"
                    + " (so abortion won't be needed)", segment.getId());
          SegmentRunner.postpone(storage, segment);
        }
      }
      RepairRunner.startRepairRun(storage, repairRun.getId(), jmxConnectionFactory);
    }
    for (RepairRun pausedRepairRun : storage.getRepairRunsWithState(RepairRun.RunState.PAUSED)) {
      RepairRunner.startRepairRun(storage, pausedRepairRun.getId(), jmxConnectionFactory);
    }
  }

  public static void startRepairRun(IStorage storage, long repairRunID,
                                    JmxConnectionFactory jmxConnectionFactory) {
    assert null != executor : "you need to initialize the thread pool first";
    LOG.info("scheduling repair for repair run #{}", repairRunID);
    try {
      if (repairRunners.containsKey(repairRunID)) {
        throw new ReaperException("RepairRunner for repair run " + repairRunID + " already exists");
      }
      RepairRunner newRunner = new RepairRunner(storage, repairRunID, jmxConnectionFactory);
      repairRunners.put(repairRunID, newRunner);
      executor.submit(newRunner);
    } catch (ReaperException e) {
      e.printStackTrace();
      LOG.warn("Failed to schedule repair for repair run #{}", repairRunID);
    }
  }

  @VisibleForTesting
  public Long getCurrentlyRunningSegmentId() {
    return this.currentlyRunningSegmentId;
  }

  /**
   * Starts/resumes a repair run that is supposed to run.
   */
  @Override
  public void run() {
    RepairRun repairRun = storage.getRepairRun(repairRunId).get();
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
    } catch (ReaperException e) {
      LOG.error("RepairRun FAILURE");
      e.printStackTrace();
      storage.updateRepairRun(repairRun.with()
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
    RepairRun repairRun = storage.getRepairRun(repairRunId).get();
    boolean success = storage.updateRepairRun(repairRun.with()
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
    RepairRun repairRun = storage.getRepairRun(repairRunId).get();
    boolean success = storage.updateRepairRun(repairRun.with()
                                                  .runState(RepairRun.RunState.DONE)
                                                  .endTime(DateTime.now())
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
    assert storage.getSegmentAmountForRepairRun(repairRunId, RepairSegment.State.RUNNING) == 0;
    Optional<RepairSegment> nextSegment = storage.getNextFreeSegment(repairRunId);
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
    RepairRun repairRun = storage.getRepairRun(repairRunId).get();
    RepairUnit repairUnit = storage.getRepairUnit(repairRun.getRepairUnitId()).get();
    String keyspace = repairUnit.getKeyspaceName();
    LOG.debug("preparing to repair segment {} on run with id {}", segmentId, repairRun.getId());

    if (!jmxConnection.isConnectionAlive()) {
      try {
        LOG.debug("reestablishing JMX proxy for repair runner on run id: {}", repairRunId);
        Cluster cluster = storage.getCluster(repairUnit.getClusterName()).get();
        jmxConnection = jmxConnectionFactory.connectAny(cluster);
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
      boolean success = storage.updateRepairRun(repairRun.with()
                                                    .runState(RepairRun.RunState.ERROR)
                                                    .build(repairRun.getId()));
      if (!success) {
        LOG.error("failed updating repair run " + repairRun.getId());
      }
      return;
    }

    this.currentlyRunningSegmentId = Long.valueOf(segmentId);
    SegmentRunner.triggerRepair(storage, segmentId, potentialCoordinators, repairTimeoutMillis,
                                jmxConnectionFactory);

    handleResult(segmentId);
  }

  private void handleResult(long segmentId) {
    RepairSegment segment = storage.getRepairSegment(segmentId).get();
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
        executor.schedule(this, intensityBasedDelayMillis(segment), TimeUnit.MILLISECONDS);
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
    RepairRun repairRun = storage.getRepairRun(repairRunId).get();
    assert repairSegment.getEndTime() != null && repairSegment.getStartTime() != null;
    long repairEnd = repairSegment.getEndTime().getMillis();
    long repairStart = repairSegment.getStartTime().getMillis();
    long repairDuration = repairEnd - repairStart;
    long delay = (long) (repairDuration / repairRun.getIntensity() - repairDuration);
    LOG.debug("Scheduling next runner run() with delay {} ms", delay);
    return delay;
  }
}
