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

import com.spotify.reaper.ReaperException;
import com.spotify.reaper.cassandra.JmxProxy;
import com.spotify.reaper.cassandra.RepairStatusHandler;
import com.spotify.reaper.core.ColumnFamily;
import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.core.RepairSegment;
import com.spotify.reaper.storage.IStorage;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RepairRunner implements Runnable {
  // TODO: test
  // TODO: handle failed storage updates

  private static final Logger LOG = LoggerFactory.getLogger(RepairRunner.class);

  private static final int JMX_FAILURE_SLEEP_DELAY_SECONDS = 30;

  private static ScheduledExecutorService executor = null;
  private static long repairTimeoutMillis;

  public static void initializeThreadPool(int threadAmount, long repairTimeout, TimeUnit timeUnit) {
    executor = Executors.newScheduledThreadPool(threadAmount);
    RepairRunner.repairTimeoutMillis = timeUnit.toMillis(repairTimeout);
  }

  public static void startNewRepairRun(IStorage storage, long repairRunID,
      JmxConnectionFactory jmxConnectionFactory) {
    // TODO: make sure that no more than one RepairRunner is created per RepairRun
    assert null != executor : "you need to initialize the thread pool first";
    LOG.info("scheduling repair for repair run #{}", repairRunID);
    try {
      executor.submit(new RepairRunner(storage, repairRunID, jmxConnectionFactory));
    } catch (ReaperException e) {
      e.printStackTrace();
      LOG.warn("Failed to schedule repair for repair run #{}", repairRunID);
    }
  }


  private final IStorage storage;
  private final long repairRunId;
  private final JmxConnectionFactory jmxConnectionFactory;
  private JmxProxy jmxConnection;

  @VisibleForTesting
  RepairRunner(IStorage storage, long repairRunId, JmxConnectionFactory jmxConnectionFactory)
      throws ReaperException {
    this.storage = storage;
    this.repairRunId = repairRunId;
    this.jmxConnectionFactory = jmxConnectionFactory;
    jmxConnection = this.jmxConnectionFactory.connectAny(Optional.<RepairStatusHandler>absent(),
        storage.getCluster(storage.getRepairRun(repairRunId).getClusterName()).getSeedHosts());
  }

  /**
   * Starts/resumes a repair run that is supposed to run.
   */
  @Override
  public void run() {
    // TODO: just like SegmentRunner, RepairRunner should probably be blocking.
    // TODO: the best way to do that is probably to remove the Runnable interface and do everything
    // TODO: in a while loop.
    RepairRun.RunState state = storage.getRepairRun(repairRunId).getRunState();
    LOG.debug("run() called for repair run #{} with run state {}", repairRunId, state);
    switch (state) {
      case NOT_STARTED:
        start();
        break;
      case RUNNING:
        startNextSegment();
        break;
      case PAUSED:
        // Do nothing
        break;
      case DONE:
        // Do nothing
        break;
    }
  }

  /**
   * Starts the repair run.
   */
  private void start() {
    LOG.info("Repairs for repair run #{} starting", repairRunId);
    RepairRun repairRun = storage.getRepairRun(repairRunId);
    storage.updateRepairRun(repairRun.with()
        .runState(RepairRun.RunState.RUNNING)
        .startTime(DateTime.now())
        .build(repairRun.getId()));
    startNextSegment();
  }

  /**
   * Concludes the repair run.
   */
  private void end() {
    LOG.info("Repairs for repair run #{} done", repairRunId);
    RepairRun repairRun = storage.getRepairRun(repairRunId);
    storage.updateRepairRun(repairRun.with()
        .runState(RepairRun.RunState.DONE)
        .endTime(DateTime.now())
        .build(repairRun.getId()));
  }

  /**
   * If no segment has the state RUNNING, start the next repair. Otherwise, mark the RUNNING segment
   * as NOT_STARTED to queue it up for a retry.
   */
  private void startNextSegment() {
    // Currently not allowing parallel repairs.
    assert storage.getSegmentAmountForRepairRun(repairRunId, RepairSegment.State.RUNNING) == 0;
    RepairSegment next = storage.getNextFreeSegment(repairRunId);
    if (next != null) {
      doRepairSegment(next.getId(), next.getTokenRange());
    } else {
      end();
    }
  }

  /**
   * Start the repair of a segment.
   *
   * @param segmentId id of the segment to repair.
   * @param tokenRange token range of the segment to repair.
   */
  private void doRepairSegment(long segmentId, RingRange tokenRange) {
    ColumnFamily columnFamily =
        storage.getColumnFamily(storage.getRepairRun(repairRunId).getColumnFamilyId());
    String keyspace = columnFamily.getKeyspaceName();

    if (!jmxConnection.isConnectionAlive()) {
      try {
        LOG.debug("reestablishing JMX proxy for repair runner on run id: {}", repairRunId);
        jmxConnection = jmxConnectionFactory.connectAny(Optional.<RepairStatusHandler>absent(),
            storage.getCluster(storage.getRepairRun(repairRunId).getClusterName()).getSeedHosts());
      } catch (ReaperException e) {
        e.printStackTrace();
        LOG.warn("Failed to reestablish JMX connection in runner #{}, reattempting in {} seconds",
            repairRunId, JMX_FAILURE_SLEEP_DELAY_SECONDS);
        executor.schedule(this, JMX_FAILURE_SLEEP_DELAY_SECONDS, TimeUnit.SECONDS);
        return;
      }
      LOG.info("successfully reestablished JMX proxy for repair runner on run id: {}", repairRunId);
    }

    List<String> potentialCoordinators = jmxConnection.tokenRangeToEndpoint(keyspace, tokenRange);
    if (potentialCoordinators == null) {
      // This segment has a faulty token range. Abort the entire repair run.
      RepairRun repairRun = storage.getRepairRun(repairRunId);
      storage.updateRepairRun(repairRun.with()
          .runState(RepairRun.RunState.ERROR)
          .build(repairRun.getId()));
      return;
    }

    try {
      SegmentRunner.triggerRepair(storage, segmentId, potentialCoordinators, repairTimeoutMillis,
          jmxConnectionFactory);
    } catch (ReaperException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    handleResult(segmentId);
  }

  private void handleResult(long segmentId) {
    RepairSegment segment = storage.getRepairSegment(segmentId);
    RepairSegment.State state = segment.getState();
    LOG.debug("In repair run #{}, triggerRepair on segment {} terminated with state {}",
        repairRunId, segmentId, state);
    switch (state) {
      case NOT_STARTED:
        // Repair timed out
        executor.submit(this);
        break;
      case DONE:
        // Successful repair
        executor.schedule(this, intensityBasedDelayMillis(segment), TimeUnit.MILLISECONDS);
        break;
      case ERROR:
        // Unsuccessful repair
        executor.schedule(this, intensityBasedDelayMillis(segment), TimeUnit.MILLISECONDS);
        break;
      case RUNNING:
        // Another thread has started a new repair on this segment already
        // Or maybe the same repair segment id should never be re-run in which case this is an error
        executor.submit(this);
    }
  }

  /**
   * Calculate the delay that should be used before starting the next repair segment.
   *
   * @param repairSegment the last finished repair segment.
   * @return the delay in milliseconds.
   */
  long intensityBasedDelayMillis(RepairSegment repairSegment) {
    RepairRun repairRun = storage.getRepairRun(repairRunId);
    assert repairSegment.getEndTime() != null && repairSegment.getStartTime() != null;
    long repairEnd = repairSegment.getEndTime().getMillis();
    long repairStart = repairSegment.getStartTime().getMillis();
    long repairDuration = repairEnd - repairStart;
    long delay = (long) (repairDuration / repairRun.getIntensity() - repairDuration);
    LOG.debug("Scheduling next runner run() with delay {} ms", delay);
    return delay;
  }
}
