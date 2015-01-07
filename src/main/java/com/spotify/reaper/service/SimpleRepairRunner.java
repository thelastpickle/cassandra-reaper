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

import com.google.common.base.Optional;

import com.spotify.reaper.ReaperException;
import com.spotify.reaper.cassandra.JmxProxy;
import com.spotify.reaper.cassandra.RepairStatusHandler;
import com.spotify.reaper.core.ColumnFamily;
import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.core.RepairSegment;
import com.spotify.reaper.storage.IStorage;

import org.apache.cassandra.service.ActiveRepairService;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * A repair runner that only triggers one segment repair at a time.
 */
public class SimpleRepairRunner implements Runnable, RepairStatusHandler {
  // TODO: test
  // TODO: logging
  // TODO: handle failed storage updates

  private static final Logger LOG = LoggerFactory.getLogger(SimpleRepairRunner.class);

  private static final int JMX_FAILURE_SLEEP_DELAY_SECONDS = 30;
  private static final int REPAIR_TIMEOUT_HOURS = 3;

  private static ScheduledExecutorService executor = null;

  public static void initializeThreadPool(int threadAmount) {
    executor = Executors.newScheduledThreadPool(threadAmount);
  }

  /**
   * Consult storage to see if any repairs are running, and resume those repair runs.
   * @param storage Reaper's internal storage.
   */
  public static void resumeRunningRepairRuns(IStorage storage) {
    // TODO: make sure that not more than one RepairRunner is created per RepairRun
    assert null != executor : "you need to initialize the thread pool first";
    Collection<RepairRun> repairRuns = storage.getAllRunningRepairRuns();
    for (RepairRun repairRun : repairRuns) {
      executor.schedule(new SimpleRepairRunner(storage, repairRun.getId()), 0, TimeUnit.SECONDS);
    }
  }

  public static void startNewRepairRun(IStorage storage, long repairRunID) {
    assert null != executor : "you need to initialize the thread pool first";
    LOG.info("scheduling repair for repair run #{}", repairRunID);
    executor.schedule(new SimpleRepairRunner(storage, repairRunID), 0, TimeUnit.SECONDS);
  }


  private final IStorage storage;
  private final long repairRunId;

  // These fields are only set when a segment is being repaired.
  // TODO: bundle them into a class?
  private ScheduledFuture<?> repairTimeout = null;
  private int currentCommandId = -1;
  private long currentSegmentId = -1;
  private JmxProxy jmxConnection = null;

  private SimpleRepairRunner(IStorage storage, long repairRunId) {
    this.storage = storage;
    this.repairRunId = repairRunId;
  }



  /**
   * Starts/resumes a repair run that is supposed to run.
   */
  @Override
  public void run() {
    RepairRun.RunState state = storage.getRepairRun(repairRunId).getRunState();
    LOG.debug("run() called with repairRunId #{} and run state {}", repairRunId, state);
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
        LOG.info("Repairs for repair run #{} done", repairRunId);
        // Do nothing
        break;
    }
  }

  /**
   * Starts the repair run.
   */
  private void start() {
    RepairRun repairRun = storage.getRepairRun(repairRunId);
    storage.updateRepairRun(
        repairRun.with().runState(RepairRun.RunState.RUNNING).build(repairRun.getId()));
    startNextSegment();
  }

  /**
   * If no segment has the state RUNNING, start the next repair. Otherwise, mark the RUNNING
   * segment as NOT_STARTED to queue it up for a retry.
   */
  private void startNextSegment() {
    RepairSegment running = storage.getTheRunningSegment(repairRunId);
    if (running != null) {
      handleRunningRepairSegment(running);
    } else {
      assert !repairIsTriggered();
      RepairSegment next = storage.getNextFreeSegment(repairRunId);
      if (next != null) {
        doRepairSegment(next);
      } else {
        RepairRun repairRun = storage.getRepairRun(repairRunId);
        storage.updateRepairRun(
            repairRun.with().runState(RepairRun.RunState.DONE).build(repairRun.getId()));
        run();
      }
    }
  }

  /**
   * Set the running repair segment back to NOT_STARTED, either now or later, based on need to wait
   * for timeout.
   * @param running the running repair segment.
   */
  public void handleRunningRepairSegment(RepairSegment running) {
    if (repairIsTriggered()) {
      // Implies that repair has timed out.
      assert repairTimeout.isDone();
      closeRepairCommand();
      storage.updateRepairSegment(
          running.with().state(RepairSegment.State.NOT_STARTED).build(running.getId()));
      run();
    } else {
      // The repair might not have finished, so let it timeout before resetting its status.
      // This may happen if the RepairRunner was created for an incomplete repair run.
      LOG.warn("Scheduling next segment to restart after {} hours", REPAIR_TIMEOUT_HOURS);
      repairTimeout = executor.schedule(this, REPAIR_TIMEOUT_HOURS, TimeUnit.HOURS);
    }
  }

  /**
   * Start the repair of a segment.
   * @param next the segment to repair.
   */
  private synchronized void doRepairSegment(RepairSegment next) {
    // TODO: directly store the right host to contact per segment (or per run, if we guarantee that
    // TODO: one host can coordinate all repair segments).
    Set<String> seeds =
        storage.getCluster(storage.getRepairRun(repairRunId).getClusterName()).getSeedHosts();

    JmxProxy jmxProxy;
    try {
      jmxProxy = JmxProxy.connect(seeds.iterator().next());
    } catch (ReaperException e) {
      e.printStackTrace();
      executor.schedule(this, JMX_FAILURE_SLEEP_DELAY_SECONDS, TimeUnit.SECONDS);
      return;
    }

    ColumnFamily
        columnFamily =
        storage.getColumnFamily(storage.getRepairRun(repairRunId).getColumnFamilyId());
    String keyspace = columnFamily.getKeyspaceName();

    List<String> potentialCoordinators =
        jmxProxy.tokenRangeToEndpoint(keyspace,
                                      storage.getNextFreeSegment(repairRunId).getTokenRange());
    if (potentialCoordinators == null) {
      // This segment has a faulty token range. Abort the entire repair run.
      RepairRun repairRun = storage.getRepairRun(repairRunId);
      storage.updateRepairRun(
          repairRun.with().runState(RepairRun.RunState.ERROR).build(repairRun.getId()));
      return;
    }

    // Connect to a node that can act as coordinator for the new repair.
    try {
      jmxProxy.close();
      jmxConnection =
          JmxProxy
              .connect(Optional.<RepairStatusHandler>of(this), potentialCoordinators.get(0));
    } catch (ReaperException e) {
      e.printStackTrace();
      executor.schedule(this, JMX_FAILURE_SLEEP_DELAY_SECONDS, TimeUnit.SECONDS);
      return;
    }

    currentSegmentId = next.getId();
    repairTimeout = executor.schedule(this, REPAIR_TIMEOUT_HOURS, TimeUnit.HOURS);
    // TODO: ensure that no repair is already running (abort all repairs)
    currentCommandId = jmxConnection
        .triggerRepair(next.getStartToken(), next.getEndToken(), keyspace, columnFamily.getName());
    LOG.debug("Triggered repair with command id {}", currentCommandId);
    storage.updateRepairSegment(
        next.with().state(RepairSegment.State.RUNNING).repairCommandId(currentCommandId)
            .build(currentSegmentId));
  }

  @Override
  public synchronized void handle(int repairNumber, ActiveRepairService.Status status, String message) {
    LOG.debug("handle called with repairRunId {}, repairNumber {} and status {}", repairRunId, repairNumber, status);
    if (repairNumber != currentCommandId) {
      LOG.warn("Repair run id != current command id. {} != {}", repairNumber, currentCommandId);
      // bj0rn: Should this ever be allowed to happen? Perhaps shut down Reaper, because repairs
      // are happening outside of Reaper?
      //throw new ReaperException("Other repairs outside of reaper's control are happening");
      return;
    }

    // if !repairIsTriggered, repair has timed out.
    if (repairIsTriggered()) {
      RepairSegment currentSegment = storage.getRepairSegment(currentSegmentId);
      // See status explanations from: https://wiki.apache.org/cassandra/RepairAsyncAPI
      switch (status) {
        case STARTED:
          DateTime now = DateTime.now();
          storage.updateRepairSegment(currentSegment.with().startTime(now).build(currentSegmentId));
          // We already set the state of the segment to RUNNING.
          break;
        case SESSION_SUCCESS:
          // This gets called once for each column family if multiple cf's were specified in a
          // single repair command.
          break;
        case SESSION_FAILED: {
          // How should we handle this? Here, it's almost treated like a success.
          RepairSegment
              updatedSegment =
              currentSegment.with().state(RepairSegment.State.ERROR).endTime(DateTime.now())
                  .build(currentSegmentId);
          storage.updateRepairSegment(updatedSegment);
          closeRepairCommand();
          executor.schedule(this, intensityBasedDelayMillis(updatedSegment), TimeUnit.MILLISECONDS);
        }
        break;
        case FINISHED: {
          RepairSegment
              updatedSegment =
              currentSegment.with().state(RepairSegment.State.DONE).endTime(DateTime.now())
                  .build(currentSegmentId);
          storage.updateRepairSegment(updatedSegment);
          closeRepairCommand();
          executor.schedule(this, intensityBasedDelayMillis(updatedSegment), TimeUnit.MILLISECONDS);
        }
        break;
      }
    }
  }

  /**
   * @return <code>true</code> if this RepairRunner has triggered a repair and is currently waiting
   * for a repair status notification from JMX.
   */
  boolean repairIsTriggered() {
    return repairTimeout != null;
  }

  /**
   * Stop countdown for repair, and stop listening for JMX notifications for the current repair.
   */
  void closeRepairCommand() {
    LOG.debug("Closing repair command with commandId {} and segmentId {}", currentCommandId,
              currentSegmentId);
    assert repairTimeout != null;

    repairTimeout.cancel(false);
    repairTimeout = null;
    currentCommandId = -1;
    currentSegmentId = -1;
    try {
      jmxConnection.close();
    } catch (ReaperException e) {
      e.printStackTrace();
    }
  }

  /**
   * Calculate the delay that should be used before starting the next repair segment.
   * @param repairSegment the last finished repair segment.
   * @return the delay in milliseconds.
   */
  long intensityBasedDelayMillis(RepairSegment repairSegment) {
    RepairRun repairRun = storage.getRepairRun(repairRunId);
    assert repairSegment.getEndTime() != null && repairSegment.getStartTime() != null;
    long repairDuration =
        repairSegment.getEndTime().getMillis() - repairSegment.getStartTime().getMillis();
    long delay = (long) (repairDuration / repairRun.getIntensity() - repairDuration);
    LOG.debug("Scheduling next runner run() with delay {} ms", delay);
    return delay;
  }
}
