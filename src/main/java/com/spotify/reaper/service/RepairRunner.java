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
import org.joda.time.Seconds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * RepairRunner controls single RepairRun, is invoked in scheduled manner on separate thread, and
 * dies when the RepairRun in question is complete.
 *
 * State of the RepairRun is in the Reaper storage, so if Reaper service is restarted, new
 * RepairRunner will be spawned upon restart.
 */
public class RepairRunner implements Runnable, RepairStatusHandler {

  private static final Logger LOG = LoggerFactory.getLogger(RepairRunner.class);

  private static final int JMX_FAILURE_SLEEP_DELAY_SEC = 30;

  private static ScheduledExecutorService executor = null;

  public static void initializeThreadPool(int threadAmount) {
    executor = Executors.newScheduledThreadPool(threadAmount);
  }

  private final IStorage storage;
  private RepairRun repairRun;
  private final String clusterSeedHost;
  private JmxProxy jmxProxy = null;

  private RepairSegment currentSegment;

  // Based on the repair intensity for current run,
  // the segment repair might have some delay between segments.
  private DateTime startNextSegmentEarliest = DateTime.now();

  private RepairRunner(IStorage storage, RepairRun repairRun, String clusterSeedHost) {
    this.storage = storage;
    this.repairRun = repairRun;
    this.clusterSeedHost = clusterSeedHost;
  }

  public static void startNewRepairRun(IStorage storage, RepairRun repairRun,
                                       String clusterSeedHost) {
    assert null != executor : "you need to initialize the thread pool first";
    LOG.info("scheduling repair for repair run #" + repairRun.getId());
    executor.schedule(new RepairRunner(storage, repairRun, clusterSeedHost), 0, TimeUnit.SECONDS);
  }

  /**
   * This run() method is run in scheduled manner, so don't make this blocking!
   *
   * NOTICE: Do scheduling next execution only in this method, or when starting run. Otherwise it is
   * a risk to have multiple parallel scheduling for same run.
   */
  @Override
  public void run() {
    LOG.debug("RepairRunner run on RepairRun \"{}\" with token range \"{}\"",
              repairRun.getId(), currentSegment == null ? "n/a" : currentSegment.getTokenRange());

    if (!checkJmxProxyInitialized()) {
      LOG.error("failed to initialize JMX proxy, retrying after {} seconds",
                JMX_FAILURE_SLEEP_DELAY_SEC);
      executor.schedule(this, JMX_FAILURE_SLEEP_DELAY_SEC, TimeUnit.SECONDS);
      // TODO: should we change current segment run state to UNKNOWN now?
      return;
    }

    // Need to check current status from database every time, if state changed etc.
    repairRun = storage.getRepairRun(repairRun.getId());
    RepairRun.RunState runState = repairRun.getRunState();

    switch (runState) {
      case NOT_STARTED:
        checkIfNeedToStartNextSegmentSync();
        break;
      case RUNNING:
        checkIfNeedToStartNextSegmentSync();
        break;
      case ERROR:
        LOG.warn("repair run {} in ERROR, not doing anything", repairRun.getId());
        return; // no new run scheduling
      case PAUSED:
        checkIfNeedToStartNextSegmentSync();
        startNextSegmentEarliest = DateTime.now().plusSeconds(10);
        break;
      case DONE:
        checkIfNeedToStartNextSegmentSync();
        return; // no new run scheduling
    }

    int sleepTime = Seconds.secondsBetween(DateTime.now(), startNextSegmentEarliest).getSeconds();
    sleepTime = sleepTime > 0 ? sleepTime : 1;
    executor.schedule(this, sleepTime, TimeUnit.SECONDS);
  }

  private boolean checkJmxProxyInitialized() {
    if (null == jmxProxy || !jmxProxy.isConnectionAlive()) {
      LOG.info("initializing new JMX proxy for repair runner on run id: {}", repairRun.getId());
      try {
        jmxProxy = JmxProxy.connect(Optional.<RepairStatusHandler>of(this), clusterSeedHost);
      } catch (ReaperException e) {
        e.printStackTrace();
        return false;
      }
    }
    return true;
  }

  private void checkIfNeedToStartNextSegmentSync() {
    // We don't want to mutate the currentSegment or repairRun in parallel with this function.
    synchronized (this) {
      checkIfNeedToStartNextSegment();
    }
  }

  private void checkIfNeedToStartNextSegment() {
    if (repairRun.getRunState() == RepairRun.RunState.PAUSED
        || repairRun.getRunState() == RepairRun.RunState.DONE
        || repairRun.getRunState() == RepairRun.RunState.ERROR) {
      LOG.debug("not starting new segment if repair run (id {}) is not running: {}",
                repairRun.getId(), repairRun.getRunState());
      return;
    }

    int newRepairCommandId = -1;
    if (null == currentSegment) {
      currentSegment = storage.getNextFreeSegment(repairRun.getId());
      if (null == currentSegment) {
        LOG.error("first segment not found for repair run {}", repairRun.getId());
        changeCurrentRepairRunState(RepairRun.RunState.ERROR);
        return;
      }
      if (repairRun.getRunState() == RepairRun.RunState.NOT_STARTED) {
        LOG.info("started new repair run {}", repairRun.getId());
        changeCurrentRepairRunState(RepairRun.RunState.RUNNING);
      } else {
        assert repairRun.getRunState() == RepairRun.RunState.RUNNING : "logical error in run state";
        LOG.info("started existing repair run {}", repairRun.getId());
      }
    } else {
      LOG.debug("checking whether we need to start new segment ({}) on run: {}",
                currentSegment.getId(), repairRun.getId());
      currentSegment = storage.getRepairSegment(currentSegment.getId());

      if (currentSegment.getState() == RepairSegment.State.RUNNING) {
        LOG.info("segment {} still running on run {}", currentSegment.getId(), repairRun.getId());
      } else if (currentSegment.getState() == RepairSegment.State.ERROR) {
        LOG.error("current segment {} in ERROR status for run {}",
                  currentSegment.getId(), repairRun.getId());
        changeCurrentRepairRunState(RepairRun.RunState.ERROR);
        return;
      } else if (currentSegment.getState() == RepairSegment.State.NOT_STARTED &&
                 DateTime.now().isAfter(startNextSegmentEarliest)) {
        LOG.info("triggering repair on segment #{} with token range {} on run id {}",
                 currentSegment.getId(), currentSegment.getTokenRange(), repairRun.getId());
        try {
          newRepairCommandId = triggerRepair(currentSegment);
        } catch (ReaperException e) {
          LOG.error("failed triggering repair on segment #{} with token range {} on run id {}",
                    currentSegment.getId(), currentSegment.getTokenRange(), repairRun.getId());
        }
      } else if (currentSegment.getState() == RepairSegment.State.DONE) {
        LOG.warn("segment {} repair completed for run {}",
                 currentSegment.getId(), repairRun.getId());

        int repairTime =
            Seconds.secondsBetween(currentSegment.getStartTime(), currentSegment.getEndTime())
                .getSeconds();
        int sleepTime = (int) (repairTime / repairRun.getIntensity() - repairTime);
        startNextSegmentEarliest = DateTime.now().plusSeconds(sleepTime);

        currentSegment = storage.getNextFreeSegment(repairRun.getId());
        if (null == currentSegment) {
          LOG.info("no new free segment found for repair run {}", repairRun.getId());
          changeCurrentRepairRunState(RepairRun.RunState.DONE);
          return;
        }
      }
    }

    if (newRepairCommandId > 0) {
      // Notice that the segment state is set separately by the JMX notifications, not here
      currentSegment = RepairSegment.getCopy(currentSegment,
                                             currentSegment.getState(),
                                             newRepairCommandId,
                                             currentSegment.getStartTime(),
                                             currentSegment.getEndTime());
      if (storage.updateRepairSegment(currentSegment)) {
        LOG.debug("updated segment {} repair command id to {}",
                  currentSegment.getId(), newRepairCommandId);
      } else {
        LOG.error("failed to update segment {} repair command id to {}",
                  currentSegment.getId(), newRepairCommandId);
        // TODO: what should we do if we fail to update storage?
      }
    }
  }

  private int triggerRepair(RepairSegment segment) throws ReaperException {
    ColumnFamily columnFamily = this.storage.getColumnFamily(segment.getColumnFamilyId());

    // Make sure that we connect to a node that can act as coordinator for this repair.
    List<String>
        potentialCoordinators =
        jmxProxy.tokenRangeToEndpoint(columnFamily.getKeyspaceName(), segment.getTokenRange());

    jmxProxy.close();
    // TODO: What if the coordinator doesn't use the default JMX port?
    jmxProxy =
        JmxProxy.connect(Optional.<RepairStatusHandler>of(this), potentialCoordinators.get(0));
    return jmxProxy
        .triggerRepair(segment.getTokenRange().getStart(), segment.getTokenRange().getEnd(),
                       columnFamily.getKeyspaceName(), columnFamily.getName());
  }

  private void changeCurrentRepairRunState(RepairRun.RunState newRunState) {
    if (repairRun.getRunState() == newRunState) {
      LOG.info("repair run {} state {} same as before, not changed",
               repairRun.getId(), newRunState);
      return;
    }
    DateTime newStartTime = repairRun.getStartTime();
    DateTime newEndTime = repairRun.getEndTime();
    if (newRunState == RepairRun.RunState.DONE || newRunState == RepairRun.RunState.ERROR) {
      newEndTime = DateTime.now();
      if (null == newStartTime) {
        LOG.warn("repair run start time not set when closing run {}, setting it to current time",
                 repairRun.getId());
        newStartTime = newEndTime;
      }
    } else if (newRunState == RepairRun.RunState.RUNNING) {
      newStartTime = DateTime.now();
      newEndTime = null;
    }

    LOG.info("repair run with id {} state change from {} to {}",
             repairRun.getId(), repairRun.getRunState().toString(), newRunState.toString());
    RepairRun updatedRun = RepairRun.getCopy(repairRun, newRunState, newStartTime, newEndTime,
                                             repairRun.getCompletedSegments());
    if (!storage.updateRepairRun(updatedRun)) {
      LOG.error("failed updating repair run status: {}", repairRun.getId());
      // TODO: what should we do if we fail to update storage?
    }
  }

  /**
   * Called when there is an event coming from JMX regarding on-going repairs.
   *
   * @param repairNumber repair sequence number, obtained when triggering a repair
   * @param status       new status of the repair (STARTED, SESSION_SUCCESS, SESSION_FAILED,
   *                     FINISHED)
   * @param message      additional information about the repair
   */
  @Override
  public void handle(int repairNumber, ActiveRepairService.Status status, String message) {
    // We don't want to mutate the currentSegment or repairRun in parallel with this function.
    synchronized (this) {
      LOG.debug("handling event: repairNumber = {}, status = {}, message = \"{}\"",
                repairNumber, status, message);
      int currentCommandId = null == currentSegment ? -1 : currentSegment.getRepairCommandId();
      if (currentCommandId != repairNumber) {
        LOG.debug("got event on non-matching repair command id {}, expecting {}",
                  repairNumber, currentCommandId);
        return;
      }

      // See status explanations from: https://wiki.apache.org/cassandra/RepairAsyncAPI
      switch (status) {
        case STARTED:
          LOG.info("repair with number {} started", repairNumber);
          //changeCurrentSegmentState(RepairSegment.State.RUNNING);
          currentSegment =
              RepairSegment.getCopy(currentSegment, RepairSegment.State.RUNNING,
                                    currentSegment.getRepairCommandId(), DateTime.now(), null);
          storage.updateRepairSegment(currentSegment);
          break;
        case SESSION_SUCCESS:
          LOG.warn("repair with number {} got SESSION_SUCCESS state, "
                   + "which is NOT HANDLED CURRENTLY", repairNumber);
          break;
        case SESSION_FAILED:
          LOG.warn("repair with number {} got SESSION_FAILED state, "
                   + "setting state to error", repairNumber);
          changeCurrentSegmentState(RepairSegment.State.ERROR);
          break;
        case FINISHED:
          LOG.info("repair with number {} finished", repairNumber);
          //changeCurrentSegmentState(RepairSegment.State.DONE);
          currentSegment =
              RepairSegment.getCopy(currentSegment, RepairSegment.State.DONE,
                                    currentSegment.getRepairCommandId(),
                                    currentSegment.getStartTime(), DateTime.now());
          storage.updateRepairSegment(currentSegment);
          repairRun =
              RepairRun.getCopy(repairRun, repairRun.getRunState(), repairRun.getStartTime(),
                                repairRun.getEndTime(), repairRun.getCompletedSegments() + 1);
          storage.updateRepairRun(repairRun);
          checkIfNeedToStartNextSegment();
          break;
      }
    }
  }

  private void changeCurrentSegmentState(RepairSegment.State newState) {
    if (currentSegment.getState() == newState) {
      LOG.info("repair segment {} state {} same as before, not changed",
               currentSegment.getId(), newState);
      return;
    }
    DateTime newStartTime = currentSegment.getStartTime();
    DateTime newEndTime = currentSegment.getEndTime();
    currentSegment = RepairSegment.getCopy(currentSegment, newState,
                                           currentSegment.getRepairCommandId(),
                                           newStartTime, newEndTime);
    if (storage.updateRepairSegment(currentSegment)) {
      LOG.info("updated segment {} state to {}", currentSegment.getId(), newState);
    } else {
      LOG.error("failed to update segment {} state to {}",
                currentSegment.getId(), newState);
      // TODO: what should we do if we fail to update storage?
    }
  }

}
