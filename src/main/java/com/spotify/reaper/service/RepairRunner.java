package com.spotify.reaper.service;

import com.spotify.reaper.ReaperException;
import com.spotify.reaper.cassandra.JmxProxy;
import com.spotify.reaper.cassandra.RepairStatusHandler;
import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.core.RepairSegment;
import com.spotify.reaper.storage.IStorage;

import org.apache.cassandra.service.ActiveRepairService;
import org.joda.time.DateTime;
import org.joda.time.Seconds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * RepairRunner controls single RepairRun, works in a separate thread, and dies when the RepairRun
 * in question is complete.
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

  private IStorage storage;
  private RepairRun repairRun;
  private String clusterSeedHost;
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
    executor.schedule(new RepairRunner(storage, repairRun, clusterSeedHost), 0, TimeUnit.SECONDS);
  }

  /**
   * This run() method is run in scheduled manner, so don't make this blocking!
   */
  @Override
  public void run() {
    LOG.debug("RepairRunner run on RepairRun \"{}\" with current segment id \"{}\"",
              repairRun.getId(), currentSegment == null ? "n/a" : currentSegment.getStartToken());

    // Need to check current status from database every time, if state changed etc.
    repairRun = storage.getRepairRun(repairRun.getId());
    RepairRun.State state = repairRun.getState();

    switch (state) {
      case NOT_STARTED:
        checkIfNeedToStartNextSegment();
        break;
      case RUNNING:
        checkIfNeedToStartNextSegment();
        break;
      case PAUSED:
        startNextSegmentEarliest = DateTime.now().plusSeconds(10);
        break;
      case DONE:
        finishRepairRun();
        return;
    }

    int sleepTime = Seconds.secondsBetween(DateTime.now(), startNextSegmentEarliest).getSeconds();
    executor.schedule(this, sleepTime > 0 ? sleepTime : 1, TimeUnit.SECONDS);
  }

  private boolean checkJmxProxyInitialized() {
    if (null == jmxProxy || !jmxProxy.isConnectionAlive()) {
      LOG.info("initializing new JMX proxy for repair runner on run id: {}", repairRun.getId());
      try {
        jmxProxy = JmxProxy.connect(clusterSeedHost);
      } catch (ReaperException e) {
        LOG.error("failed to initialize JMX proxy, retrying after {} seconds",
                  JMX_FAILURE_SLEEP_DELAY_SEC);
        e.printStackTrace();
        executor.schedule(this, JMX_FAILURE_SLEEP_DELAY_SEC, TimeUnit.SECONDS);
        return false;
      }
    }
    return true;
  }

  private void checkIfNeedToStartNextSegment() {
    // TODO:
  }

  private void changeCurrentSegmentState(RepairSegment.State running) {
    // TODO:
  }

  private void finishRepairRun() {
    // TODO:
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
        changeCurrentSegmentState(RepairSegment.State.RUNNING);
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
        changeCurrentSegmentState(RepairSegment.State.DONE);
        checkIfNeedToStartNextSegment();
        break;
    }
  }

}
