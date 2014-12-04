package com.spotify.reaper.service;

import com.spotify.reaper.ReaperException;
import com.spotify.reaper.cassandra.JmxProxy;
import com.spotify.reaper.cassandra.RepairStatusHandler;
import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.core.RepairSegment;
import com.spotify.reaper.storage.IStorage;

import org.apache.cassandra.service.ActiveRepairService;
import org.joda.time.DateTime;
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

  /**
   * This run() method is run in scheduled manner, so don't make this blocking!
   */
  @Override
  public void run() {
    // TODO:
    // check status of the repair runs and segments on every run... update states etc.

    if (null == jmxProxy || !jmxProxy.isConnectionAlive()) {
      LOG.info("initializing new JMX proxy for repair runner on run id: {}", repairRun.getId());
      try {
        jmxProxy = JmxProxy.connect(clusterSeedHost);
      } catch (ReaperException e) {
        LOG.error("failed to initialize JMX proxy, retrying after {} seconds",
                  JMX_FAILURE_SLEEP_DELAY_SEC);
        e.printStackTrace();
        executor.schedule(this, JMX_FAILURE_SLEEP_DELAY_SEC, TimeUnit.SECONDS);
        return;
      }
    }

    if (null == currentSegment) {
      RepairSegment nextSegment = storage.getNextFreeSegment(repairRun.getId());
      if (null == nextSegment) {
        // TODO: check whether we are done with this repair run
        LOG.error("not implemented yet, no new segment to repair");
        return;
      }

    }

    // check if new segment must be started
    // check if paused

    long sleepTime = 0; // TODO: delay to start next segment, etc.
    executor.schedule(this, sleepTime, TimeUnit.SECONDS);
  }

  public static void startNewRepairRun(IStorage storage, RepairRun repairRun,
                                       String clusterSeedHost) {
    assert null != executor : "you need to initialize the thread pool first";
    executor.schedule(new RepairRunner(storage, repairRun, clusterSeedHost),
                      0, TimeUnit.SECONDS);
  }

  /**
   * Called when there is an event coming from JMX regarding on-going repair.
   *
   * @param repairNumber repair sequence number, obtained when triggering a repair
   * @param status new status of the repair (STARTED, SESSION_SUCCESS, SESSION_FAILED, FINISHED)
   * @param message additional information about the repair
   */
  @Override
  public void handle(int repairNumber, ActiveRepairService.Status status, String message) {
    // TODO:
  }
}
