package com.spotify.reaper.service;

import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.core.RepairSegment;

import org.joda.time.DateTime;

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
public class RepairRunner implements Runnable {

  // If we want to make this service run multiple threads in parallel,
  // maybe use Runtime.getRuntime().availableProcessors(), instead of 1.
  private static ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);

  private RepairRun repairRun;

  private RepairSegment currentSegment;
  private DateTime startNextSegment;

  private RepairRunner(RepairRun repairRun) {
    this.repairRun = repairRun;
  }

  /**
   * This run() method is run in scheduled manner, so don't make this blocking!
   */
  @Override
  public void run() {
    // TODO:
    // check status of the repair runs and segments on every run... update states etc.

    // check jmx proxy is up
    // check if new segment must be started
    // check if paused

    long sleepTime = 0; // TODO: delay to start next segment, etc.
    executor.schedule(this, sleepTime, TimeUnit.SECONDS);
  }

  public static void startNewRepairRun(RepairRun repairRun) {
    executor.schedule(new RepairRunner(repairRun), 0, TimeUnit.SECONDS);
  }

}
