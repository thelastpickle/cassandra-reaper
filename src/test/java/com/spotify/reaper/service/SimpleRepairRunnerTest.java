package com.spotify.reaper.service;

import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.storage.IStorage;
import com.spotify.reaper.storage.MemoryStorage;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SimpleRepairRunnerTest {

  IStorage storage;

  @Before
  public void setUp() throws Exception {
    storage = new MemoryStorage();
  }

  @Test
  public void runTest() throws InterruptedException {
    final int RUN_ID = 1;
    final int CF_ID = 1;
    final double INTENSITY = 0.5f;
    final long TIME_CREATION = 41l;
    final long TIME_START = 42l;
    final long TIME_END = 43l;

    // place a dummy repair run into the storage
    DateTimeUtils.setCurrentMillisFixed(TIME_CREATION);
    RepairRun.Builder runBuilder = new RepairRun.Builder("TestCluster", CF_ID,
        RepairRun.RunState.NOT_STARTED, DateTime.now(), INTENSITY);
    storage.addRepairRun(runBuilder);

    // start the repair
    DateTimeUtils.setCurrentMillisFixed(TIME_START);
    SimpleRepairRunner.initializeThreadPool(1, 180);
    SimpleRepairRunner.startNewRepairRun(storage, RUN_ID);
    Thread.sleep(200);

    // check if the start time was properly set
    DateTime startTime = storage.getRepairRun(RUN_ID).getStartTime();
    assertNotNull(startTime);
    assertEquals(TIME_START, startTime.getMillis());

    // end the repair
    DateTimeUtils.setCurrentMillisFixed(TIME_END);
    RepairRun run = storage.getRepairRun(RUN_ID);
    storage.updateRepairRun(run.with().runState(RepairRun.RunState.DONE).build(RUN_ID));
    SimpleRepairRunner.startNewRepairRun(storage, RUN_ID);
    Thread.sleep(200);

    // check if the end time was properly set
    DateTime endTime = storage.getRepairRun(RUN_ID).getEndTime();
    assertNotNull(endTime);
    assertEquals(TIME_END, endTime.getMillis());
  }

}
