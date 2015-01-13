package com.spotify.reaper.service;

import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.core.RepairSegment;
import com.spotify.reaper.storage.IStorage;
import com.spotify.reaper.storage.MemoryStorage;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class RepairRunnerTest {

  IStorage storage;

  @Before
  public void setUp() throws Exception {
    storage = new MemoryStorage();
  }

  @Test
  public void noSegmentsTest() throws InterruptedException {
    final int RUN_ID = 1;
    final int CF_ID = 1;
    final double INTENSITY = 0.5f;
    final long TIME_CREATION = 41l;
    final long TIME_START = 42l;

    // place a dummy repair run into the storage
    DateTimeUtils.setCurrentMillisFixed(TIME_CREATION);
    RepairRun.Builder runBuilder = new RepairRun.Builder("TestCluster", CF_ID,
        RepairRun.RunState.NOT_STARTED, DateTime.now(), INTENSITY);
    storage.addRepairRun(runBuilder);
    storage.addRepairSegments(Collections.<RepairSegment.Builder>emptySet(), RUN_ID);

    // start the repair
    DateTimeUtils.setCurrentMillisFixed(TIME_START);
    RepairRunner.initializeThreadPool(1, 180);
    RepairRunner.startNewRepairRun(storage, RUN_ID);
    Thread.sleep(200);

    // check if the start time was properly set
    DateTime startTime = storage.getRepairRun(RUN_ID).getStartTime();
    assertNotNull(startTime);
    assertEquals(TIME_START, startTime.getMillis());

    // end time will also be set immediately
    DateTime endTime = storage.getRepairRun(RUN_ID).getEndTime();
    assertNotNull(endTime);
    assertEquals(TIME_START, endTime.getMillis());
  }

}
