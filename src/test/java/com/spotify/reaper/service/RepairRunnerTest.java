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
import com.google.common.collect.Lists;

import com.spotify.reaper.ReaperException;
import com.spotify.reaper.cassandra.JmxProxy;
import com.spotify.reaper.cassandra.RepairStatusHandler;
import com.spotify.reaper.core.ColumnFamily;
import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.core.RepairSegment;
import com.spotify.reaper.storage.IStorage;
import com.spotify.reaper.storage.MemoryStorage;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyCollectionOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RepairRunnerTest {

  @Test
  public void noSegmentsTest() throws InterruptedException {
    final int RUN_ID = 1;
    final int CF_ID = 1;
    final double INTENSITY = 0.5f;
    final long TIME_CREATION = 41l;
    final long TIME_START = 42l;

    IStorage storage = new MemoryStorage();

    // place a dummy repair run into the storage
    DateTimeUtils.setCurrentMillisFixed(TIME_CREATION);
    RepairRun.Builder runBuilder = new RepairRun.Builder("TestCluster", CF_ID,
                                                         RepairRun.RunState.NOT_STARTED,
                                                         DateTime.now(), INTENSITY);
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


  ////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////
  @Test
  public void testHangingRepair() throws ReaperException {
    final String CLUSTER_NAME = "reaper";
    final String KS_NAME = "reaper";
    final String CF_NAME = "reaper";
    final long TIME_CREATION = 41l;
    final double INTENSITY = 0.5f;

    RepairRunner.initializeThreadPool(15, 180);
    IStorage storage = new MemoryStorage();

    ColumnFamily cf =
        storage.addColumnFamily(new ColumnFamily.Builder(CLUSTER_NAME, KS_NAME, CF_NAME, 1, false));

    DateTimeUtils.setCurrentMillisFixed(TIME_CREATION);
    RepairRun repairRun = storage.addRepairRun(
        new RepairRun.Builder(CLUSTER_NAME, cf.getId(), RepairRun.RunState.NOT_STARTED,
                              DateTime.now(), INTENSITY));

    storage.addRepairSegments(Collections.singleton(
        new RepairSegment.Builder(repairRun.getId(), new RingRange(null, null), RepairSegment.State.NOT_STARTED)));

    JmxProxy jmx = mock(JmxProxy.class);
    when(jmx.getClusterName()).thenReturn(CLUSTER_NAME);
    when(jmx.isConnectionAlive()).thenReturn(true);
    when(jmx.tokenRangeToEndpoint(anyString(), any(RingRange.class))).thenReturn(
        Lists.newArrayList(""));
    when(jmx.switchNode(Matchers.<Optional<RepairStatusHandler>>any(), anyString()))
        .thenReturn(jmx);
    when(jmx.triggerRepair(any(BigInteger.class), any(BigInteger.class), anyString(), anyString())).then(
        new Answer<Integer>() {
          @Override
          public Integer answer(InvocationOnMock invocation) throws Throwable {
            return 0;
          }
        });

    RepairRunner repairRunner = new RepairRunner(storage, 1, jmx);
    repairRunner.run();
  }

  @Test
  public void testAlreadyStartedRepair() {
    // TODO
  }
}
