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
import com.spotify.reaper.core.Cluster;
import com.spotify.reaper.core.ColumnFamily;
import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.core.RepairSegment;
import com.spotify.reaper.storage.IStorage;
import com.spotify.reaper.storage.MemoryStorage;

import org.apache.cassandra.service.ActiveRepairService;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.math.BigInteger;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
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
    final String TEST_CLUSTER = "TestCluster";

    IStorage storage = new MemoryStorage();

    // place a dummy cluster into storage
    storage.addCluster(new Cluster(TEST_CLUSTER, null, Collections.<String>singleton(null)));

    // place a dummy repair run into the storage
    DateTimeUtils.setCurrentMillisFixed(TIME_CREATION);
    RepairRun.Builder runBuilder =
        new RepairRun.Builder(TEST_CLUSTER, CF_ID, RepairRun.RunState.NOT_STARTED, DateTime.now(),
            INTENSITY);
    storage.addRepairRun(runBuilder);
    storage.addRepairSegments(Collections.<RepairSegment.Builder>emptySet(), RUN_ID);

    // start the repair
    DateTimeUtils.setCurrentMillisFixed(TIME_START);
    RepairRunner.initializeThreadPool(1, 180);
    RepairRunner.startNewRepairRun(storage, RUN_ID, new JmxConnectionFactory() {
      @Override
      public JmxProxy create(Optional<RepairStatusHandler> handler, String host)
          throws ReaperException {
        return null;
      }
    });
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


  @Test
  public void testHangingRepair() throws ReaperException, InterruptedException {
    final String CLUSTER_NAME = "reaper";
    final String KS_NAME = "reaper";
    final String CF_NAME = "reaper";
    final long TIME_RUN = 41l;
    final long TIME_RERUN = 42l;
    final double INTENSITY = 0.5f;

    IStorage storage = new MemoryStorage();

    storage.addCluster(new Cluster(CLUSTER_NAME, null, Collections.<String>singleton(null)));

    ColumnFamily cf =
        storage.addColumnFamily(new ColumnFamily.Builder(CLUSTER_NAME, KS_NAME, CF_NAME, 1, false));

    DateTimeUtils.setCurrentMillisFixed(TIME_RUN);
    RepairRun repairRun = storage.addRepairRun(
        new RepairRun.Builder(CLUSTER_NAME, cf.getId(), RepairRun.RunState.NOT_STARTED,
            DateTime.now(), INTENSITY));

    storage.addRepairSegments(Collections.singleton(
        new RepairSegment.Builder(repairRun.getId(), new RingRange(BigInteger.ZERO, BigInteger.ONE),
            RepairSegment.State.NOT_STARTED)), 1);

    final JmxProxy jmx = mock(JmxProxy.class);

    when(jmx.getClusterName()).thenReturn(CLUSTER_NAME);
    when(jmx.isConnectionAlive()).thenReturn(true);
    when(jmx.tokenRangeToEndpoint(anyString(), any(RingRange.class)))
        .thenReturn(Lists.newArrayList(""));

    final AtomicInteger repairAttempts = new AtomicInteger(0);
    when(jmx.triggerRepair(any(BigInteger.class), any(BigInteger.class), anyString(), anyString()))
        .then(new Answer<Integer>() {
          @Override
          public Integer answer(InvocationOnMock invocation) throws Throwable {
            return repairAttempts.incrementAndGet();
          }
        });

    RepairRunner.initializeThreadPool(1, 1);
    final RepairRunner repairRunner = new RepairRunner(storage, 1, new JmxConnectionFactory() {
      @Override
      public JmxProxy create(Optional<RepairStatusHandler> handler, String host)
          throws ReaperException {
        return jmx;
      }
    });

    assertEquals(storage.getRepairSegment(1).getState(), RepairSegment.State.NOT_STARTED);
    assertEquals(0, repairAttempts.get());
    repairRunner.run();
    assertEquals(1, repairAttempts.get());
    assertEquals(storage.getRepairSegment(1).getState(), RepairSegment.State.RUNNING);
    repairRunner.handle(repairAttempts.get(), ActiveRepairService.Status.STARTED,
        "Repair " + repairAttempts + " started");
    assertEquals(DateTime.now(), storage.getRepairSegment(1).getStartTime());
    assertEquals(RepairRun.RunState.RUNNING, storage.getRepairRun(1).getRunState());

    Thread.sleep(1500);
    assertEquals(2, repairAttempts.get());
    assertEquals(storage.getRepairSegment(1).getState(), RepairSegment.State.RUNNING);

    DateTimeUtils.setCurrentMillisFixed(TIME_RERUN);
    repairRunner.handle(repairAttempts.get(), ActiveRepairService.Status.STARTED,
        "Repair " + repairAttempts + " started");
    assertEquals(DateTime.now(), storage.getRepairSegment(1).getStartTime());
    assertEquals(RepairRun.RunState.RUNNING, storage.getRepairRun(1).getRunState());

    repairRunner.handle(repairAttempts.get(), ActiveRepairService.Status.FINISHED,
        "Repair " + repairAttempts + " finished");
    Thread.sleep(100);
    assertEquals(RepairRun.RunState.DONE, storage.getRepairRun(1).getRunState());
  }


  @Test
  public void testAlreadyStartedRepair() {
    // TODO
  }
}
