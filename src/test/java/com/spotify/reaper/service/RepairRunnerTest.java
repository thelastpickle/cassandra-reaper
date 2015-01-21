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
import com.spotify.reaper.cassandra.JmxConnectionFactory;
import com.spotify.reaper.cassandra.JmxProxy;
import com.spotify.reaper.cassandra.RepairStatusHandler;
import com.spotify.reaper.core.Cluster;
import com.spotify.reaper.core.RepairUnit;
import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.core.RepairSegment;
import com.spotify.reaper.storage.IStorage;
import com.spotify.reaper.storage.MemoryStorage;

import org.apache.cassandra.service.ActiveRepairService;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.math.BigInteger;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
    final String TEST_CLUSTER = "TestCluster";

    IStorage storage = new MemoryStorage();

    // place a dummy cluster into storage
    storage.addCluster(new Cluster(TEST_CLUSTER, null, Collections.<String>singleton(null)));

    // place a dummy repair run into the storage
    DateTimeUtils.setCurrentMillisFixed(TIME_CREATION);
    RepairRun.Builder runBuilder =
        new RepairRun.Builder(TEST_CLUSTER, CF_ID, DateTime.now(),
            INTENSITY);
    storage.addRepairRun(runBuilder);
    storage.addRepairSegments(Collections.<RepairSegment.Builder>emptySet(), RUN_ID);

    // start the repair
    DateTimeUtils.setCurrentMillisFixed(TIME_START);
    RepairRunner.initializeThreadPool(1, 3, TimeUnit.HOURS, 30, TimeUnit.SECONDS);
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

    final IStorage storage = new MemoryStorage();

    storage.addCluster(new Cluster(CLUSTER_NAME, null, Collections.<String>singleton(null)));
    RepairUnit cf =
        storage.addColumnFamily(new RepairUnit.Builder(CLUSTER_NAME, KS_NAME, CF_NAME, 1, false));
    DateTimeUtils.setCurrentMillisFixed(TIME_RUN);
    RepairRun run = storage.addRepairRun(
        new RepairRun.Builder(CLUSTER_NAME, cf.getId(), DateTime.now(), INTENSITY));
    storage.addRepairSegments(Collections.singleton(
        new RepairSegment.Builder(run.getId(), new RingRange(BigInteger.ZERO, BigInteger.ONE),
            cf.getId())), run.getId());
    final long RUN_ID = run.getId();
    final long SEGMENT_ID = storage.getNextFreeSegment(run.getId()).getId();

    RepairRunner.initializeThreadPool(1, 500, TimeUnit.MILLISECONDS, 1, TimeUnit.MILLISECONDS);

    assertEquals(storage.getRepairSegment(SEGMENT_ID).getState(), RepairSegment.State.NOT_STARTED);
    RepairRunner.startNewRepairRun(storage, RUN_ID, new JmxConnectionFactory() {
      final AtomicInteger repairAttempts = new AtomicInteger(0);

      @Override
      public JmxProxy create(final Optional<RepairStatusHandler> handler, String host)
          throws ReaperException {
        final JmxProxy jmx = mock(JmxProxy.class);
        when(jmx.getClusterName()).thenReturn(CLUSTER_NAME);
        when(jmx.isConnectionAlive()).thenReturn(true);
        when(jmx.tokenRangeToEndpoint(anyString(), any(RingRange.class)))
            .thenReturn(Lists.newArrayList(""));
        when(jmx.triggerRepair(any(BigInteger.class), any(BigInteger.class), anyString(),
            anyString())).then(
            new Answer<Integer>() {
              @Override
              public Integer answer(InvocationOnMock invocation) throws Throwable {
                assertEquals(RepairSegment.State.NOT_STARTED,
                    storage.getRepairSegment(SEGMENT_ID).getState());

                final int repairNumber = repairAttempts.getAndIncrement();
                switch (repairNumber) {
                  case 0:
                    new Thread() {
                      @Override
                      public void run() {
                        handler.get()
                            .handle(repairNumber, ActiveRepairService.Status.STARTED, null);
                        assertEquals(RepairSegment.State.RUNNING,
                            storage.getRepairSegment(SEGMENT_ID).getState());
                      }
                    }.start();
                    break;
                  case 1:
                    new Thread() {
                      @Override
                      public void run() {
                        handler.get()
                            .handle(repairNumber, ActiveRepairService.Status.STARTED, null);
                        assertEquals(RepairSegment.State.RUNNING,
                            storage.getRepairSegment(SEGMENT_ID).getState());
                        handler.get()
                            .handle(repairNumber, ActiveRepairService.Status.SESSION_SUCCESS, null);
                        assertEquals(RepairSegment.State.RUNNING,
                            storage.getRepairSegment(SEGMENT_ID).getState());
                        handler.get()
                            .handle(repairNumber, ActiveRepairService.Status.FINISHED, null);
                      }
                    }.start();
                    break;
                  default:
                    fail("triggerRepair should only have been called twice");
                }
                return repairNumber;
              }
            });
        return jmx;
      }
    });

    // TODO: refactor so that we can properly wait for the repair runner to finish rather than
    // TODO: using this sleep().
    Thread.sleep(600);
    assertEquals(RepairRun.RunState.DONE, storage.getRepairRun(RUN_ID).getRunState());
  }

  @Test
  public void testResumeRepair() throws InterruptedException {
    final String CLUSTER_NAME = "reaper";
    final String KS_NAME = "reaper";
    final String CF_NAME = "reaper";
    final long TIME_RUN = 41l;
    final long TIME_RERUN = 42l;
    final double INTENSITY = 0.5f;

    final IStorage storage = new MemoryStorage();

    storage.addCluster(new Cluster(CLUSTER_NAME, null, Collections.<String>singleton(null)));
    long cf =
        storage.addColumnFamily(new RepairUnit.Builder(CLUSTER_NAME, KS_NAME, CF_NAME, 1, false))
            .getId();
    DateTimeUtils.setCurrentMillisFixed(TIME_RUN);
    RepairRun run = storage.addRepairRun(
        new RepairRun.Builder(CLUSTER_NAME, cf, DateTime.now(), INTENSITY));
    storage.addRepairSegments(Lists.newArrayList(
        new RepairSegment.Builder(run.getId(), new RingRange(BigInteger.ZERO, BigInteger.ONE), cf).state(
            RepairSegment.State.RUNNING).startTime(DateTime.now()).coordinatorHost("reaper").repairCommandId(1337),
        new RepairSegment.Builder(run.getId(), new RingRange(BigInteger.ONE, BigInteger.ZERO), cf)
    ), run.getId());
    final long RUN_ID = run.getId();
    final long SEGMENT_ID = storage.getNextFreeSegment(run.getId()).getId();

    RepairRunner.initializeThreadPool(1, 500, TimeUnit.MILLISECONDS, 1, TimeUnit.MILLISECONDS);

    assertEquals(storage.getRepairSegment(SEGMENT_ID).getState(), RepairSegment.State.NOT_STARTED);
    JmxConnectionFactory factory = new JmxConnectionFactory() {
      @Override
      public JmxProxy create(final Optional<RepairStatusHandler> handler, String host)
          throws ReaperException {
        final JmxProxy jmx = mock(JmxProxy.class);
        when(jmx.getClusterName()).thenReturn(CLUSTER_NAME);
        when(jmx.isConnectionAlive()).thenReturn(true);
        when(jmx.tokenRangeToEndpoint(anyString(), any(RingRange.class)))
            .thenReturn(Lists.newArrayList(""));
        when(jmx.triggerRepair(any(BigInteger.class), any(BigInteger.class), anyString(),
            anyString())).then(
            new Answer<Integer>() {
              @Override
              public Integer answer(InvocationOnMock invocation) throws Throwable {
                assertEquals(RepairSegment.State.NOT_STARTED,
                    storage.getRepairSegment(SEGMENT_ID).getState());
                new Thread() {
                  @Override
                  public void run() {
                    handler.get().handle(1, ActiveRepairService.Status.STARTED, null);
                    handler.get().handle(1, ActiveRepairService.Status.SESSION_SUCCESS, null);
                    handler.get().handle(1, ActiveRepairService.Status.FINISHED, null);
                  }
                }.start();
                return 1;
              }
            });
        return jmx;
      }
    };

    assertEquals(RepairRun.RunState.NOT_STARTED, storage.getRepairRun(RUN_ID).getRunState());
    RepairRunner.resumeRunningRepairRuns(storage, factory);
    assertEquals(RepairRun.RunState.NOT_STARTED, storage.getRepairRun(RUN_ID).getRunState());
    storage.updateRepairRun(run.with().runState(RepairRun.RunState.RUNNING).build(RUN_ID));
    RepairRunner.resumeRunningRepairRuns(storage, factory);
    Thread.sleep(100);
    assertEquals(RepairRun.RunState.DONE, storage.getRepairRun(RUN_ID).getRunState());
  }
}
