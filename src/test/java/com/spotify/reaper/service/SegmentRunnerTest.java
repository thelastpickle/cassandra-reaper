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
import com.google.common.collect.Sets;

import com.spotify.reaper.ReaperException;
import com.spotify.reaper.cassandra.JmxConnectionFactory;
import com.spotify.reaper.cassandra.JmxProxy;
import com.spotify.reaper.cassandra.RepairStatusHandler;
import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.core.RepairSegment;
import com.spotify.reaper.core.RepairUnit;
import com.spotify.reaper.storage.IStorage;
import com.spotify.reaper.storage.MemoryStorage;

import org.apache.cassandra.service.ActiveRepairService;
import org.apache.commons.lang3.mutable.MutableObject;
import org.joda.time.DateTime;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.math.BigInteger;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SegmentRunnerTest {
  // TODO: Clean up tests. There's a lot of code duplication across these tests.

  @Test
  public void timeoutTest() throws InterruptedException, ReaperException, ExecutionException {
    final IStorage storage = new MemoryStorage();
    RepairUnit cf = storage.addRepairUnit(
        new RepairUnit.Builder("reaper", "reaper", Sets.newHashSet("reaper"), 1,
                               RepairUnit.RepairParallelism.PARALLEL));
    RepairRun run = storage.addRepairRun(
        new RepairRun.Builder("reaper", cf.getId(), DateTime.now(), 0.5));
    storage.addRepairSegments(Collections.singleton(
        new RepairSegment.Builder(run.getId(), new RingRange(BigInteger.ONE, BigInteger.ZERO),
                                  cf.getId())), run.getId());
    final long segmentId = storage.getNextFreeSegment(run.getId()).get().getId();

    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final MutableObject<Future<?>> future = new MutableObject<>();

    SegmentRunner.triggerRepair(storage, segmentId,
                                Collections.singleton(""), 100, new JmxConnectionFactory() {
          @Override
          public JmxProxy connect(final Optional<RepairStatusHandler> handler, String host)
              throws ReaperException {
            JmxProxy jmx = mock(JmxProxy.class);
            when(jmx.getClusterName()).thenReturn("reaper");
            when(jmx.isConnectionAlive()).thenReturn(true);
            when(jmx.tokenRangeToEndpoint(anyString(), any(RingRange.class)))
                .thenReturn(Lists.newArrayList(""));
            when(jmx.triggerRepair(any(BigInteger.class), any(BigInteger.class), anyString(),
                                   Sets.newHashSet(anyString())))
                .then(new Answer<Integer>() {
                  @Override
                  public Integer answer(InvocationOnMock invocation) {
                    assertEquals(RepairSegment.State.NOT_STARTED,
                                 storage.getRepairSegment(segmentId).get().getState());
                    future.setValue(executor.submit(new Thread() {
                      @Override
                      public void run() {
                        handler.get().handle(1, ActiveRepairService.Status.STARTED,
                                             "Repair command 1 has started");
                        assertEquals(RepairSegment.State.RUNNING,
                                     storage.getRepairSegment(segmentId).get().getState());
                      }
                    }));
                    return 1;
                  }
                });

            return jmx;
          }
        });

    future.getValue().get();
    executor.shutdown();

    assertEquals(RepairSegment.State.NOT_STARTED,
                 storage.getRepairSegment(segmentId).get().getState());
    assertEquals(1, storage.getRepairSegment(segmentId).get().getFailCount());
  }

  @Test
  public void successTest() throws InterruptedException, ReaperException, ExecutionException {
    final IStorage storage = new MemoryStorage();
    RepairUnit cf = storage.addRepairUnit(
        new RepairUnit.Builder("reaper", "reaper", Sets.newHashSet("reaper"), 1,
                               RepairUnit.RepairParallelism.PARALLEL));
    RepairRun run = storage.addRepairRun(
        new RepairRun.Builder("reaper", cf.getId(), DateTime.now(), 0.5));
    storage.addRepairSegments(Collections.singleton(
        new RepairSegment.Builder(run.getId(), new RingRange(BigInteger.ONE, BigInteger.ZERO),
                                  cf.getId())), run.getId());
    final long segmentId = storage.getNextFreeSegment(run.getId()).get().getId();

    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final MutableObject<Future<?>> future = new MutableObject<>();

    SegmentRunner.triggerRepair(storage, segmentId,
                                Collections.singleton(""), 1000, new JmxConnectionFactory() {
          @Override
          public JmxProxy connect(final Optional<RepairStatusHandler> handler, String host)
              throws ReaperException {
            JmxProxy jmx = mock(JmxProxy.class);
            when(jmx.getClusterName()).thenReturn("reaper");
            when(jmx.isConnectionAlive()).thenReturn(true);
            when(jmx.tokenRangeToEndpoint(anyString(), any(RingRange.class)))
                .thenReturn(Lists.newArrayList(""));
            when(jmx.triggerRepair(any(BigInteger.class), any(BigInteger.class), anyString(),
                                   Sets.newHashSet(anyString())))
                .then(new Answer<Integer>() {
                  @Override
                  public Integer answer(InvocationOnMock invocation) {
                    assertEquals(RepairSegment.State.NOT_STARTED,
                                 storage.getRepairSegment(segmentId).get().getState());
                    future.setValue(executor.submit(new Runnable() {
                      @Override
                      public void run() {
                        handler.get().handle(1, ActiveRepairService.Status.STARTED,
                                             "Repair command 1 has started");
                        assertEquals(RepairSegment.State.RUNNING,
                                     storage.getRepairSegment(segmentId).get().getState());
                        // report about an unrelated repair. Shouldn't affect anything.
                        handler.get().handle(2, ActiveRepairService.Status.SESSION_FAILED,
                                             "Repair command 2 has failed");
                        handler.get().handle(1, ActiveRepairService.Status.SESSION_SUCCESS,
                                             "Repair session succeeded in command 1");
                        assertEquals(RepairSegment.State.RUNNING,
                                     storage.getRepairSegment(segmentId).get().getState());
                        handler.get().handle(1, ActiveRepairService.Status.FINISHED,
                                             "Repair command 1 has finished");
                        assertEquals(RepairSegment.State.DONE,
                                     storage.getRepairSegment(segmentId).get().getState());
                      }
                    }));
                    return 1;
                  }
                });

            return jmx;
          }
        });

    future.getValue().get();
    executor.shutdown();

    assertEquals(RepairSegment.State.DONE, storage.getRepairSegment(segmentId).get().getState());
    assertEquals(0, storage.getRepairSegment(segmentId).get().getFailCount());
  }

  @Test
  public void failureTest() throws InterruptedException, ReaperException, ExecutionException {
    final IStorage storage = new MemoryStorage();
    RepairUnit cf =
        storage.addRepairUnit(
            new RepairUnit.Builder("reaper", "reaper", Sets.newHashSet("reaper"), 1,
                                   RepairUnit.RepairParallelism.PARALLEL));
    RepairRun run = storage.addRepairRun(
        new RepairRun.Builder("reaper", cf.getId(), DateTime.now(), 0.5));
    storage.addRepairSegments(Collections.singleton(
        new RepairSegment.Builder(run.getId(), new RingRange(BigInteger.ONE, BigInteger.ZERO),
                                  cf.getId())), run.getId());
    final long segmentId = storage.getNextFreeSegment(run.getId()).get().getId();

    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final MutableObject<Future<?>> future = new MutableObject<>();

    SegmentRunner.triggerRepair(storage, segmentId,
                                Collections.singleton(""), 1000, new JmxConnectionFactory() {
          @Override
          public JmxProxy connect(final Optional<RepairStatusHandler> handler, String host)
              throws ReaperException {
            JmxProxy jmx = mock(JmxProxy.class);
            when(jmx.getClusterName()).thenReturn("reaper");
            when(jmx.isConnectionAlive()).thenReturn(true);
            when(jmx.tokenRangeToEndpoint(anyString(), any(RingRange.class)))
                .thenReturn(Lists.newArrayList(""));
            when(jmx.triggerRepair(any(BigInteger.class), any(BigInteger.class), anyString(),
                                   Sets.newHashSet(anyString())))
                .then(new Answer<Integer>() {
                  @Override
                  public Integer answer(InvocationOnMock invocation) {
                    assertEquals(RepairSegment.State.NOT_STARTED,
                                 storage.getRepairSegment(segmentId).get().getState());
                    future.setValue(executor.submit(new Runnable() {
                      @Override
                      public void run() {
                        handler.get().handle(1, ActiveRepairService.Status.STARTED,
                                             "Repair command 1 has started");
                        assertEquals(RepairSegment.State.RUNNING,
                                     storage.getRepairSegment(segmentId).get().getState());
                        handler.get().handle(1, ActiveRepairService.Status.SESSION_SUCCESS,
                                             "Repair session succeeded in command 1");
                        assertEquals(RepairSegment.State.RUNNING,
                                     storage.getRepairSegment(segmentId).get().getState());
                        handler.get().handle(1, ActiveRepairService.Status.SESSION_FAILED,
                                             "Repair command 1 has failed");
                        assertEquals(RepairSegment.State.NOT_STARTED,
                                     storage.getRepairSegment(segmentId).get().getState());
                      }
                    }));

                    return 1;
                  }
                });

            return jmx;
          }
        });

    future.getValue().get();
    executor.shutdown();

    assertEquals(RepairSegment.State.NOT_STARTED,
                 storage.getRepairSegment(segmentId).get().getState());
    assertEquals(1, storage.getRepairSegment(segmentId).get().getFailCount());
  }
}
