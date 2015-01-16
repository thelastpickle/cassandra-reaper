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

import org.apache.cassandra.service.ActiveRepairService;
import org.joda.time.DateTime;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.math.BigInteger;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SegmentRunnerTest {

  @Test
  public void timeoutTest() throws InterruptedException, ReaperException {
    final IStorage storage = new MemoryStorage();
    ColumnFamily cf =
        storage.addColumnFamily(new ColumnFamily.Builder("reaper", "reaper", "reaper", 1, false));
    RepairRun run = storage.addRepairRun(
        new RepairRun.Builder("reaper", cf.getId(), DateTime.now(),
            0.5));
    storage.addRepairSegments(Collections.singleton(
        new RepairSegment.Builder(run.getId(), new RingRange(BigInteger.ONE, BigInteger.ZERO),
            cf.getId())), run.getId());
    final long segmentId = storage.getNextFreeSegment(run.getId()).getId();

    SegmentRunner.triggerRepair(storage, segmentId,
        Collections.singleton(""), 500, new JmxConnectionFactory() {
          @Override
          public JmxProxy create(final Optional<RepairStatusHandler> handler, String host)
              throws ReaperException {
            JmxProxy jmx = mock(JmxProxy.class);
            when(jmx.getClusterName()).thenReturn("reaper");
            when(jmx.isConnectionAlive()).thenReturn(true);
            when(jmx.tokenRangeToEndpoint(anyString(), any(RingRange.class)))
                .thenReturn(Lists.newArrayList(""));
            when(jmx.triggerRepair(any(BigInteger.class), any(BigInteger.class), anyString(),
                anyString()))
                .then(new Answer<Integer>() {
                  @Override
                  public Integer answer(InvocationOnMock invocation) throws Throwable {
                    new Thread() {
                      @Override
                      public void run() {
                        System.out.println("Repair has been triggered");
                        try {
                          sleep(10);
                          handler.get().handle(1, ActiveRepairService.Status.STARTED,
                              "Repair command 1 has started");
                          sleep(100);
                          assertEquals(RepairSegment.State.RUNNING,
                              storage.getRepairSegment(segmentId).getState());
                        } catch (InterruptedException e) {
                          e.printStackTrace();
                        }
                      }
                    }.start();
                    return 1;
                  }
                });

            return jmx;
          }
        });

    assertEquals(RepairSegment.State.NOT_STARTED, storage.getRepairSegment(segmentId).getState());
  }

  @Test
  public void successTest() throws InterruptedException, ReaperException {
    final IStorage storage = new MemoryStorage();
    ColumnFamily cf =
        storage.addColumnFamily(new ColumnFamily.Builder("reaper", "reaper", "reaper", 1, false));
    RepairRun run = storage.addRepairRun(
        new RepairRun.Builder("reaper", cf.getId(), DateTime.now(), 0.5));
    storage.addRepairSegments(Collections.singleton(
        new RepairSegment.Builder(run.getId(), new RingRange(BigInteger.ONE, BigInteger.ZERO),
            cf.getId())), run.getId());
    final long segmentId = storage.getNextFreeSegment(run.getId()).getId();

    SegmentRunner.triggerRepair(storage, segmentId,
        Collections.singleton(""), 500, new JmxConnectionFactory() {
          @Override
          public JmxProxy create(final Optional<RepairStatusHandler> handler, String host)
              throws ReaperException {
            JmxProxy jmx = mock(JmxProxy.class);
            when(jmx.getClusterName()).thenReturn("reaper");
            when(jmx.isConnectionAlive()).thenReturn(true);
            when(jmx.tokenRangeToEndpoint(anyString(), any(RingRange.class)))
                .thenReturn(Lists.newArrayList(""));
            when(jmx.triggerRepair(any(BigInteger.class), any(BigInteger.class), anyString(),
                anyString()))
                .then(new Answer<Integer>() {
                  @Override
                  public Integer answer(InvocationOnMock invocation) throws Throwable {
                    new Thread() {
                      @Override
                      public void run() {
                        System.out.println("Repair has been triggered");
                        try {
                          sleep(10);

                          handler.get().handle(1, ActiveRepairService.Status.STARTED,
                              "Repair command 1 has started");
                          sleep(100);
                          assertEquals(RepairSegment.State.RUNNING,
                              storage.getRepairSegment(segmentId).getState());

                          // report about an unrelated repair. Shouldn't affect anything.
                          handler.get().handle(2, ActiveRepairService.Status.SESSION_FAILED,
                              "Repair command 2 has failed");
                          handler.get().handle(1, ActiveRepairService.Status.SESSION_SUCCESS,
                              "Repair session succeeded in command 1");
                          sleep(10);

                          assertEquals(RepairSegment.State.RUNNING,
                              storage.getRepairSegment(segmentId).getState());
                          handler.get().handle(1, ActiveRepairService.Status.FINISHED,
                              "Repair command 1 has finished");
                        } catch (InterruptedException e) {
                          e.printStackTrace();
                        }
                      }
                    }.start();
                    return 1;
                  }
                });

            return jmx;
          }
        });

    assertEquals(RepairSegment.State.DONE, storage.getRepairSegment(segmentId).getState());
  }

  @Test
  public void failureTest() throws InterruptedException, ReaperException {
    final IStorage storage = new MemoryStorage();
    ColumnFamily cf =
        storage.addColumnFamily(new ColumnFamily.Builder("reaper", "reaper", "reaper", 1, false));
    RepairRun run = storage.addRepairRun(
        new RepairRun.Builder("reaper", cf.getId(), DateTime.now(), 0.5));
    storage.addRepairSegments(Collections.singleton(
        new RepairSegment.Builder(run.getId(), new RingRange(BigInteger.ONE, BigInteger.ZERO),
            cf.getId())), run.getId());
    final long segmentId = storage.getNextFreeSegment(run.getId()).getId();

    SegmentRunner.triggerRepair(storage, segmentId,
        Collections.singleton(""), 500, new JmxConnectionFactory() {
          @Override
          public JmxProxy create(final Optional<RepairStatusHandler> handler, String host)
              throws ReaperException {
            JmxProxy jmx = mock(JmxProxy.class);
            when(jmx.getClusterName()).thenReturn("reaper");
            when(jmx.isConnectionAlive()).thenReturn(true);
            when(jmx.tokenRangeToEndpoint(anyString(), any(RingRange.class)))
                .thenReturn(Lists.newArrayList(""));
            when(jmx.triggerRepair(any(BigInteger.class), any(BigInteger.class), anyString(),
                anyString()))
                .then(new Answer<Integer>() {
                  @Override
                  public Integer answer(InvocationOnMock invocation) throws Throwable {
                    new Thread() {
                      @Override
                      public void run() {
                        System.out.println("Repair has been triggered");
                        try {
                          sleep(10);
                          handler.get().handle(1, ActiveRepairService.Status.STARTED,
                              "Repair command 1 has started");
                          sleep(100);
                          assertEquals(RepairSegment.State.RUNNING,
                              storage.getRepairSegment(segmentId).getState());
                          handler.get().handle(1, ActiveRepairService.Status.SESSION_SUCCESS,
                              "Repair session succeeded in command 1");
                          sleep(10);
                          assertEquals(RepairSegment.State.RUNNING,
                              storage.getRepairSegment(segmentId).getState());
                          handler.get().handle(1, ActiveRepairService.Status.SESSION_FAILED,
                              "Repair command 1 has failed");
                        } catch (InterruptedException e) {
                          e.printStackTrace();
                        }
                      }
                    }.start();
                    return 1;
                  }
                });

            return jmx;
          }
        });

    assertEquals(RepairSegment.State.ERROR, storage.getRepairSegment(segmentId).getState());
  }
}
