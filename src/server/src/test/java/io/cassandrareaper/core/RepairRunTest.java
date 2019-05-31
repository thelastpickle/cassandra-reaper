/*
 *
 * Copyright 2019-2019 The Last Pickle Ltd
 *
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

package io.cassandrareaper.core;

import io.cassandrareaper.core.RepairRun.RunState;

import java.util.Set;
import java.util.UUID;

import com.google.common.collect.ImmutableSet;
import org.apache.cassandra.repair.RepairParallelism;
import org.joda.time.DateTime;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public final class RepairRunTest {

  private static final Set<String> TABLES = ImmutableSet.of("table1");

  @Test(expected = IllegalStateException.class)
  public void testNotStartedWithStartTime() {
    RepairRun.builder("test", UUID.randomUUID())
                                   .repairParallelism(RepairParallelism.DATACENTER_AWARE)
                                   .tables(TABLES)
                                   .intensity(1.0)
                                   .segmentCount(10)
                                   .runState(RunState.NOT_STARTED)
                                   .startTime(DateTime.now())
                                   .build(UUID.randomUUID());
  }

  @Test
  public void testNotStartedWithNoStartTime() {
    RepairRun repairRun = RepairRun.builder("test", UUID.randomUUID())
                                   .repairParallelism(RepairParallelism.DATACENTER_AWARE)
                                   .tables(TABLES)
                                   .intensity(1.0)
                                   .segmentCount(10)
                                   .runState(RunState.NOT_STARTED)
                                   .build(UUID.randomUUID());

    assertEquals(RunState.NOT_STARTED, repairRun.getRunState());

  }

  @Test(expected = IllegalStateException.class)
  public void testRunningWithNoStartTime() {
    RepairRun.builder("test", UUID.randomUUID())
                                   .repairParallelism(RepairParallelism.DATACENTER_AWARE)
                                   .tables(TABLES)
                                   .intensity(1.0)
                                   .segmentCount(10)
                                   .runState(RunState.RUNNING)
                                   .build(UUID.randomUUID());
  }

  @Test
  public void testRunningWithStartTime() {
    RepairRun.builder("test", UUID.randomUUID())
                                   .repairParallelism(RepairParallelism.DATACENTER_AWARE)
                                   .tables(TABLES)
                                   .intensity(1.0)
                                   .segmentCount(10)
                                   .runState(RunState.RUNNING)
                                   .startTime(DateTime.now())
                                   .build(UUID.randomUUID());
  }

  @Test(expected = IllegalStateException.class)
  public void testRunningWithEndTime() {
    RepairRun.builder("test", UUID.randomUUID())
                                   .repairParallelism(RepairParallelism.DATACENTER_AWARE)
                                   .tables(TABLES)
                                   .intensity(1.0)
                                   .segmentCount(10)
                                   .runState(RunState.RUNNING)
                                   .startTime(DateTime.now())
                                   .endTime(DateTime.now())
                                   .build(UUID.randomUUID());
  }

  @Test(expected = IllegalStateException.class)
  public void testRunningWithPauseTime() {
    RepairRun.builder("test", UUID.randomUUID())
                                   .repairParallelism(RepairParallelism.DATACENTER_AWARE)
                                   .tables(TABLES)
                                   .intensity(1.0)
                                   .segmentCount(10)
                                   .runState(RunState.RUNNING)
                                   .startTime(DateTime.now())
                                   .pauseTime(DateTime.now())
                                   .build(UUID.randomUUID());
  }

  @Test
  public void testPausedWithPauseTime() {
    RepairRun.builder("test", UUID.randomUUID())
                                   .repairParallelism(RepairParallelism.DATACENTER_AWARE)
                                   .tables(TABLES)
                                   .intensity(1.0)
                                   .segmentCount(10)
                                   .runState(RunState.PAUSED)
                                   .startTime(DateTime.now())
                                   .pauseTime(DateTime.now())
                                   .build(UUID.randomUUID());
  }

  @Test(expected = IllegalStateException.class)
  public void testPausedWithNoPauseTime() {
    RepairRun.builder("test", UUID.randomUUID())
                                   .repairParallelism(RepairParallelism.DATACENTER_AWARE)
                                   .tables(TABLES)
                                   .intensity(1.0)
                                   .segmentCount(10)
                                   .runState(RunState.PAUSED)
                                   .startTime(DateTime.now())
                                   .build(UUID.randomUUID());
  }

  @Test(expected = IllegalStateException.class)
  public void testPausedWithEndTime() {
    RepairRun.builder("test", UUID.randomUUID())
                                   .repairParallelism(RepairParallelism.DATACENTER_AWARE)
                                   .tables(TABLES)
                                   .intensity(1.0)
                                   .segmentCount(10)
                                   .runState(RunState.PAUSED)
                                   .startTime(DateTime.now())
                                   .pauseTime(DateTime.now())
                                   .endTime(DateTime.now())
                                   .build(UUID.randomUUID());
  }

  @Test(expected = IllegalStateException.class)
  public void testAbortedWithPauseTime() {
    RepairRun.builder("test", UUID.randomUUID())
                                   .repairParallelism(RepairParallelism.DATACENTER_AWARE)
                                   .tables(TABLES)
                                   .intensity(1.0)
                                   .segmentCount(10)
                                   .runState(RunState.ABORTED)
                                   .startTime(DateTime.now())
                                   .pauseTime(DateTime.now())
                                   .endTime(DateTime.now())
                                   .build(UUID.randomUUID());
  }

  @Test
  public void testAborted() {
    RepairRun.builder("test", UUID.randomUUID())
                                   .repairParallelism(RepairParallelism.DATACENTER_AWARE)
                                   .tables(TABLES)
                                   .intensity(1.0)
                                   .segmentCount(10)
                                   .runState(RunState.ABORTED)
                                   .startTime(DateTime.now())
                                   .endTime(DateTime.now())
                                   .build(UUID.randomUUID());
  }

  @Test(expected = IllegalStateException.class)
  public void testAbortedWithNoEndTime() {
    RepairRun.builder("test", UUID.randomUUID())
                                   .repairParallelism(RepairParallelism.DATACENTER_AWARE)
                                   .tables(TABLES)
                                   .intensity(1.0)
                                   .segmentCount(10)
                                   .runState(RunState.ABORTED)
                                   .startTime(DateTime.now())
                                   .pauseTime(DateTime.now())
                                   .build(UUID.randomUUID());
  }

  @Test(expected = IllegalStateException.class)
  public void testDoneWithPauseTime() {
    RepairRun.builder("test", UUID.randomUUID())
                                   .repairParallelism(RepairParallelism.DATACENTER_AWARE)
                                   .tables(TABLES)
                                   .intensity(1.0)
                                   .segmentCount(10)
                                   .runState(RunState.DONE)
                                   .startTime(DateTime.now())
                                   .pauseTime(DateTime.now())
                                   .endTime(DateTime.now())
                                   .build(UUID.randomUUID());
  }

  @Test
  public void testDone() {
    RepairRun.builder("test", UUID.randomUUID())
                                   .repairParallelism(RepairParallelism.DATACENTER_AWARE)
                                   .tables(TABLES)
                                   .intensity(1.0)
                                   .segmentCount(10)
                                   .runState(RunState.DONE)
                                   .startTime(DateTime.now())
                                   .endTime(DateTime.now())
                                   .build(UUID.randomUUID());
  }

  @Test(expected = IllegalStateException.class)
  public void testDoneWithNoEndTime() {
    RepairRun.builder("test", UUID.randomUUID())
                                   .repairParallelism(RepairParallelism.DATACENTER_AWARE)
                                   .tables(TABLES)
                                   .intensity(1.0)
                                   .segmentCount(10)
                                   .runState(RunState.DONE)
                                   .startTime(DateTime.now())
                                   .pauseTime(DateTime.now())
                                   .build(UUID.randomUUID());
  }
}
