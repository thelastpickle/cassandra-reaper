/*
 * Copyright 2014-2017 Spotify AB
 * Copyright 2016-2018 The Last Pickle Ltd
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

package io.cassandrareaper.service;

import io.cassandrareaper.AppContext;
import io.cassandrareaper.ReaperApplicationConfiguration;
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.PercentRepairedMetric;
import io.cassandrareaper.core.RepairRun;
import io.cassandrareaper.core.RepairRun.RunState;
import io.cassandrareaper.core.RepairSchedule;
import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.storage.CassandraStorage;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.IntStream;

import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.Lists;

import org.apache.cassandra.repair.RepairParallelism;
import org.joda.time.DateTime;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class SchedulingManagerTest {

  @Test
  public void testCurrentReaperIsSchedulingLeader() {
    AppContext context = new AppContext();
    context.isDistributed.set(true);
    List<UUID> reaperInstances = Lists.newArrayList();
    // Generate some fake reaper instances id after the current instance id was generated
    IntStream.range(0,5).forEach(i -> reaperInstances.add(UUIDs.timeBased()));
    // Add the current reaper instance id to the list
    reaperInstances.add(context.reaperInstanceId);
    context.storage = mock(CassandraStorage.class);
    when(((CassandraStorage)context.storage).getRunningReapers()).thenReturn(reaperInstances);
    SchedulingManager schedulingManager = SchedulingManager.create(context);
    assertTrue("The eldest Reaper instance should be the scheduling leader",
        schedulingManager.currentReaperIsSchedulingLeader());
  }

  @Test
  public void testCurrentReaperIsNotSchedulingLeader() {
    List<UUID> reaperInstances = Lists.newArrayList();
    // Generate some fake reaper instances id before the current instance id is generated
    IntStream.range(0,5).forEach(i -> reaperInstances.add(UUIDs.timeBased()));
    AppContext context = new AppContext();
    context.isDistributed.set(true);
    // Add the current reaper instance id to the list
    reaperInstances.add(context.reaperInstanceId);
    context.storage = mock(CassandraStorage.class);
    when(((CassandraStorage)context.storage).getRunningReapers()).thenReturn(reaperInstances);
    SchedulingManager schedulingManager = SchedulingManager.create(context);
    assertFalse("The eldest Reaper instance should be the scheduling leader",
        schedulingManager.currentReaperIsSchedulingLeader());
  }

  @Test
  public void lastRepairRunIsOldEnoughTest() {
    RepairRun repairRun = RepairRun.builder("test", UUIDs.timeBased())
        .repairParallelism(RepairParallelism.PARALLEL)
        .intensity(1.0)
        .segmentCount(10)
        .tables(Collections.emptySet())
        .runState(RunState.DONE)
        .startTime(DateTime.now().minusMinutes(10))
        .endTime(DateTime.now().minusMinutes(5))
        .build(UUIDs.timeBased());

    AppContext context = new AppContext();
    context.storage = mock(CassandraStorage.class);
    when(((CassandraStorage)context.storage).getRepairRun(any())).thenReturn(Optional.of(repairRun));

    context.config = new ReaperApplicationConfiguration();
    context.config.setPercentRepairedCheckIntervalMinutes(1);
    SchedulingManager schedulingManager = SchedulingManager.create(context);
    RepairSchedule repairSchedule = RepairSchedule.builder(UUIDs.timeBased())
        .daysBetween(1)
        .nextActivation(DateTime.now())
        .repairParallelism(RepairParallelism.PARALLEL)
        .intensity(1)
        .segmentCountPerNode(10)
        .lastRun(repairRun.getId())
        .percentUnrepairedThreshold(5)
        .build(UUIDs.timeBased());
    assertTrue("Enough time must pass since last run to trigger a new one",
        schedulingManager.lastRepairRunIsOldEnough(repairSchedule));
  }

  @Test
  public void lastRepairRunIsNotOldEnoughTest() {
    RepairRun repairRun = RepairRun.builder("test", UUIDs.timeBased())
        .repairParallelism(RepairParallelism.PARALLEL)
        .intensity(1.0)
        .segmentCount(10)
        .tables(Collections.emptySet())
        .runState(RunState.DONE)
        .startTime(DateTime.now().minusMinutes(10))
        .endTime(DateTime.now().minusMinutes(1))
        .build(UUIDs.timeBased());

    AppContext context = new AppContext();
    context.storage = mock(CassandraStorage.class);
    RepairManager repairManager = mock(RepairManager.class);
    context.repairManager = repairManager;
    when(((CassandraStorage)context.storage).getRepairRun(any())).thenReturn(Optional.of(repairRun));

    context.config = new ReaperApplicationConfiguration();
    context.config.setPercentRepairedCheckIntervalMinutes(10);
    SchedulingManager schedulingManager = SchedulingManager.create(context);
    RepairSchedule repairSchedule = RepairSchedule.builder(UUIDs.timeBased())
        .daysBetween(1)
        .nextActivation(DateTime.now())
        .repairParallelism(RepairParallelism.PARALLEL)
        .intensity(1)
        .segmentCountPerNode(10)
        .lastRun(repairRun.getId())
        .percentUnrepairedThreshold(5)
        .build(UUIDs.timeBased());
    assertFalse("Enough time must pass since last run to trigger a new one",
        schedulingManager.lastRepairRunIsOldEnough(repairSchedule));
  }

  @Test
  public void manageIncRepairAbovePercentThresholdSchedule() throws ReaperException {
    AppContext context = new AppContext();
    context.storage = mock(CassandraStorage.class);
    RepairManager repairManager = mock(RepairManager.class);
    context.repairManager = repairManager;

    RepairUnit repairUnit = RepairUnit.builder()
        .clusterName("test")
        .incrementalRepair(true)
        .keyspaceName("test")
        .repairThreadCount(1)
        .timeout(30)
        .build(UUIDs.timeBased());

    DateTime startTime = DateTime.now().minusMinutes(10);
    DateTime endTime = DateTime.now().minusMinutes(5);
    RepairRun repairRun = RepairRun.builder("test", repairUnit.getId())
        .repairParallelism(RepairParallelism.PARALLEL)
        .intensity(1.0)
        .segmentCount(10)
        .tables(Collections.emptySet())
        .runState(RunState.DONE)
        .startTime(startTime)
        .endTime(endTime)
        .build(UUIDs.timeBased());
    when(context.storage.getRepairRun(any())).thenReturn(Optional.of(repairRun));

    RepairSchedule repairSchedule = RepairSchedule.builder(repairUnit.getId())
        .daysBetween(1)
        .nextActivation(DateTime.now())
        .repairParallelism(RepairParallelism.PARALLEL)
        .intensity(1)
        .segmentCountPerNode(10)
        .lastRun(repairRun.getId())
        .percentUnrepairedThreshold(5)
        .state(RepairSchedule.State.ACTIVE)
        .build(UUIDs.timeBased());
    when(context.storage.getRepairUnit(any(UUID.class))).thenReturn(repairUnit);

    PercentRepairedMetric percentRepairedMetric = PercentRepairedMetric.builder()
        .withCluster("test")
        .withKeyspaceName("test")
        .withNode("127.0.0.1")
        .withRepairScheduleId(repairSchedule.getId())
        .withTableName("test")
        .withPercentRepaired(90)
        .build();

    when(context.storage.getPercentRepairedMetrics(any(), any(), any()))
      .thenReturn(Lists.newArrayList(percentRepairedMetric));

    when(context.storage.updateRepairSchedule(any())).thenReturn(true);

    context.config = new ReaperApplicationConfiguration();
    context.config.setPercentRepairedCheckIntervalMinutes(10);
    RepairRunService repairRunService = mock(RepairRunService.class);
    RepairRun newRepairRun = RepairRun.builder("test", repairUnit.getId())
        .repairParallelism(RepairParallelism.PARALLEL)
        .intensity(1.0)
        .segmentCount(10)
        .tables(Collections.emptySet())
        .runState(RunState.NOT_STARTED)
        .build(UUIDs.timeBased());
    SchedulingManager schedulingManager = SchedulingManager.create(context, () -> repairRunService);
    when(repairRunService.registerRepairRun(any(), any(), any(), any(), any(), any(), any(), any()))
      .thenReturn(newRepairRun);
    schedulingManager.manageSchedule(repairSchedule);

    // We're above the threshold and the repair should've started
    Mockito.verify(context.repairManager, Mockito.times(1)).startRepairRun(any());
  }

  @Test
  public void manageIncRepairBelowPercentThresholdSchedule() throws ReaperException {
    AppContext context = new AppContext();
    context.storage = mock(CassandraStorage.class);
    RepairManager repairManager = mock(RepairManager.class);
    context.repairManager = repairManager;

    RepairUnit repairUnit = RepairUnit.builder()
        .clusterName("test")
        .incrementalRepair(true)
        .keyspaceName("test")
        .repairThreadCount(1)
        .timeout(30)
        .build(UUIDs.timeBased());

    RepairRun repairRun = RepairRun.builder("test", repairUnit.getId())
        .repairParallelism(RepairParallelism.PARALLEL)
        .intensity(1.0)
        .segmentCount(10)
        .tables(Collections.emptySet())
        .runState(RunState.DONE)
        .startTime(DateTime.now().minusMinutes(10))
        .endTime(DateTime.now().minusMinutes(5))
        .build(UUIDs.timeBased());
    when(((CassandraStorage)context.storage).getRepairRun(any())).thenReturn(Optional.of(repairRun));

    RepairSchedule repairSchedule = RepairSchedule.builder(repairUnit.getId())
        .daysBetween(1)
        .nextActivation(DateTime.now().plusDays(1))
        .repairParallelism(RepairParallelism.PARALLEL)
        .intensity(1)
        .segmentCountPerNode(10)
        .lastRun(repairRun.getId())
        .percentUnrepairedThreshold(5)
        .state(RepairSchedule.State.ACTIVE)
        .build(UUIDs.timeBased());
    when(context.storage.getRepairUnit(any(UUID.class))).thenReturn(repairUnit);

    PercentRepairedMetric percentRepairedMetric = PercentRepairedMetric.builder()
        .withCluster("test")
        .withKeyspaceName("test")
        .withNode("127.0.0.1")
        .withRepairScheduleId(repairSchedule.getId())
        .withTableName("test")
        .withPercentRepaired(99)
        .build();

    when(context.storage.getPercentRepairedMetrics(any(), any(), any()))
        .thenReturn(Lists.newArrayList(percentRepairedMetric));

    when(context.storage.updateRepairSchedule(any())).thenReturn(true);

    context.config = new ReaperApplicationConfiguration();
    context.config.setPercentRepairedCheckIntervalMinutes(10);
    RepairRunService repairRunService = mock(RepairRunService.class);
    RepairRun newRepairRun = RepairRun.builder("test", repairUnit.getId())
        .repairParallelism(RepairParallelism.PARALLEL)
        .intensity(1.0)
        .segmentCount(10)
        .tables(Collections.emptySet())
        .runState(RunState.NOT_STARTED)
        .build(UUIDs.timeBased());
    SchedulingManager schedulingManager = SchedulingManager.create(context, () -> repairRunService);
    when(repairRunService.registerRepairRun(any(), any(), any(), any(), any(), any(), any(), any()))
      .thenReturn(newRepairRun);
    schedulingManager.manageSchedule(repairSchedule);

    // We're below the threshold and the repair shouldn't start
    Mockito.verify(context.repairManager, Mockito.times(0)).startRepairRun(any());
  }

  @Test
  public void managePausedRepairSchedule() throws ReaperException {
    AppContext context = new AppContext();
    context.storage = mock(CassandraStorage.class);
    RepairManager repairManager = mock(RepairManager.class);
    context.repairManager = repairManager;

    RepairUnit repairUnit = RepairUnit.builder()
        .clusterName("test")
        .incrementalRepair(true)
        .keyspaceName("test")
        .repairThreadCount(1)
        .timeout(30)
        .build(UUIDs.timeBased());

    RepairRun repairRun = RepairRun.builder("test", repairUnit.getId())
        .repairParallelism(RepairParallelism.PARALLEL)
        .intensity(1.0)
        .segmentCount(10)
        .tables(Collections.emptySet())
        .runState(RunState.DONE)
        .startTime(DateTime.now().minusMinutes(10))
        .endTime(DateTime.now().minusMinutes(5))
        .build(UUIDs.timeBased());
    when(((CassandraStorage)context.storage).getRepairRun(any())).thenReturn(Optional.of(repairRun));

    RepairSchedule repairSchedule = RepairSchedule.builder(repairUnit.getId())
        .daysBetween(1)
        .nextActivation(DateTime.now().plusDays(1))
        .repairParallelism(RepairParallelism.PARALLEL)
        .intensity(1)
        .segmentCountPerNode(10)
        .lastRun(repairRun.getId())
        .percentUnrepairedThreshold(5)
        .state(RepairSchedule.State.PAUSED)
        .build(UUIDs.timeBased());
    when(context.storage.getRepairUnit(any(UUID.class))).thenReturn(repairUnit);

    context.config = new ReaperApplicationConfiguration();
    RepairRunService repairRunService = mock(RepairRunService.class);
    SchedulingManager schedulingManager = SchedulingManager.create(context, () -> repairRunService);
    schedulingManager.manageSchedule(repairSchedule);

    // We're below the threshold and the repair shouldn't start
    Mockito.verify(context.repairManager, Mockito.times(0)).startRepairRun(any());
  }
}
