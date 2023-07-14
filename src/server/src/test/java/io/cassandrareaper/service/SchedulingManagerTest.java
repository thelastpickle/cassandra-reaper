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
import io.cassandrareaper.storage.cassandra.CassandraStorageFacade;
import io.cassandrareaper.storage.cluster.IClusterDao;
import io.cassandrareaper.storage.repairrun.IRepairRunDao;
import io.cassandrareaper.storage.repairschedule.IRepairScheduleDao;
import io.cassandrareaper.storage.repairunit.IRepairUnitDao;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.cassandra.repair.RepairParallelism;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class SchedulingManagerTest {

  @Before
  public void setUp() {
    // Some other test classes change this to use fake predictable times.
    // We need to reset this to using the current time for scheduling tests.
    DateTimeUtils.setCurrentMillisSystem();
  }

  @Test
  public void testCurrentReaperIsSchedulingLeader() {
    AppContext context = new AppContext();
    context.isDistributed.set(true);
    List<UUID> reaperInstances = Lists.newArrayList();
    // Generate some fake reaper instances id after the current instance id was generated
    IntStream.range(0, 5).forEach(i -> reaperInstances.add(UUIDs.timeBased()));
    // Add the current reaper instance id to the list
    reaperInstances.add(context.reaperInstanceId);
    context.storage = mock(CassandraStorageFacade.class);
    when(((CassandraStorageFacade) context.storage).getRunningReapers()).thenReturn(reaperInstances);
    SchedulingManager schedulingManager = SchedulingManager.create(context, context.storage.getRepairRunDao());
    assertTrue("The eldest Reaper instance should be the scheduling leader",
        schedulingManager.currentReaperIsSchedulingLeader());
  }

  @Test
  public void testCurrentReaperIsNotSchedulingLeader() {
    List<UUID> reaperInstances = Lists.newArrayList();
    // Generate some fake reaper instances id before the current instance id is generated
    IntStream.range(0, 5).forEach(i -> reaperInstances.add(UUIDs.timeBased()));
    AppContext context = new AppContext();
    context.isDistributed.set(true);
    // Add the current reaper instance id to the list
    reaperInstances.add(context.reaperInstanceId);
    context.storage = mock(CassandraStorageFacade.class);
    when(((CassandraStorageFacade) context.storage).getRunningReapers()).thenReturn(reaperInstances);
    SchedulingManager schedulingManager = SchedulingManager.create(context, context.storage.getRepairRunDao());
    assertFalse("The eldest Reaper instance should be the scheduling leader",
        schedulingManager.currentReaperIsSchedulingLeader());
  }

  @Test
  public void testCurrentReaperIsSchedulingLeaderEmptyList() {
    AppContext context = new AppContext();
    context.isDistributed.set(true);
    List<UUID> reaperInstances = Lists.newArrayList();
    // Generate some fake reaper instances id after the current instance id was generated
    IntStream.range(0, 5).forEach(i -> reaperInstances.add(UUIDs.timeBased()));
    // Add the current reaper instance id to the list
    reaperInstances.add(context.reaperInstanceId);
    context.storage = mock(CassandraStorageFacade.class);
    when(((CassandraStorageFacade) context.storage).getRunningReapers()).thenReturn(Collections.emptyList());
    SchedulingManager schedulingManager = SchedulingManager.create(context, context.storage.getRepairRunDao());
    assertFalse("If we cannot get the list of running reapers, none should be scheduling leader",
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
    context.storage = mock(CassandraStorageFacade.class);
    IRepairRunDao mockedRepairRunDao = mock(IRepairRunDao.class);
    Mockito.when(mockedRepairRunDao.getRepairRun(any())).thenReturn(Optional.of(repairRun));
    Mockito.when(((CassandraStorageFacade) context.storage).getRepairRunDao()).thenReturn(mockedRepairRunDao);

    context.config = new ReaperApplicationConfiguration();
    context.config.setPercentRepairedCheckIntervalMinutes(1);
    SchedulingManager schedulingManager = SchedulingManager.create(context, context.storage.getRepairRunDao());
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
    context.storage = mock(CassandraStorageFacade.class);
    RepairManager repairManager = mock(RepairManager.class);
    context.repairManager = repairManager;
    IRepairRunDao mockedRepairRunDao = mock(IRepairRunDao.class);
    Mockito.when(mockedRepairRunDao.getRepairRun(any())).thenReturn(Optional.of(repairRun));
    Mockito.when(((CassandraStorageFacade) context.storage).getRepairRunDao()).thenReturn(mockedRepairRunDao);


    context.config = new ReaperApplicationConfiguration();
    context.config.setPercentRepairedCheckIntervalMinutes(10);
    SchedulingManager schedulingManager = SchedulingManager.create(context, context.storage.getRepairRunDao());
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
    context.storage = mock(CassandraStorageFacade.class);
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
    IRepairRunDao mockedRepairRunDao = mock(IRepairRunDao.class);
    Mockito.when(mockedRepairRunDao.getRepairRun(any())).thenReturn(Optional.of(repairRun));
    Mockito.when(((CassandraStorageFacade) context.storage).getRepairRunDao()).thenReturn(mockedRepairRunDao);

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
    IRepairUnitDao mockedRepairUnitDao = mock(IRepairUnitDao.class);
    Mockito.when(context.storage.getRepairUnitDao()).thenReturn(mockedRepairUnitDao);
    when(mockedRepairUnitDao.getRepairUnit(any(UUID.class))).thenReturn(repairUnit);

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
    IRepairScheduleDao mockedRepairScheduleDao = Mockito.mock(IRepairScheduleDao.class);
    Mockito.when(context.storage.getRepairScheduleDao()).thenReturn(mockedRepairScheduleDao);
    when(mockedRepairScheduleDao.updateRepairSchedule(any())).thenReturn(true);

    IClusterDao mockedClusterDao = Mockito.mock(IClusterDao.class);
    Mockito.when(context.storage.getClusterDao()).thenReturn(mockedClusterDao);

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
    SchedulingManager schedulingManager = SchedulingManager.create(context, () -> repairRunService,
        context.storage.getRepairRunDao());
    when(repairRunService.registerRepairRun(any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(newRepairRun);
    schedulingManager.manageSchedule(repairSchedule);

    // We're above the threshold and the repair should've started
    Mockito.verify(context.repairManager, Mockito.times(1)).startRepairRun(any());
  }

  @Test
  public void manageIncRepairBelowPercentThresholdSchedule() throws ReaperException {
    AppContext context = new AppContext();
    context.storage = mock(CassandraStorageFacade.class);
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
    IRepairRunDao mockedRepairRunDao = mock(IRepairRunDao.class);
    Mockito.when(mockedRepairRunDao.getRepairRun(any())).thenReturn(Optional.of(repairRun));
    Mockito.when(((CassandraStorageFacade) context.storage).getRepairRunDao()).thenReturn(mockedRepairRunDao);

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
    IRepairUnitDao mockedRepairUnitDao = mock(IRepairUnitDao.class);
    Mockito.when(context.storage.getRepairUnitDao()).thenReturn(mockedRepairUnitDao);
    when(mockedRepairUnitDao.getRepairUnit(any(UUID.class))).thenReturn(repairUnit);

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
    IRepairScheduleDao mockedRepairScheduleDao = Mockito.mock(IRepairScheduleDao.class);
    Mockito.when(context.storage.getRepairScheduleDao()).thenReturn(mockedRepairScheduleDao);
    when(mockedRepairScheduleDao.updateRepairSchedule(any())).thenReturn(true);

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
    SchedulingManager schedulingManager = SchedulingManager.create(context, () -> repairRunService,
        context.storage.getRepairRunDao());
    when(repairRunService.registerRepairRun(any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(newRepairRun);
    schedulingManager.manageSchedule(repairSchedule);

    // We're below the threshold and the repair shouldn't start
    Mockito.verify(context.repairManager, Mockito.times(0)).startRepairRun(any());
  }

  @Test
  public void managePausedRepairSchedule() throws ReaperException {
    AppContext context = new AppContext();
    context.storage = mock(CassandraStorageFacade.class);
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
    IRepairRunDao mockedRepairRunDao = mock(IRepairRunDao.class);
    Mockito.when(mockedRepairRunDao.getRepairRun(any())).thenReturn(Optional.of(repairRun));
    Mockito.when(((CassandraStorageFacade) context.storage).getRepairRunDao()).thenReturn(mockedRepairRunDao);


    IRepairUnitDao mockedRepairUnitDao = mock(IRepairUnitDao.class);
    Mockito.when(context.storage.getRepairUnitDao()).thenReturn(mockedRepairUnitDao);
    when(mockedRepairUnitDao.getRepairUnit(any(UUID.class))).thenReturn(repairUnit);

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

    context.config = new ReaperApplicationConfiguration();
    RepairRunService repairRunService = mock(RepairRunService.class);
    SchedulingManager schedulingManager = SchedulingManager.create(context, () -> repairRunService,
        context.storage.getRepairRunDao());
    schedulingManager.manageSchedule(repairSchedule);

    // We're below the threshold and the repair shouldn't start
    Mockito.verify(context.repairManager, Mockito.times(0)).startRepairRun(any());
  }

  @Test
  public void cleanupMetricsRegistryTest() {
    AppContext context = new AppContext();
    context.storage = mock(CassandraStorageFacade.class);
    context.config = new ReaperApplicationConfiguration();
    context.config.setPercentRepairedCheckIntervalMinutes(10);
    context.metricRegistry = mock(MetricRegistry.class);
    List<UUID> scheduleIds = Lists.newArrayList();
    IntStream.range(0, 4).forEach(i -> scheduleIds.add(UUIDs.timeBased()));
    HashMap<String, Metric> metrics = Maps.newHashMap();
    scheduleIds.stream().forEach(scheduleId ->
        metrics.put(MetricRegistry.name(RepairScheduleService.MILLIS_SINCE_LAST_REPAIR_METRIC_NAME,
            "test", "test", scheduleId.toString()), null));

    List<RepairSchedule> repairSchedules = scheduleIds.stream().map(scheduleId ->
        RepairSchedule.builder(scheduleId)
            .daysBetween(1)
            .nextActivation(DateTime.now().plusDays(1))
            .repairParallelism(RepairParallelism.PARALLEL)
            .intensity(1)
            .segmentCountPerNode(10)
            .lastRun(UUIDs.timeBased())
            .percentUnrepairedThreshold(5)
            .state(RepairSchedule.State.ACTIVE)
            .build(scheduleId)).collect(Collectors.toList());

    // Removing a schedule should trigger the removal of one metric
    repairSchedules.remove(0);
    when(context.metricRegistry.getMetrics()).thenReturn(metrics);
    SchedulingManager schedulingManager = SchedulingManager.create(context, () -> null,
        context.storage.getRepairRunDao());
    schedulingManager.cleanupMetricsRegistry(repairSchedules);
    Mockito.verify(context.metricRegistry, Mockito.times(1)).remove(any());
  }
}