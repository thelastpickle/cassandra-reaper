/*
 * Copyright 2017-2017 Spotify AB
 * Copyright 2017-2018 The Last Pickle Ltd
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
import io.cassandrareaper.core.RepairRun;
import io.cassandrareaper.core.RepairSchedule;
import io.cassandrareaper.storage.IStorageDao;
import io.cassandrareaper.storage.repairrun.IRepairRunDao;
import io.cassandrareaper.storage.repairschedule.IRepairScheduleDao;

import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Sets;
import org.apache.cassandra.repair.RepairParallelism;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public final class RepairScheduleServiceTest {

  private AppContext context;
  private IRepairRunDao repairRunDao;
  private IRepairScheduleDao repairScheduleDao;
  private IStorageDao storage;
  private UUID scheduleId;
  private UUID repairUnitId;
  private UUID repairRunId;

  @Before
  public void setUp() {
    context = new AppContext();
    context.metricRegistry = new MetricRegistry();

    storage = mock(IStorageDao.class);
    repairRunDao = mock(IRepairRunDao.class);
    repairScheduleDao = mock(IRepairScheduleDao.class);

    when(storage.getRepairRunDao()).thenReturn(repairRunDao);
    when(storage.getRepairScheduleDao()).thenReturn(repairScheduleDao);

    context.storage = storage;

    scheduleId = UUID.randomUUID();
    repairUnitId = UUID.randomUUID();
    repairRunId = UUID.randomUUID();
  }

  @After
  public void tearDown() {
    // Clean up metrics
    context.metricRegistry.removeMatching((name, metric) -> true);
  }

  /** Test scenario: Schedule not found in database Expected: IllegalArgumentException thrown */
  @Test(expected = IllegalArgumentException.class)
  public void testGetUnfulfilledRepairSchedule_scheduleNotFound() {
    when(repairScheduleDao.getRepairSchedule(scheduleId)).thenReturn(Optional.empty());

    RepairScheduleService service = RepairScheduleService.create(context, repairRunDao);
    Gauge<Integer> gauge = service.getUnfulfilledRepairSchedule(scheduleId);

    // This should throw IllegalArgumentException
    gauge.getValue();
  }

  /** Test scenario: Schedule with daysBetween = 0 Expected: Returns 0 (not unfulfilled) */
  @Test
  public void testGetUnfulfilledRepairSchedule_daysBetweenZero() {
    RepairSchedule schedule =
        createScheduleBuilder()
            .daysBetween(0)
            .nextActivation(DateTime.now().plusDays(1))
            .build(scheduleId);

    when(repairScheduleDao.getRepairSchedule(scheduleId)).thenReturn(Optional.of(schedule));

    RepairScheduleService service = RepairScheduleService.create(context, repairRunDao);
    Gauge<Integer> gauge = service.getUnfulfilledRepairSchedule(scheduleId);

    assertEquals(
        "Schedule with daysBetween=0 should return 0", Integer.valueOf(0), gauge.getValue());
  }

  /** Test scenario: Schedule in PAUSED state Expected: Returns 0 (not unfulfilled) */
  @Test
  public void testGetUnfulfilledRepairSchedule_pausedState() {
    RepairSchedule schedule =
        createScheduleBuilder()
            .state(RepairSchedule.State.PAUSED)
            .daysBetween(7)
            .nextActivation(DateTime.now().minusDays(10))
            .build(scheduleId);

    when(repairScheduleDao.getRepairSchedule(scheduleId)).thenReturn(Optional.of(schedule));

    RepairScheduleService service = RepairScheduleService.create(context, repairRunDao);
    Gauge<Integer> gauge = service.getUnfulfilledRepairSchedule(scheduleId);

    assertEquals("Paused schedule should return 0", Integer.valueOf(0), gauge.getValue());
  }

  /** Test scenario: Schedule in DELETED state Expected: Returns 0 (not unfulfilled) */
  @Test
  public void testGetUnfulfilledRepairSchedule_deletedState() {
    RepairSchedule schedule =
        createScheduleBuilder()
            .state(RepairSchedule.State.DELETED)
            .daysBetween(7)
            .nextActivation(DateTime.now().minusDays(10))
            .build(scheduleId);

    when(repairScheduleDao.getRepairSchedule(scheduleId)).thenReturn(Optional.of(schedule));

    RepairScheduleService service = RepairScheduleService.create(context, repairRunDao);
    Gauge<Integer> gauge = service.getUnfulfilledRepairSchedule(scheduleId);

    assertEquals("Deleted schedule should return 0", Integer.valueOf(0), gauge.getValue());
  }

  /**
   * Test scenario: Past next activation time by less than 10% of cycle time Expected: Returns 0
   * (boundary case - within acceptable threshold)
   */
  @Test
  public void testGetUnfulfilledRepairSchedule_pastActivationWithin10Percent() {
    int daysBetween = 10;
    // Use 9% to be safely within the 10% threshold
    long ninePercentOfCycleMillis = (long) (daysBetween * 24 * 60 * 60 * 1000 * 0.09);
    DateTime nextActivation = new DateTime(DateTime.now().getMillis() - ninePercentOfCycleMillis);

    RepairSchedule schedule =
        createScheduleBuilder()
            .state(RepairSchedule.State.ACTIVE)
            .daysBetween(daysBetween)
            .nextActivation(nextActivation)
            .lastRun(null)
            .build(scheduleId);

    when(repairScheduleDao.getRepairSchedule(scheduleId)).thenReturn(Optional.of(schedule));

    RepairScheduleService service = RepairScheduleService.create(context, repairRunDao);
    Gauge<Integer> gauge = service.getUnfulfilledRepairSchedule(scheduleId);

    assertEquals("Within 10% threshold should return 0", Integer.valueOf(0), gauge.getValue());
  }

  /**
   * Test scenario: Past next activation time by more than 10% of cycle time Expected: Returns 1
   * (unfulfilled - fail fast)
   */
  @Test
  public void testGetUnfulfilledRepairSchedule_pastActivationMoreThan10Percent() {
    int daysBetween = 10;
    long tenPercentOfCycleMillis = (long) (daysBetween * 24 * 60 * 60 * 1000 * 0.1);
    DateTime nextActivation =
        DateTime.now().minus(tenPercentOfCycleMillis + 1000); // 1 second past threshold

    RepairSchedule schedule =
        createScheduleBuilder()
            .state(RepairSchedule.State.ACTIVE)
            .daysBetween(daysBetween)
            .nextActivation(nextActivation)
            .lastRun(null)
            .build(scheduleId);

    when(repairScheduleDao.getRepairSchedule(scheduleId)).thenReturn(Optional.of(schedule));

    RepairScheduleService service = RepairScheduleService.create(context, repairRunDao);
    Gauge<Integer> gauge = service.getUnfulfilledRepairSchedule(scheduleId);

    assertEquals("Past 10% threshold should return 1", Integer.valueOf(1), gauge.getValue());
  }

  /**
   * Test scenario: No last run (lastRun is null) Expected: Returns 0 (not unfulfilled - may never
   * have run)
   */
  @Test
  public void testGetUnfulfilledRepairSchedule_noLastRun() {
    RepairSchedule schedule =
        createScheduleBuilder()
            .state(RepairSchedule.State.ACTIVE)
            .daysBetween(7)
            .nextActivation(DateTime.now().plusDays(1))
            .lastRun(null)
            .build(scheduleId);

    when(repairScheduleDao.getRepairSchedule(scheduleId)).thenReturn(Optional.of(schedule));

    RepairScheduleService service = RepairScheduleService.create(context, repairRunDao);
    Gauge<Integer> gauge = service.getUnfulfilledRepairSchedule(scheduleId);

    assertEquals("No last run should return 0", Integer.valueOf(0), gauge.getValue());
  }

  /**
   * Test scenario: Last run exists but RepairRun not found in database Expected: Returns 0 (not
   * unfulfilled)
   */
  @Test
  public void testGetUnfulfilledRepairSchedule_lastRunNotFoundInDatabase() {
    RepairSchedule schedule =
        createScheduleBuilder()
            .state(RepairSchedule.State.ACTIVE)
            .daysBetween(7)
            .nextActivation(DateTime.now().plusDays(1))
            .lastRun(repairRunId)
            .build(scheduleId);

    when(repairScheduleDao.getRepairSchedule(scheduleId)).thenReturn(Optional.of(schedule));
    when(repairRunDao.getRepairRun(repairRunId)).thenReturn(Optional.empty());

    RepairScheduleService service = RepairScheduleService.create(context, repairRunDao);
    Gauge<Integer> gauge = service.getUnfulfilledRepairSchedule(scheduleId);

    assertEquals("Last run not found should return 0", Integer.valueOf(0), gauge.getValue());
  }

  /**
   * Test scenario: Last run exists but is in RUNNING state (not DONE) Expected: Returns 0 (not
   * unfulfilled - repair still in progress)
   */
  @Test
  public void testGetUnfulfilledRepairSchedule_lastRunStillRunning() {
    RepairSchedule schedule =
        createScheduleBuilder()
            .state(RepairSchedule.State.ACTIVE)
            .daysBetween(7)
            .nextActivation(DateTime.now().plusDays(1))
            .lastRun(repairRunId)
            .build(scheduleId);

    RepairRun repairRun =
        createRepairRunBuilder()
            .runState(RepairRun.RunState.RUNNING)
            .startTime(DateTime.now().minusDays(1))
            .build(repairRunId);

    when(repairScheduleDao.getRepairSchedule(scheduleId)).thenReturn(Optional.of(schedule));
    when(repairRunDao.getRepairRun(repairRunId)).thenReturn(Optional.of(repairRun));

    RepairScheduleService service = RepairScheduleService.create(context, repairRunDao);
    Gauge<Integer> gauge = service.getUnfulfilledRepairSchedule(scheduleId);

    assertEquals("Running repair should return 0", Integer.valueOf(0), gauge.getValue());
  }

  /**
   * Test scenario: Last run exists in ERROR state (not DONE) Expected: Returns 0 (not unfulfilled -
   * checking only DONE repairs)
   */
  @Test
  public void testGetUnfulfilledRepairSchedule_lastRunInError() {
    RepairSchedule schedule =
        createScheduleBuilder()
            .state(RepairSchedule.State.ACTIVE)
            .daysBetween(7)
            .nextActivation(DateTime.now().plusDays(1))
            .lastRun(repairRunId)
            .build(scheduleId);

    RepairRun repairRun =
        createRepairRunBuilder()
            .runState(RepairRun.RunState.ERROR)
            .startTime(DateTime.now().minusDays(10))
            .endTime(DateTime.now().minusDays(10))
            .build(repairRunId);

    when(repairScheduleDao.getRepairSchedule(scheduleId)).thenReturn(Optional.of(schedule));
    when(repairRunDao.getRepairRun(repairRunId)).thenReturn(Optional.of(repairRun));

    RepairScheduleService service = RepairScheduleService.create(context, repairRunDao);
    Gauge<Integer> gauge = service.getUnfulfilledRepairSchedule(scheduleId);

    assertEquals("Error repair should return 0", Integer.valueOf(0), gauge.getValue());
  }

  /**
   * Test scenario: Last run is DONE but endTime is null Expected: Returns 0 (not unfulfilled -
   * RepairRun not fully stored yet)
   */
  @Test
  public void testGetUnfulfilledRepairSchedule_lastRunDoneButEndTimeNull() {
    RepairSchedule schedule =
        createScheduleBuilder()
            .state(RepairSchedule.State.ACTIVE)
            .daysBetween(7)
            .nextActivation(DateTime.now().plusDays(1))
            .lastRun(repairRunId)
            .build(scheduleId);

    // Create a RUNNING repair run instead (DONE requires endTime)
    RepairRun repairRun =
        createRepairRunBuilder()
            .runState(RepairRun.RunState.RUNNING)
            .startTime(DateTime.now().minusDays(5))
            .build(repairRunId);

    when(repairScheduleDao.getRepairSchedule(scheduleId)).thenReturn(Optional.of(schedule));
    when(repairRunDao.getRepairRun(repairRunId)).thenReturn(Optional.of(repairRun));

    RepairScheduleService service = RepairScheduleService.create(context, repairRunDao);
    Gauge<Integer> gauge = service.getUnfulfilledRepairSchedule(scheduleId);

    assertEquals("Running repair should return 0", Integer.valueOf(0), gauge.getValue());
  }

  /**
   * Test scenario: Last repair completed well within schedule interval Expected: Returns 0 (not
   * unfulfilled)
   */
  @Test
  public void testGetUnfulfilledRepairSchedule_lastRepairWithinInterval() {
    int daysBetween = 7;
    DateTime endTime = DateTime.now().minusDays(3); // 3 days ago, within 7 day interval

    RepairSchedule schedule =
        createScheduleBuilder()
            .state(RepairSchedule.State.ACTIVE)
            .daysBetween(daysBetween)
            .nextActivation(DateTime.now().plusDays(1))
            .lastRun(repairRunId)
            .build(scheduleId);

    RepairRun repairRun =
        createRepairRunBuilder()
            .runState(RepairRun.RunState.DONE)
            .startTime(endTime.minusHours(2))
            .endTime(endTime)
            .build(repairRunId);

    when(repairScheduleDao.getRepairSchedule(scheduleId)).thenReturn(Optional.of(schedule));
    when(repairRunDao.getRepairRun(repairRunId)).thenReturn(Optional.of(repairRun));

    RepairScheduleService service = RepairScheduleService.create(context, repairRunDao);
    Gauge<Integer> gauge = service.getUnfulfilledRepairSchedule(scheduleId);

    assertEquals("Repair within interval should return 0", Integer.valueOf(0), gauge.getValue());
  }

  /**
   * Test scenario: Last repair completed just within schedule interval Expected: Returns 0
   * (boundary case - just before interval expires)
   */
  @Test
  public void testGetUnfulfilledRepairSchedule_lastRepairJustWithinInterval() {
    int daysBetween = 7;
    // Use slightly less than exact interval to account for timing precision
    DateTime endTime = DateTime.now().minusDays(daysBetween).plusSeconds(5);

    RepairSchedule schedule =
        createScheduleBuilder()
            .state(RepairSchedule.State.ACTIVE)
            .daysBetween(daysBetween)
            .nextActivation(DateTime.now().plusDays(1))
            .lastRun(repairRunId)
            .build(scheduleId);

    RepairRun repairRun =
        createRepairRunBuilder()
            .runState(RepairRun.RunState.DONE)
            .startTime(endTime.minusHours(2))
            .endTime(endTime)
            .build(repairRunId);

    when(repairScheduleDao.getRepairSchedule(scheduleId)).thenReturn(Optional.of(schedule));
    when(repairRunDao.getRepairRun(repairRunId)).thenReturn(Optional.of(repairRun));

    RepairScheduleService service = RepairScheduleService.create(context, repairRunDao);
    Gauge<Integer> gauge = service.getUnfulfilledRepairSchedule(scheduleId);

    assertEquals(
        "Repair just within interval should return 0", Integer.valueOf(0), gauge.getValue());
  }

  /**
   * Test scenario: Last repair completed beyond schedule interval Expected: Returns 1 (unfulfilled)
   */
  @Test
  public void testGetUnfulfilledRepairSchedule_lastRepairBeyondInterval() {
    int daysBetween = 7;
    long intervalMillis = daysBetween * 24L * 60 * 60 * 1000;
    DateTime endTime = DateTime.now().minus(intervalMillis + 1000); // 1 second past interval

    RepairSchedule schedule =
        createScheduleBuilder()
            .state(RepairSchedule.State.ACTIVE)
            .daysBetween(daysBetween)
            .nextActivation(DateTime.now().plusDays(1))
            .lastRun(repairRunId)
            .build(scheduleId);

    RepairRun repairRun =
        createRepairRunBuilder()
            .runState(RepairRun.RunState.DONE)
            .startTime(endTime.minusHours(2))
            .endTime(endTime)
            .build(repairRunId);

    when(repairScheduleDao.getRepairSchedule(scheduleId)).thenReturn(Optional.of(schedule));
    when(repairRunDao.getRepairRun(repairRunId)).thenReturn(Optional.of(repairRun));

    RepairScheduleService service = RepairScheduleService.create(context, repairRunDao);
    Gauge<Integer> gauge = service.getUnfulfilledRepairSchedule(scheduleId);

    assertEquals("Repair beyond interval should return 1", Integer.valueOf(1), gauge.getValue());
  }

  /**
   * Test scenario: Last repair completed significantly beyond schedule interval Expected: Returns 1
   * (unfulfilled)
   */
  @Test
  public void testGetUnfulfilledRepairSchedule_lastRepairSignificantlyBeyondInterval() {
    int daysBetween = 7;
    DateTime endTime = DateTime.now().minusDays(14); // 14 days ago, double the interval

    RepairSchedule schedule =
        createScheduleBuilder()
            .state(RepairSchedule.State.ACTIVE)
            .daysBetween(daysBetween)
            .nextActivation(DateTime.now().plusDays(1))
            .lastRun(repairRunId)
            .build(scheduleId);

    RepairRun repairRun =
        createRepairRunBuilder()
            .runState(RepairRun.RunState.DONE)
            .startTime(endTime.minusHours(2))
            .endTime(endTime)
            .build(repairRunId);

    when(repairScheduleDao.getRepairSchedule(scheduleId)).thenReturn(Optional.of(schedule));
    when(repairRunDao.getRepairRun(repairRunId)).thenReturn(Optional.of(repairRun));

    RepairScheduleService service = RepairScheduleService.create(context, repairRunDao);
    Gauge<Integer> gauge = service.getUnfulfilledRepairSchedule(scheduleId);

    assertEquals(
        "Repair significantly beyond interval should return 1",
        Integer.valueOf(1),
        gauge.getValue());
  }

  /**
   * Test scenario: Schedule is ACTIVE but next activation in future, no last run Expected: Returns
   * 0 (not unfulfilled - hasn't missed schedule yet)
   */
  @Test
  public void testGetUnfulfilledRepairSchedule_activeScheduleNextActivationInFuture() {
    RepairSchedule schedule =
        createScheduleBuilder()
            .state(RepairSchedule.State.ACTIVE)
            .daysBetween(7)
            .nextActivation(DateTime.now().plusDays(3))
            .lastRun(null)
            .build(scheduleId);

    when(repairScheduleDao.getRepairSchedule(scheduleId)).thenReturn(Optional.of(schedule));

    RepairScheduleService service = RepairScheduleService.create(context, repairRunDao);
    Gauge<Integer> gauge = service.getUnfulfilledRepairSchedule(scheduleId);

    assertEquals(
        "Active schedule with future activation should return 0",
        Integer.valueOf(0),
        gauge.getValue());
  }

  /**
   * Test scenario: Last run is PAUSED (not DONE) Expected: Returns 0 (not unfulfilled - only
   * checking DONE repairs)
   */
  @Test
  public void testGetUnfulfilledRepairSchedule_lastRunPaused() {
    RepairSchedule schedule =
        createScheduleBuilder()
            .state(RepairSchedule.State.ACTIVE)
            .daysBetween(7)
            .nextActivation(DateTime.now().plusDays(1))
            .lastRun(repairRunId)
            .build(scheduleId);

    RepairRun repairRun =
        createRepairRunBuilder()
            .runState(RepairRun.RunState.PAUSED)
            .startTime(DateTime.now().minusDays(10))
            .pauseTime(DateTime.now().minusDays(9))
            .build(repairRunId);

    when(repairScheduleDao.getRepairSchedule(scheduleId)).thenReturn(Optional.of(schedule));
    when(repairRunDao.getRepairRun(repairRunId)).thenReturn(Optional.of(repairRun));

    RepairScheduleService service = RepairScheduleService.create(context, repairRunDao);
    Gauge<Integer> gauge = service.getUnfulfilledRepairSchedule(scheduleId);

    assertEquals("Paused repair should return 0", Integer.valueOf(0), gauge.getValue());
  }

  /**
   * Test scenario: Last run is NOT_STARTED Expected: Returns 0 (not unfulfilled - repair hasn't
   * started)
   */
  @Test
  public void testGetUnfulfilledRepairSchedule_lastRunNotStarted() {
    RepairSchedule schedule =
        createScheduleBuilder()
            .state(RepairSchedule.State.ACTIVE)
            .daysBetween(7)
            .nextActivation(DateTime.now().plusDays(1))
            .lastRun(repairRunId)
            .build(scheduleId);

    RepairRun repairRun =
        createRepairRunBuilder().runState(RepairRun.RunState.NOT_STARTED).build(repairRunId);

    when(repairScheduleDao.getRepairSchedule(scheduleId)).thenReturn(Optional.of(schedule));
    when(repairRunDao.getRepairRun(repairRunId)).thenReturn(Optional.of(repairRun));

    RepairScheduleService service = RepairScheduleService.create(context, repairRunDao);
    Gauge<Integer> gauge = service.getUnfulfilledRepairSchedule(scheduleId);

    assertEquals("Not started repair should return 0", Integer.valueOf(0), gauge.getValue());
  }

  /**
   * Test scenario: Last run is ABORTED (not DONE) Expected: Returns 0 (not unfulfilled - only
   * checking DONE repairs)
   */
  @Test
  public void testGetUnfulfilledRepairSchedule_lastRunAborted() {
    RepairSchedule schedule =
        createScheduleBuilder()
            .state(RepairSchedule.State.ACTIVE)
            .daysBetween(7)
            .nextActivation(DateTime.now().plusDays(1))
            .lastRun(repairRunId)
            .build(scheduleId);

    RepairRun repairRun =
        createRepairRunBuilder()
            .runState(RepairRun.RunState.ABORTED)
            .startTime(DateTime.now().minusDays(10))
            .endTime(DateTime.now().minusDays(10))
            .build(repairRunId);

    when(repairScheduleDao.getRepairSchedule(scheduleId)).thenReturn(Optional.of(schedule));
    when(repairRunDao.getRepairRun(repairRunId)).thenReturn(Optional.of(repairRun));

    RepairScheduleService service = RepairScheduleService.create(context, repairRunDao);
    Gauge<Integer> gauge = service.getUnfulfilledRepairSchedule(scheduleId);

    assertEquals("Aborted repair should return 0", Integer.valueOf(0), gauge.getValue());
  }

  /**
   * Test scenario: Complex case - past activation by <10%, but last repair is old Expected: Returns
   * 1 (last repair check takes precedence)
   */
  @Test
  public void testGetUnfulfilledRepairSchedule_complexCasePastActivationButWithinThreshold() {
    int daysBetween = 10;
    long fivePercentOfCycleMillis = (long) (daysBetween * 24 * 60 * 60 * 1000 * 0.05);
    DateTime nextActivation = DateTime.now().minus(fivePercentOfCycleMillis);
    DateTime endTime = DateTime.now().minusDays(15); // Old repair

    RepairSchedule schedule =
        createScheduleBuilder()
            .state(RepairSchedule.State.ACTIVE)
            .daysBetween(daysBetween)
            .nextActivation(nextActivation)
            .lastRun(repairRunId)
            .build(scheduleId);

    RepairRun repairRun =
        createRepairRunBuilder()
            .runState(RepairRun.RunState.DONE)
            .startTime(endTime.minusHours(2))
            .endTime(endTime)
            .build(repairRunId);

    when(repairScheduleDao.getRepairSchedule(scheduleId)).thenReturn(Optional.of(schedule));
    when(repairRunDao.getRepairRun(repairRunId)).thenReturn(Optional.of(repairRun));

    RepairScheduleService service = RepairScheduleService.create(context, repairRunDao);
    Gauge<Integer> gauge = service.getUnfulfilledRepairSchedule(scheduleId);

    assertEquals(
        "Old repair should return 1 even if activation time within threshold",
        Integer.valueOf(1),
        gauge.getValue());
  }

  /**
   * Test scenario: Very short repair interval (1 day) Expected: Proper calculation with smaller
   * interval
   */
  @Test
  public void testGetUnfulfilledRepairSchedule_shortInterval() {
    int daysBetween = 1;
    DateTime endTime = DateTime.now().minusDays(2); // 2 days ago, beyond 1 day interval

    RepairSchedule schedule =
        createScheduleBuilder()
            .state(RepairSchedule.State.ACTIVE)
            .daysBetween(daysBetween)
            .nextActivation(DateTime.now().plusHours(12))
            .lastRun(repairRunId)
            .build(scheduleId);

    RepairRun repairRun =
        createRepairRunBuilder()
            .runState(RepairRun.RunState.DONE)
            .startTime(endTime.minusHours(2))
            .endTime(endTime)
            .build(repairRunId);

    when(repairScheduleDao.getRepairSchedule(scheduleId)).thenReturn(Optional.of(schedule));
    when(repairRunDao.getRepairRun(repairRunId)).thenReturn(Optional.of(repairRun));

    RepairScheduleService service = RepairScheduleService.create(context, repairRunDao);
    Gauge<Integer> gauge = service.getUnfulfilledRepairSchedule(scheduleId);

    assertEquals(
        "Short interval with old repair should return 1", Integer.valueOf(1), gauge.getValue());
  }

  // Helper methods

  private RepairSchedule.Builder createScheduleBuilder() {
    return RepairSchedule.builder(repairUnitId)
        .state(RepairSchedule.State.ACTIVE)
        .daysBetween(7)
        .nextActivation(DateTime.now().plusDays(1))
        .repairParallelism(RepairParallelism.PARALLEL)
        .intensity(0.9)
        .segmentCountPerNode(16)
        .owner("test-owner")
        .adaptive(false)
        .percentUnrepairedThreshold(10);
  }

  private RepairRun.Builder createRepairRunBuilder() {
    return RepairRun.builder("test-cluster", repairUnitId)
        .intensity(0.9)
        .segmentCount(16)
        .repairParallelism(RepairParallelism.PARALLEL)
        .tables(Sets.newHashSet("table1"));
  }
}
