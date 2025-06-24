/*
 * Copyright 2015-2017 Spotify AB
 * Copyright 2016-2019 The Last Pickle Ltd
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
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.RepairRun;
import io.cassandrareaper.management.ClusterFacade;
import io.cassandrareaper.storage.repairrun.IRepairRunDao;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.codahale.metrics.MetricRegistry;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.google.common.collect.ImmutableSet;
import org.apache.cassandra.repair.RepairParallelism;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public final class RepairManagerEnhancedTest {

  @Mock private AppContext mockContext;
  @Mock private ClusterFacade mockClusterFacade;
  @Mock private ScheduledExecutorService mockExecutor;
  @Mock private IRepairRunDao mockRepairRunDao;
  @Mock private SchedulingManager mockSchedulingManager;

  private RepairManager repairManager;

  private static final String CLUSTER_NAME = "test-cluster";
  private static final Set<String> COLUMN_FAMILIES = ImmutableSet.of("test_table");
  private static final double INTENSITY = 0.5;

  @Before
  public void setUp() throws ReaperException {
    MockitoAnnotations.initMocks(this);

    // Set up mock context - these are public fields, not methods, so we set them directly
    mockContext.metricRegistry = new MetricRegistry();
    mockContext.config = new ReaperApplicationConfiguration();
    mockContext.schedulingManager = mockSchedulingManager;
    doNothing().when(mockSchedulingManager).maybeRegisterRepairRunCompleted(any());
  }

  @After
  public void tearDown() {
    if (repairManager != null) {
      repairManager.close();
    }
  }

  @Test
  public void testCreateRepairManagerWithAllParameters() throws ReaperException {
    // Test the static factory method with all parameters
    RepairManager manager =
        RepairManager.create(
            mockContext,
            mockClusterFacade,
            mockExecutor,
            1000L,
            TimeUnit.MILLISECONDS,
            5,
            mockRepairRunDao);

    assertNotNull(manager);
  }

  @Test
  public void testPauseRepairRun() throws ReaperException {
    // Setup
    when(mockRepairRunDao.updateRepairRun(any())).thenReturn(true);

    repairManager =
        RepairManager.create(
            mockContext,
            mockClusterFacade,
            mockExecutor,
            100L,
            TimeUnit.MILLISECONDS,
            1,
            mockRepairRunDao);

    RepairRun repairRun =
        RepairRun.builder(CLUSTER_NAME, Uuids.timeBased())
            .intensity(INTENSITY)
            .segmentCount(1)
            .repairParallelism(RepairParallelism.PARALLEL)
            .tables(COLUMN_FAMILIES)
            .runState(RepairRun.RunState.RUNNING)
            .startTime(DateTime.now())
            .build(Uuids.timeBased());

    // Execute
    RepairRun result = repairManager.pauseRepairRun(repairRun);

    // Verify
    assertEquals(RepairRun.RunState.PAUSED, result.getRunState());
    assertNotNull(result.getPauseTime());
    verify(mockRepairRunDao).updateRepairRun(any());
  }

  @Test
  public void testAbortRepairRun() throws ReaperException {
    // Setup
    when(mockRepairRunDao.updateRepairRun(any())).thenReturn(true);

    repairManager =
        RepairManager.create(
            mockContext,
            mockClusterFacade,
            mockExecutor,
            100L,
            TimeUnit.MILLISECONDS,
            1,
            mockRepairRunDao);

    RepairRun repairRun =
        RepairRun.builder(CLUSTER_NAME, Uuids.timeBased())
            .intensity(INTENSITY)
            .segmentCount(1)
            .repairParallelism(RepairParallelism.PARALLEL)
            .tables(COLUMN_FAMILIES)
            .runState(RepairRun.RunState.RUNNING)
            .startTime(DateTime.now())
            .build(Uuids.timeBased());

    // Execute
    RepairRun result = repairManager.abortRepairRun(repairRun);

    // Verify
    assertEquals(RepairRun.RunState.ABORTED, result.getRunState());
    assertNotNull(result.getEndTime());
    verify(mockRepairRunDao).updateRepairRun(any());
  }

  @Test
  public void testUpdateRepairRunIntensity() throws ReaperException {
    // Setup
    when(mockRepairRunDao.updateRepairRun(any(), any())).thenReturn(true);

    repairManager =
        RepairManager.create(
            mockContext,
            mockClusterFacade,
            mockExecutor,
            100L,
            TimeUnit.MILLISECONDS,
            1,
            mockRepairRunDao);

    RepairRun repairRun =
        RepairRun.builder(CLUSTER_NAME, Uuids.timeBased())
            .intensity(INTENSITY)
            .segmentCount(1)
            .repairParallelism(RepairParallelism.PARALLEL)
            .tables(COLUMN_FAMILIES)
            .runState(RepairRun.RunState.RUNNING)
            .startTime(DateTime.now().minusHours(1))
            .build(Uuids.timeBased());

    double newIntensity = 0.8;

    // Execute
    RepairRun result = repairManager.updateRepairRunIntensity(repairRun, newIntensity);

    // Verify
    assertEquals(newIntensity, result.getIntensity(), 0.001);
    verify(mockRepairRunDao).updateRepairRun(any(), any());
  }

  @Test
  public void testCountRepairRunnersForCluster() throws ReaperException {
    // Setup
    repairManager =
        RepairManager.create(
            mockContext,
            mockClusterFacade,
            mockExecutor,
            100L,
            TimeUnit.MILLISECONDS,
            5,
            mockRepairRunDao);

    // Create mock repair runners
    RepairRunner runner1 = mock(RepairRunner.class);
    RepairRunner runner2 = mock(RepairRunner.class);
    RepairRunner runner3 = mock(RepairRunner.class);

    Cluster cluster1 =
        Cluster.builder().withName("cluster1").withSeedHosts(ImmutableSet.of("127.0.0.1")).build();
    Cluster cluster2 =
        Cluster.builder().withName("cluster2").withSeedHosts(ImmutableSet.of("127.0.0.2")).build();

    when(runner1.getCluster()).thenReturn(cluster1);
    when(runner2.getCluster()).thenReturn(cluster1);
    when(runner3.getCluster()).thenReturn(cluster2);

    // Add runners to the manager
    repairManager.repairRunners.put(Uuids.timeBased(), runner1);
    repairManager.repairRunners.put(Uuids.timeBased(), runner2);
    repairManager.repairRunners.put(Uuids.timeBased(), runner3);

    // Execute & Verify
    assertEquals(2, repairManager.countRepairRunnersForCluster("cluster1"));
    assertEquals(1, repairManager.countRepairRunnersForCluster("cluster2"));
    assertEquals(0, repairManager.countRepairRunnersForCluster("nonexistent"));
  }

  @Test
  public void testRemoveRunner() throws ReaperException {
    // Setup
    repairManager =
        RepairManager.create(
            mockContext,
            mockClusterFacade,
            mockExecutor,
            100L,
            TimeUnit.MILLISECONDS,
            1,
            mockRepairRunDao);

    UUID runId = Uuids.timeBased();
    RepairRunner mockRunner = mock(RepairRunner.class);
    when(mockRunner.getRepairRunId()).thenReturn(runId);

    // Add runner manually
    repairManager.repairRunners.put(runId, mockRunner);
    assertTrue(repairManager.repairRunners.containsKey(runId));

    // Execute
    repairManager.removeRunner(mockRunner);

    // Verify
    assertFalse(repairManager.repairRunners.containsKey(runId));
  }

  @Test
  public void testClose() throws ReaperException {
    // Setup
    ScheduledExecutorService mockScheduledExecutor = mock(ScheduledExecutorService.class);
    repairManager =
        RepairManager.create(
            mockContext,
            mockClusterFacade,
            mockScheduledExecutor,
            1000L,
            TimeUnit.MILLISECONDS,
            1,
            mockRepairRunDao);

    // Execute
    repairManager.close();

    // Verify
    verify(mockScheduledExecutor).shutdownNow();
  }

  @Test
  public void testUpdateIntensityDaoUpdateFailure() throws ReaperException {
    // Setup with mock to simulate DAO failure
    when(mockRepairRunDao.updateRepairRun(any(), any())).thenReturn(false);

    repairManager =
        RepairManager.create(
            mockContext,
            mockClusterFacade,
            mockExecutor,
            100L,
            TimeUnit.MILLISECONDS,
            1,
            mockRepairRunDao);

    RepairRun repairRun =
        RepairRun.builder(CLUSTER_NAME, Uuids.timeBased())
            .intensity(INTENSITY)
            .segmentCount(1)
            .repairParallelism(RepairParallelism.PARALLEL)
            .tables(COLUMN_FAMILIES)
            .runState(RepairRun.RunState.RUNNING)
            .startTime(DateTime.now().minusHours(1))
            .build(Uuids.timeBased());

    // Execute & Verify
    assertThrows(
        ReaperException.class, () -> repairManager.updateRepairRunIntensity(repairRun, 0.8));
  }

  @Test
  public void testResumeRunningRepairRunsBasic() throws ReaperException {
    // Setup
    repairManager =
        RepairManager.create(
            mockContext,
            mockClusterFacade,
            mockExecutor,
            100L,
            TimeUnit.MILLISECONDS,
            1,
            mockRepairRunDao);

    // Mock empty results to avoid complex setup
    when(mockRepairRunDao.getRepairRunsWithState(RepairRun.RunState.RUNNING))
        .thenReturn(Collections.emptyList());
    when(mockRepairRunDao.getRepairRunsWithState(RepairRun.RunState.PAUSED))
        .thenReturn(Collections.emptyList());

    // Execute - should not throw exception with empty results
    repairManager.resumeRunningRepairRuns();

    // Verify
    verify(mockRepairRunDao).getRepairRunsWithState(RepairRun.RunState.RUNNING);
    verify(mockRepairRunDao).getRepairRunsWithState(RepairRun.RunState.PAUSED);
  }

  @Test
  public void testRepairRunnersMapAccess() throws ReaperException {
    // Setup
    repairManager =
        RepairManager.create(
            mockContext,
            mockClusterFacade,
            mockExecutor,
            100L,
            TimeUnit.MILLISECONDS,
            1,
            mockRepairRunDao);

    // Add some mock runners
    UUID runId1 = Uuids.timeBased();
    UUID runId2 = Uuids.timeBased();
    RepairRunner runner1 = mock(RepairRunner.class);
    RepairRunner runner2 = mock(RepairRunner.class);

    when(runner1.getRepairRunId()).thenReturn(runId1);
    when(runner2.getRepairRunId()).thenReturn(runId2);

    repairManager.repairRunners.put(runId1, runner1);
    repairManager.repairRunners.put(runId2, runner2);

    // Execute
    Set<UUID> runningIds = repairManager.repairRunners.keySet();

    // Verify
    assertEquals(2, runningIds.size());
    assertTrue(runningIds.contains(runId1));
    assertTrue(runningIds.contains(runId2));
  }

  @Test
  public void testRepairRunnersMapEmpty() throws ReaperException {
    // Setup
    repairManager =
        RepairManager.create(
            mockContext,
            mockClusterFacade,
            mockExecutor,
            100L,
            TimeUnit.MILLISECONDS,
            1,
            mockRepairRunDao);

    // Execute
    Set<UUID> runningIds = repairManager.repairRunners.keySet();

    // Verify
    assertEquals(0, runningIds.size());
    assertTrue(runningIds.isEmpty());
  }
}
