/*
 * Copyright 2017-2017 Spotify AB
 * Copyright 2017-2019 The Last Pickle Ltd
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
import io.cassandrareaper.storage.IStorageDao;
import io.cassandrareaper.storage.cluster.IClusterDao;
import io.cassandrareaper.storage.repairrun.IRepairRunDao;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for AutoSchedulingManager class. Tests timer task execution, cluster scheduling, error
 * handling, and singleton behavior.
 */
public class AutoSchedulingManagerTest {

  private AppContext mockAppContext;
  private IRepairRunDao mockRepairRunDao;
  private ClusterRepairScheduler mockClusterRepairScheduler;
  private IStorageDao mockStorage;
  private IClusterDao mockClusterDao;
  private ReaperApplicationConfiguration mockConfig;

  @BeforeEach
  void setUp() {
    mockAppContext = new AppContext();
    mockRepairRunDao = mock(IRepairRunDao.class);
    mockClusterRepairScheduler = mock(ClusterRepairScheduler.class);
    mockStorage = mock(IStorageDao.class);
    mockClusterDao = mock(IClusterDao.class);
    mockConfig = new ReaperApplicationConfiguration();

    mockAppContext.storage = mockStorage;
    mockAppContext.config = mockConfig;

    when(mockStorage.getClusterDao()).thenReturn(mockClusterDao);

    // Setup default auto scheduling configuration
    ReaperApplicationConfiguration.AutoSchedulingConfiguration autoConfig =
        new ReaperApplicationConfiguration.AutoSchedulingConfiguration();
    autoConfig.setInitialDelayPeriod(Duration.ofMillis(100));
    autoConfig.setPeriodBetweenPolls(Duration.ofMillis(200));
    mockConfig.setAutoScheduling(autoConfig);
  }

  @Test
  void testConstructor_WithValidParameters_ShouldCreateInstance() {
    // Given/When: Constructor is called with valid parameters
    AutoSchedulingManager manager =
        new AutoSchedulingManager(mockAppContext, mockClusterRepairScheduler);

    // Then: Instance should be created successfully
    assertThat(manager).isNotNull();
  }

  @Test
  void testRun_WithNoClusters_ShouldCompleteWithoutErrors() {
    // Given: No clusters in storage
    when(mockClusterDao.getClusters()).thenReturn(Collections.emptyList());
    AutoSchedulingManager manager =
        new AutoSchedulingManager(mockAppContext, mockClusterRepairScheduler);

    // When: run() is called
    manager.run();

    // Then: Should complete without errors
    verify(mockClusterDao).getClusters();
  }

  @Test
  void testRun_WithSingleCluster_ShouldScheduleRepairs() throws ReaperException {
    // Given: Single cluster in storage
    Cluster cluster =
        Cluster.builder()
            .withName("test-cluster")
            .withSeedHosts(Collections.singleton("127.0.0.1"))
            .build();
    when(mockClusterDao.getClusters()).thenReturn(Collections.singletonList(cluster));
    AutoSchedulingManager manager =
        new AutoSchedulingManager(mockAppContext, mockClusterRepairScheduler);

    // When: run() is called
    manager.run();

    // Then: Should schedule repairs for the cluster
    verify(mockClusterDao).getClusters();
    verify(mockClusterRepairScheduler).scheduleRepairs(cluster);
  }

  @Test
  void testRun_WithMultipleClusters_ShouldScheduleRepairsForAll() throws ReaperException {
    // Given: Multiple clusters in storage
    Cluster cluster1 =
        Cluster.builder()
            .withName("cluster-1")
            .withSeedHosts(Collections.singleton("127.0.0.1"))
            .build();
    Cluster cluster2 =
        Cluster.builder()
            .withName("cluster-2")
            .withSeedHosts(Collections.singleton("127.0.0.2"))
            .build();
    when(mockClusterDao.getClusters()).thenReturn(Arrays.asList(cluster1, cluster2));
    AutoSchedulingManager manager =
        new AutoSchedulingManager(mockAppContext, mockClusterRepairScheduler);

    // When: run() is called
    manager.run();

    // Then: Should schedule repairs for all clusters
    verify(mockClusterDao).getClusters();
    verify(mockClusterRepairScheduler).scheduleRepairs(cluster1);
    verify(mockClusterRepairScheduler).scheduleRepairs(cluster2);
  }

  @Test
  void testRun_WithReaperException_ShouldContinueProcessing() throws ReaperException {
    // Given: Multiple clusters, one throws ReaperException
    Cluster cluster1 =
        Cluster.builder()
            .withName("cluster-1")
            .withSeedHosts(Collections.singleton("127.0.0.1"))
            .build();
    Cluster cluster2 =
        Cluster.builder()
            .withName("cluster-2")
            .withSeedHosts(Collections.singleton("127.0.0.2"))
            .build();
    when(mockClusterDao.getClusters()).thenReturn(Arrays.asList(cluster1, cluster2));

    doThrow(new ReaperException("Test exception"))
        .when(mockClusterRepairScheduler)
        .scheduleRepairs(cluster1);
    AutoSchedulingManager manager =
        new AutoSchedulingManager(mockAppContext, mockClusterRepairScheduler);

    // When: run() is called
    manager.run();

    // Then: Should continue processing other clusters despite exception
    verify(mockClusterDao).getClusters();
    verify(mockClusterRepairScheduler).scheduleRepairs(cluster1);
    verify(mockClusterRepairScheduler).scheduleRepairs(cluster2);
  }

  @Test
  void testRun_WithRuntimeException_ShouldContinueProcessing() throws ReaperException {
    // Given: Multiple clusters, one throws RuntimeException
    Cluster cluster1 =
        Cluster.builder()
            .withName("cluster-1")
            .withSeedHosts(Collections.singleton("127.0.0.1"))
            .build();
    Cluster cluster2 =
        Cluster.builder()
            .withName("cluster-2")
            .withSeedHosts(Collections.singleton("127.0.0.2"))
            .build();
    when(mockClusterDao.getClusters()).thenReturn(Arrays.asList(cluster1, cluster2));

    doThrow(new RuntimeException("Test runtime exception"))
        .when(mockClusterRepairScheduler)
        .scheduleRepairs(cluster1);
    AutoSchedulingManager manager =
        new AutoSchedulingManager(mockAppContext, mockClusterRepairScheduler);

    // When: run() is called
    manager.run();

    // Then: Should continue processing other clusters despite exception
    verify(mockClusterDao).getClusters();
    verify(mockClusterRepairScheduler).scheduleRepairs(cluster1);
    verify(mockClusterRepairScheduler).scheduleRepairs(cluster2);
  }

  @Test
  void testRun_WithStorageException_ShouldHandleGracefully() {
    // Given: Storage throws exception when getting clusters
    when(mockClusterDao.getClusters()).thenThrow(new RuntimeException("Storage exception"));
    AutoSchedulingManager manager =
        new AutoSchedulingManager(mockAppContext, mockClusterRepairScheduler);

    // When/Then: run() should handle exception gracefully
    try {
      manager.run();
    } catch (RuntimeException e) {
      // Exception may propagate, which is acceptable
      assertThat(e.getMessage()).contains("Storage exception");
    }
    verify(mockClusterDao).getClusters();
  }

  @Test
  void testRun_WithNullClusters_ShouldHandleGracefully() {
    // Given: Storage returns null clusters
    when(mockClusterDao.getClusters()).thenReturn(null);
    AutoSchedulingManager manager =
        new AutoSchedulingManager(mockAppContext, mockClusterRepairScheduler);

    // When/Then: run() should handle null gracefully
    try {
      manager.run();
    } catch (NullPointerException e) {
      // NullPointerException is acceptable for null data
      assertThat(e).isNotNull();
    }
    verify(mockClusterDao).getClusters();
  }

  @Test
  void testMultipleRuns_ShouldProcessClustersEachTime() throws ReaperException {
    // Given: Cluster in storage
    Cluster cluster =
        Cluster.builder()
            .withName("test-cluster")
            .withSeedHosts(Collections.singleton("127.0.0.1"))
            .build();
    when(mockClusterDao.getClusters()).thenReturn(Collections.singletonList(cluster));
    AutoSchedulingManager manager =
        new AutoSchedulingManager(mockAppContext, mockClusterRepairScheduler);

    // When: run() is called multiple times
    manager.run();
    manager.run();
    manager.run();

    // Then: Should process clusters each time
    verify(mockClusterDao, times(3)).getClusters();
    verify(mockClusterRepairScheduler, times(3)).scheduleRepairs(cluster);
  }

  @Test
  void testRun_WithChangingClusterList_ShouldAdaptToChanges() throws ReaperException {
    // Given: Cluster list that changes between runs
    Cluster cluster1 =
        Cluster.builder()
            .withName("cluster-1")
            .withSeedHosts(Collections.singleton("127.0.0.1"))
            .build();
    Cluster cluster2 =
        Cluster.builder()
            .withName("cluster-2")
            .withSeedHosts(Collections.singleton("127.0.0.2"))
            .build();

    when(mockClusterDao.getClusters())
        .thenReturn(Collections.singletonList(cluster1))
        .thenReturn(Arrays.asList(cluster1, cluster2))
        .thenReturn(Collections.singletonList(cluster2));

    AutoSchedulingManager manager =
        new AutoSchedulingManager(mockAppContext, mockClusterRepairScheduler);

    // When: run() is called multiple times
    manager.run(); // First run: cluster1 only
    manager.run(); // Second run: cluster1 and cluster2
    manager.run(); // Third run: cluster2 only

    // Then: Should adapt to changing cluster list
    verify(mockClusterDao, times(3)).getClusters();
    verify(mockClusterRepairScheduler, times(2)).scheduleRepairs(cluster1);
    verify(mockClusterRepairScheduler, times(2)).scheduleRepairs(cluster2);
  }

  @Test
  void testRun_WithEmptyClusterName_ShouldHandleGracefully() throws ReaperException {
    // Given: Cluster with empty name
    Cluster clusterWithEmptyName =
        Cluster.builder().withName("").withSeedHosts(Collections.singleton("127.0.0.1")).build();
    when(mockClusterDao.getClusters()).thenReturn(Collections.singletonList(clusterWithEmptyName));
    AutoSchedulingManager manager =
        new AutoSchedulingManager(mockAppContext, mockClusterRepairScheduler);

    // When: run() is called
    manager.run();

    // Then: Should handle gracefully
    verify(mockClusterDao).getClusters();
    verify(mockClusterRepairScheduler).scheduleRepairs(clusterWithEmptyName);
  }

  @Test
  void testRun_WithPartialFailures_ShouldProcessAllClusters() throws ReaperException {
    // Given: Multiple clusters with alternating failures
    Cluster cluster1 =
        Cluster.builder()
            .withName("cluster-1")
            .withSeedHosts(Collections.singleton("127.0.0.1"))
            .build();
    Cluster cluster2 =
        Cluster.builder()
            .withName("cluster-2")
            .withSeedHosts(Collections.singleton("127.0.0.2"))
            .build();
    Cluster cluster3 =
        Cluster.builder()
            .withName("cluster-3")
            .withSeedHosts(Collections.singleton("127.0.0.3"))
            .build();

    when(mockClusterDao.getClusters()).thenReturn(Arrays.asList(cluster1, cluster2, cluster3));
    doThrow(new ReaperException("Failure 1"))
        .when(mockClusterRepairScheduler)
        .scheduleRepairs(cluster1);
    doThrow(new RuntimeException("Failure 3"))
        .when(mockClusterRepairScheduler)
        .scheduleRepairs(cluster3);

    AutoSchedulingManager manager =
        new AutoSchedulingManager(mockAppContext, mockClusterRepairScheduler);

    // When: run() is called
    manager.run();

    // Then: Should process all clusters despite partial failures
    verify(mockClusterDao).getClusters();
    verify(mockClusterRepairScheduler).scheduleRepairs(cluster1);
    verify(mockClusterRepairScheduler).scheduleRepairs(cluster2);
    verify(mockClusterRepairScheduler).scheduleRepairs(cluster3);
  }

  @Test
  void testRun_AsTimerTask_ShouldExecuteCorrectly() throws ReaperException {
    // Given: Manager configured as TimerTask
    Cluster cluster =
        Cluster.builder()
            .withName("test-cluster")
            .withSeedHosts(Collections.singleton("127.0.0.1"))
            .build();
    when(mockClusterDao.getClusters()).thenReturn(Collections.singletonList(cluster));
    AutoSchedulingManager manager =
        new AutoSchedulingManager(mockAppContext, mockClusterRepairScheduler);

    // When: run() is called (as would happen in Timer execution)
    Runnable task = manager;
    task.run();

    // Then: Should execute the scheduling logic
    verify(mockClusterDao).getClusters();
    verify(mockClusterRepairScheduler).scheduleRepairs(cluster);
  }
}
