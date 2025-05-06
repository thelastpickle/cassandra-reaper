/*
 * Copyright 2018-2019 The Last Pickle Ltd
 *
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
import io.cassandrareaper.core.RepairRun.RunState;
import io.cassandrareaper.storage.IStorageDao;
import io.cassandrareaper.storage.cluster.IClusterDao;
import io.cassandrareaper.storage.repairrun.IRepairRunDao;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.cassandra.repair.RepairParallelism;
import org.joda.time.DateTime;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class PurgeServiceTest {

  private static final Logger LOG = LoggerFactory.getLogger(PurgeServiceTest.class);
  private static final String CLUSTER_NAME = "test";
  private static final Set<String> SEEDS = ImmutableSet.of("127.0.0.1");
  private static final Set<String> TABLES = ImmutableSet.of("table1");

  @Test
  public void testPurgeByDate() throws InterruptedException, ReaperException {
    AppContext context = new AppContext();
    context.config = new ReaperApplicationConfiguration();
    context.config.setPurgeRecordsAfterInDays(1);

    // Create storage mock
    context.storage = mock(IStorageDao.class);

    List<Cluster> clusters = Arrays.asList(Cluster.builder().withName(CLUSTER_NAME).withSeedHosts(SEEDS).build());
    IClusterDao mockedClusterDao = mock(IClusterDao.class);
    when(context.storage.getClusterDao()).thenReturn(mockedClusterDao);
    when(context.storage.getClusterDao().getClusters()).thenReturn(clusters);

    // Add repair runs to the mock
    List<RepairRun> repairRuns = Lists.newArrayList();
    DateTime currentDate = DateTime.now();
    for (int i = 0; i < 10; i++) {
      UUID repairUnitId = Uuids.timeBased();
      DateTime startTime = currentDate.minusDays(i).minusHours(1);

      repairRuns.add(
          RepairRun.builder(CLUSTER_NAME, repairUnitId)
              .startTime(startTime)
              .intensity(0.9)
              .segmentCount(10)
              .repairParallelism(RepairParallelism.DATACENTER_AWARE)
              .tables(TABLES)
              .endTime(startTime.plusSeconds(1))
              .runState(RunState.DONE)
              .build(Uuids.timeBased()));
    }
    IRepairRunDao mockedRepairRunDao = mock(IRepairRunDao.class);
    when(context.storage.getRepairRunDao()).thenReturn(mockedRepairRunDao);
    when(mockedRepairRunDao.getRepairRunsForCluster(anyString(), any())).thenReturn(repairRuns);

    // Invoke the purge manager
    int purged = PurgeService.create(context, context.storage.getRepairRunDao()).purgeDatabase();

    // Check that runs were removed
    assertEquals(9, purged);
  }

  @Test
  public void testPurgeByHistoryDepth() throws InterruptedException, ReaperException {
    AppContext context = new AppContext();
    context.config = new ReaperApplicationConfiguration();
    context.config.setNumberOfRunsToKeepPerUnit(5);

    // Create storage mock
    context.storage = mock(IStorageDao.class);

    List<Cluster> clusters = Arrays.asList(Cluster.builder().withName(CLUSTER_NAME).withSeedHosts(SEEDS).build());
    IClusterDao mockedClusterDao = mock(IClusterDao.class);
    when(context.storage.getClusterDao()).thenReturn(mockedClusterDao);
    when(context.storage.getClusterDao().getClusters()).thenReturn(clusters);

    // Add repair runs to the mock
    List<RepairRun> repairRuns = Lists.newArrayList();
    DateTime currentDate = DateTime.now();
    UUID repairUnitId = Uuids.timeBased();
    for (int i = 0; i < 20; i++) {
      DateTime startTime = currentDate.minusDays(i).minusHours(1);

      repairRuns.add(
          RepairRun.builder(CLUSTER_NAME, repairUnitId)
              .startTime(startTime)
              .intensity(0.9)
              .segmentCount(10)
              .repairParallelism(RepairParallelism.DATACENTER_AWARE)
              .tables(TABLES)
              .endTime(startTime.plusSeconds(1))
              .runState(RunState.DONE)
              .build(Uuids.timeBased()));
    }

    IRepairRunDao mockedRepairRunDao = mock(IRepairRunDao.class);
    when(context.storage.getRepairRunDao()).thenReturn(mockedRepairRunDao);
    when(mockedRepairRunDao.getRepairRunsForCluster(anyString(), any())).thenReturn(repairRuns);

    // Invoke the purge manager
    int purged = PurgeService.create(context, context.storage.getRepairRunDao()).purgeDatabase();

    // Check that runs were removed
    assertEquals(15, purged);
  }

  @Test
  public void testSkipPurgeOngoingRuns() throws InterruptedException, ReaperException {
    AppContext context = new AppContext();
    context.config = new ReaperApplicationConfiguration();
    context.config.setPurgeRecordsAfterInDays(1);

    // Create storage mock
    context.storage = mock(IStorageDao.class);

    List<Cluster> clusters = Arrays.asList(Cluster.builder().withName(CLUSTER_NAME).withSeedHosts(SEEDS).build());

    IClusterDao mockedClusterDao = mock(IClusterDao.class);
    when(context.storage.getClusterDao()).thenReturn(mockedClusterDao);
    when(mockedClusterDao.getClusters()).thenReturn(clusters);

    // Add repair runs to the mock
    List<RepairRun> repairRuns = Lists.newArrayList();
    DateTime currentDate = DateTime.now();
    for (int i = 0; i < 10; i++) {
      UUID repairUnitId = Uuids.timeBased();
      DateTime startTime = currentDate.minusDays(i).minusHours(1);

      repairRuns.add(
          RepairRun.builder(CLUSTER_NAME, repairUnitId)
              .startTime(startTime)
              .intensity(0.9)
              .segmentCount(10)
              .repairParallelism(RepairParallelism.DATACENTER_AWARE)
              .tables(TABLES)
              .pauseTime(startTime.plusSeconds(1))
              .runState(RunState.PAUSED)
              .build(Uuids.timeBased()));
    }

    IRepairRunDao mockedRepairRunDao = mock(IRepairRunDao.class);
    when(context.storage.getRepairRunDao()).thenReturn(mockedRepairRunDao);
    when(mockedRepairRunDao.getRepairRunsForCluster(anyString(), any())).thenReturn(repairRuns);

    // Invoke the purge manager
    int purged = PurgeService.create(context, context.storage.getRepairRunDao()).purgeDatabase();

    // Check that runs were removed
    assertEquals(0, purged);
  }

}