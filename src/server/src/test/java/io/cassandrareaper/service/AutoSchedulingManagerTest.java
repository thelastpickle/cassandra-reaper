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
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.storage.MemoryStorageFacade;

import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public final class AutoSchedulingManagerTest {

  private static final Cluster CLUSTER_1 = Cluster.builder()
      .withName("cluster1")
      .withSeedHosts(ImmutableSet.of("127.0.0.1"))
      .withState(Cluster.State.ACTIVE)
      .build();

  private static final Cluster CLUSTER_2 = Cluster.builder()
      .withName("cluster2")
      .withSeedHosts(ImmutableSet.of("127.0.0.1"))
      .withState(Cluster.State.ACTIVE)
      .build();

  private AppContext context;
  private AutoSchedulingManager repairAutoSchedulingManager;
  private ClusterRepairScheduler clusterRepairScheduler;

  @Before
  public void setup() throws ReaperException {
    context = new AppContext();
    context.storage = new MemoryStorageFacade();
    context.config = TestRepairConfiguration.defaultConfig();
    clusterRepairScheduler = mock(ClusterRepairScheduler.class);
    repairAutoSchedulingManager = new AutoSchedulingManager(context, clusterRepairScheduler);
  }

  @Test
  public void schedulesRepairForAllKeyspacesInAllClusters() throws Exception {
    context.storage.getClusterDao().addCluster(CLUSTER_1);
    context.storage.getClusterDao().addCluster(CLUSTER_2);

    repairAutoSchedulingManager.run();

    verify(clusterRepairScheduler).scheduleRepairs(CLUSTER_1);
    verify(clusterRepairScheduler).scheduleRepairs(CLUSTER_2);
  }

  @Test
  public void continueProcessingOtherClusterWhenSchedulingFailsForACluster() throws Exception {
    context.storage.getClusterDao().addCluster(CLUSTER_1);
    context.storage.getClusterDao().addCluster(CLUSTER_2);

    doThrow(new RuntimeException("throw for test purposes")).when(clusterRepairScheduler).scheduleRepairs(CLUSTER_1);

    repairAutoSchedulingManager.run();

    verify(clusterRepairScheduler).scheduleRepairs(CLUSTER_1);
    verify(clusterRepairScheduler).scheduleRepairs(CLUSTER_2);
  }

}