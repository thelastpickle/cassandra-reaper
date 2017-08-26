package com.spotify.reaper.unit.service;

import com.spotify.reaper.AppContext;
import com.spotify.reaper.repair.ReaperException;
import com.spotify.reaper.cluster.Cluster;
import com.spotify.reaper.scheduler.AutoSchedulingManager;
import com.spotify.reaper.scheduler.ClusterRepairScheduler;
import com.spotify.reaper.storage.memory.MemoryStorage;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static org.mockito.Mockito.*;

public class AutoSchedulingManagerTest {

  private static final Cluster CLUSTER_1 = new Cluster("cluster1", null, Collections.singleton(null));
  private static final Cluster CLUSTER_2 = new Cluster("cluster2", null, Collections.singleton(null));
  private AppContext context;
  private AutoSchedulingManager repairAutoSchedulingManager;
  private ClusterRepairScheduler clusterRepairScheduler;

  @Before
  public void setup() throws ReaperException {
    context = new AppContext();
    context.storage = new MemoryStorage();
    context.config = TestRepairConfiguration.defaultConfig();
    clusterRepairScheduler = mock(ClusterRepairScheduler.class);
    repairAutoSchedulingManager = new AutoSchedulingManager(context, clusterRepairScheduler);
  }

  @Test
  public void schedulesRepairForAllKeyspacesInAllClusters() throws Exception {
    context.storage.addCluster(CLUSTER_1);
    context.storage.addCluster(CLUSTER_2);

    repairAutoSchedulingManager.run();

    verify(clusterRepairScheduler).scheduleRepairs(CLUSTER_1);
    verify(clusterRepairScheduler).scheduleRepairs(CLUSTER_2);
  }

  @Test
  public void continueProcessingOtherClusterWhenSchedulingFailsForACluster() throws Exception {
    context.storage.addCluster(CLUSTER_1);
    context.storage.addCluster(CLUSTER_2);

    doThrow(new RuntimeException("throw for test purposes")).when(clusterRepairScheduler).scheduleRepairs(CLUSTER_1);

    repairAutoSchedulingManager.run();

    verify(clusterRepairScheduler).scheduleRepairs(CLUSTER_1);
    verify(clusterRepairScheduler).scheduleRepairs(CLUSTER_2);
  }

}
