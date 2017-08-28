package com.spotify.reaper.service;

import com.spotify.reaper.AppContext;
import com.spotify.reaper.core.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class AutoSchedulingManager extends TimerTask {

  private static final Logger LOG = LoggerFactory.getLogger(AutoSchedulingManager.class);
  private static AutoSchedulingManager repairAutoSchedulingManager;

  private final AppContext context;
  private final ClusterRepairScheduler clusterRepairScheduler;

  private AutoSchedulingManager(AppContext context) {
    this(context, new ClusterRepairScheduler(context));
  }

  public AutoSchedulingManager(AppContext context, ClusterRepairScheduler clusterRepairScheduler) {
    this.context = context;
    this.clusterRepairScheduler = clusterRepairScheduler;
  }
  
  public static synchronized void start(AppContext context) {
    if (null == repairAutoSchedulingManager) {
      LOG.info("Starting new {} instance. First check in {}ms. Subsequent polls every {}ms",
          AutoSchedulingManager.class.getSimpleName(),
          context.config.getAutoScheduling().getInitialDelayPeriod().toMillis(),
          context.config.getAutoScheduling().getPeriodBetweenPolls().toMillis()
      );

      repairAutoSchedulingManager = new AutoSchedulingManager(context);
      Timer timer = new Timer("AutoSchedulingManagerTimer");
      timer.schedule(
          repairAutoSchedulingManager,
          context.config.getAutoScheduling().getInitialDelayPeriod().toMillis(),
          context.config.getAutoScheduling().getPeriodBetweenPolls().toMillis()
      );
    } else {
      LOG.warn("there is already one instance of {} running, not starting new one", AutoSchedulingManager.class.getSimpleName());
    }
  }

  @Override
  public void run() {
    LOG.debug("Checking cluster keyspaces to identify which ones require repair schedules...");
    Collection<Cluster> clusters = context.storage.getClusters();
    for (Cluster cluster : clusters) {
      try {
        clusterRepairScheduler.scheduleRepairs(cluster);
      } catch (Exception e) {
        LOG.error("Error while scheduling repairs for cluster {}", cluster, e);
      }
    }
  }

}
