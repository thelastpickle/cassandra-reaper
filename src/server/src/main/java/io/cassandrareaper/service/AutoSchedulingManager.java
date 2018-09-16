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
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Cluster;

import java.util.Collection;
import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AutoSchedulingManager extends TimerTask {

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
      LOG.info(
          "Starting new {} instance. First check in {}ms. Subsequent polls every {}ms",
          AutoSchedulingManager.class.getSimpleName(),
          context.config.getAutoScheduling().getInitialDelayPeriod().toMillis(),
          context.config.getAutoScheduling().getPeriodBetweenPolls().toMillis());

      repairAutoSchedulingManager = new AutoSchedulingManager(context);
      Timer timer = new Timer("AutoSchedulingManagerTimer");
      timer.schedule(
          repairAutoSchedulingManager,
          context.config.getAutoScheduling().getInitialDelayPeriod().toMillis(),
          context.config.getAutoScheduling().getPeriodBetweenPolls().toMillis());
    } else {
      LOG.warn(
          "there is already one instance of {} running, not starting new one",
          AutoSchedulingManager.class.getSimpleName());
    }
  }

  @Override
  public void run() {
    LOG.debug("Checking cluster keyspaces to identify which ones require repair schedules...");
    Collection<Cluster> clusters;
    try {
      clusters = context.storage.getClusters();
      for (Cluster cluster : clusters) {
        try {
          clusterRepairScheduler.scheduleRepairs(cluster);
        } catch (ReaperException | RuntimeException e) {
          LOG.error("Error while scheduling repairs for cluster {}", cluster, e);
        }
      }
    } catch (ReaperException e1) {
      LOG.error("Error while listing cluster to autoschedule repairs", e1);
    }
  }
}
