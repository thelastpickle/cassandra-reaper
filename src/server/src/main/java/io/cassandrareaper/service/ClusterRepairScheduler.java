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
import io.cassandrareaper.core.RepairSchedule;
import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.jmx.ClusterFacade;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.String.format;

public final class ClusterRepairScheduler {

  private static final Logger LOG = LoggerFactory.getLogger(ClusterRepairScheduler.class);
  private static final String REPAIR_OWNER = "auto-scheduling";
  private static final String SYSTEM_KEYSPACE_PREFIX = "system";

  private final AppContext context;
  private final RepairUnitService repairUnitService;
  private final RepairScheduleService repairScheduleService;

  public ClusterRepairScheduler(AppContext context) {
    this.context = context;
    this.repairUnitService = RepairUnitService.create(context);
    this.repairScheduleService = RepairScheduleService.create(context);
  }

  public void scheduleRepairs(Cluster cluster) throws ReaperException {
    List<String> excludedClusters = context
        .config
        .getAutoScheduling()
        .getExcludedClusters()
        .stream()
        .map(Cluster::toSymbolicName)
        .collect(Collectors.toList());
    if (excludedClusters.contains(cluster.getName())) {
      LOG.debug("Not creating schedules for excluded cluster {}.", cluster.getName());
      return;
    }
    AtomicInteger scheduleIndex = new AtomicInteger();
    ScheduledRepairDiffView schedulesDiff = ScheduledRepairDiffView.compareWithExistingSchedules(context, cluster);
    schedulesDiff.keyspacesDeleted().forEach(keyspace -> deleteRepairSchedule(cluster, keyspace));
    schedulesDiff
        .keyspacesWithoutSchedules()
        .stream()
        .filter(keyspace -> keyspaceCandidateForRepair(cluster, keyspace))
        .forEach(
            keyspace
              -> createRepairSchedule(cluster, keyspace, nextActivationStartDate(scheduleIndex.getAndIncrement())));
  }

  private DateTime nextActivationStartDate(int scheduleIndex) {
    DateTime timeBeforeFirstSchedule
        = DateTime.now().plus(context.config.getAutoScheduling().getTimeBeforeFirstSchedule().toMillis());

    if (context.config.getAutoScheduling().hasScheduleSpreadPeriod()) {
      return timeBeforeFirstSchedule.plus(
          scheduleIndex * context.config.getAutoScheduling().getScheduleSpreadPeriod().toMillis());
    }
    return timeBeforeFirstSchedule;
  }

  private void deleteRepairSchedule(Cluster cluster, String keyspace) {
    Collection<RepairSchedule> scheduleCollection
        = context.storage.getRepairSchedulesForClusterAndKeyspace(cluster.getName(), keyspace);

    scheduleCollection.forEach(
        repairSchedule -> {
          context.storage.deleteRepairSchedule(repairSchedule.getId());
          LOG.info("Scheduled repair deleted: {}", repairSchedule);
        });
  }

  private boolean keyspaceCandidateForRepair(Cluster cluster, String keyspace) {
    if (keyspace.toLowerCase().startsWith(ClusterRepairScheduler.SYSTEM_KEYSPACE_PREFIX)
        || context.config.getAutoScheduling().getExcludedKeyspaces().contains(keyspace)) {
      LOG.debug("Scheduled repair skipped for system keyspace {} in cluster {}.", keyspace, cluster.getName());
      return false;
    }
    if (repairUnitService.getTableNamesForKeyspace(cluster, keyspace).isEmpty()) {
      LOG.warn(
          "No tables found for keyspace {} in cluster {}. No repair will be scheduled for this keyspace.",
          keyspace,
          cluster.getName());
      return false;
    }
    return true;
  }

  private void createRepairSchedule(Cluster cluster, String keyspace, DateTime nextActivationTime) {
    boolean incrementalRepair = context.config.getAutoScheduling().incremental();

    RepairUnit.Builder builder = RepairUnit.builder()
        .clusterName(cluster.getName())
        .keyspaceName(keyspace)
        .incrementalRepair(incrementalRepair)
        .repairThreadCount(context.config.getRepairThreadCount())
        .timeout(context.config.getHangingRepairTimeoutMins());

    RepairSchedule repairSchedule = repairScheduleService.storeNewRepairSchedule(
            cluster,
            repairUnitService.getOrCreateRepairUnit(cluster, builder).get(),
            context.config.getScheduleDaysBetween(),
            nextActivationTime,
            REPAIR_OWNER,
            context.config.getSegmentCountPerNode(),
            context.config.getRepairParallelism(),
            context.config.getRepairIntensity(),
            false,
            context.config.getAutoScheduling().isAdaptive(),
            context.config.getAutoScheduling().getPercentUnrepairedThreshold());

    LOG.info("Scheduled repair created: {}", repairSchedule);
  }

  private static class ScheduledRepairDiffView {

    private final ImmutableSet<String> keyspacesThatRequireSchedules;
    private final ImmutableSet<String> keyspacesDeleted;

    ScheduledRepairDiffView(AppContext context, Cluster cluster) throws ReaperException {
      Set<String> allKeyspacesInCluster = keyspacesInCluster(context, cluster);
      Set<String> keyspacesThatHaveSchedules = keyspacesThatHaveSchedules(context, cluster);

      keyspacesThatRequireSchedules
          = Sets.difference(allKeyspacesInCluster, keyspacesThatHaveSchedules).immutableCopy();

      keyspacesDeleted = Sets.difference(keyspacesThatHaveSchedules, allKeyspacesInCluster).immutableCopy();
    }

    static ScheduledRepairDiffView compareWithExistingSchedules(AppContext context, Cluster cluster)
        throws ReaperException {

      return new ScheduledRepairDiffView(context, cluster);
    }

    Set<String> keyspacesWithoutSchedules() {
      return keyspacesThatRequireSchedules;
    }

    Set<String> keyspacesDeleted() {
      return keyspacesDeleted;
    }

    private Set<String> keyspacesThatHaveSchedules(AppContext context, Cluster cluster) {
      Collection<RepairSchedule> currentSchedules = context.storage.getRepairSchedulesForCluster(cluster.getName());
      return currentSchedules
          .stream()
          .map(repairSchedule -> context.storage.getRepairUnit(repairSchedule.getRepairUnitId()).getKeyspaceName())
          .collect(Collectors.toSet());
    }

    private Set<String> keyspacesInCluster(AppContext context, Cluster cluster) throws ReaperException {
      List<String> keyspaces = ClusterFacade.create(context).getKeyspaces(cluster);
      if (keyspaces.isEmpty()) {
        String message = format("No keyspace found in cluster %s", cluster.getName());
        LOG.debug(message);
        throw new IllegalArgumentException(message);
      }
      return Sets.newHashSet(keyspaces);
    }
  }
}
