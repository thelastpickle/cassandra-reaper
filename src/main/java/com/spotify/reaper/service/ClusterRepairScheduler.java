package com.spotify.reaper.service;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.spotify.reaper.AppContext;
import com.spotify.reaper.ReaperException;
import com.spotify.reaper.cassandra.JmxProxy;
import com.spotify.reaper.core.Cluster;
import com.spotify.reaper.core.RepairSchedule;
import com.spotify.reaper.core.RepairUnit;
import com.spotify.reaper.resources.CommonTools;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class ClusterRepairScheduler {
  private static final Logger LOG = LoggerFactory.getLogger(ClusterRepairScheduler.class);
  private static final String REPAIR_OWNER = "auto-scheduling";
  private static final List<String> SYSTEM_KEYSPACES = Arrays.asList("system", "system_auth", "system_traces");
  private final AppContext context;

  public ClusterRepairScheduler(AppContext context) {
    this.context = context;
  }

  public void scheduleRepairs(Cluster cluster) throws ReaperException {
    ScheduledRepairDiffView schedulesDiff = ScheduledRepairDiffView.compareWithExistingSchedules(context, cluster);
    schedulesDiff.keyspacesWithoutSchedules().forEach(keyspace -> createRepairSchedule(cluster, keyspace));
    schedulesDiff.keyspacesDeleted().forEach(keyspace -> deleteRepairSchedule(cluster, keyspace));
  }

  private void deleteRepairSchedule(Cluster cluster, String keyspace) {
    Collection<RepairSchedule> scheduleCollection = context.storage.getRepairSchedulesForClusterAndKeyspace(cluster.getName(), keyspace);
    scheduleCollection.forEach(repairSchedule -> {
      context.storage.deleteRepairSchedule(repairSchedule.getId());
      LOG.info("Scheduled repair deleted: {}", repairSchedule);
    });
  }

  private void createRepairSchedule(Cluster cluster, String keyspace) {
    try {
      if (SYSTEM_KEYSPACES.contains(keyspace.toLowerCase())) {
        LOG.info("Scheduled repair skipped for system keyspace {} in cluster {}.", keyspace, cluster.getName());
        return;
      }
      if (keyspaceHasNoTable(context, cluster, keyspace)) {
        LOG.warn("No tables found for keyspace {} in cluster {}. No repair will be scheduled for this keyspace.", keyspace, cluster.getName());
        return;
      }

      RepairSchedule repairSchedule = CommonTools.storeNewRepairSchedule(
          context,
          cluster,
          CommonTools.getNewOrExistingRepairUnit(context, cluster, keyspace, Collections.emptySet()),
          context.config.getScheduleDaysBetween(),
          DateTime.now().plus(context.config.getAutoScheduling().getSchedulingNextActivationPeriod().toMillis()),
          REPAIR_OWNER,
          context.config.getSegmentCount(),
          context.config.getRepairParallelism(),
          context.config.getRepairIntensity()
      );
      LOG.info("Scheduled repair created: {}", repairSchedule);
    } catch (ReaperException e) {
      Throwables.propagate(e);
    }
  }

  private boolean keyspaceHasNoTable(AppContext context, Cluster cluster, String keyspace) throws ReaperException {
    try (JmxProxy jmxProxy = context.jmxConnectionFactory.connectAny(cluster)) {
      Set<String> tables = jmxProxy.getTableNamesForKeyspace(keyspace);
      return tables.isEmpty();
    }
  }


  private static class ScheduledRepairDiffView {
    private final ImmutableSet<String> keyspacesThatRequireSchedules;
    private final ImmutableSet<String> keyspacesDeleted;

    public static ScheduledRepairDiffView compareWithExistingSchedules(AppContext context, Cluster cluster) throws ReaperException {
      return new ScheduledRepairDiffView(context, cluster);
    }

    public ScheduledRepairDiffView(AppContext context, Cluster cluster) throws ReaperException {
      Set<String> allKeyspacesInCluster = keyspacesInCluster(context, cluster);
      Set<String> keyspacesThatHaveSchedules = keyspacesThatHaveSchedules(context, cluster);
      keyspacesThatRequireSchedules = Sets.difference(allKeyspacesInCluster, keyspacesThatHaveSchedules).immutableCopy();
      keyspacesDeleted = Sets.difference(keyspacesThatHaveSchedules, allKeyspacesInCluster).immutableCopy();
    }

    public Set<String> keyspacesWithoutSchedules() {
      return keyspacesThatRequireSchedules;
    }

    public Set<String> keyspacesDeleted() {
      return keyspacesDeleted;
    }

    private Set<String> keyspacesThatHaveSchedules(AppContext context, Cluster cluster) {
      Collection<RepairSchedule> currentSchedules = context.storage.getRepairSchedulesForCluster(cluster.getName());
      return currentSchedules.stream().map(repairSchedule -> {
        Optional<RepairUnit> repairUnit = context.storage.getRepairUnit(repairSchedule.getRepairUnitId());
        return repairUnit.get().getKeyspaceName();
      }).collect(Collectors.toSet());
    }

    private Set<String> keyspacesInCluster(AppContext context, Cluster cluster) throws ReaperException {
      try (JmxProxy jmxProxy = context.jmxConnectionFactory.connectAny(cluster)) {
        List<String> keyspaces = jmxProxy.getKeyspaces();
        if (keyspaces.isEmpty()) {
          String message = format("No keyspace found in cluster %s", cluster.getName());
          LOG.debug(message);
          throw new IllegalArgumentException(message);
        }
        return Sets.newHashSet(keyspaces);
      }
    }
  }
}
