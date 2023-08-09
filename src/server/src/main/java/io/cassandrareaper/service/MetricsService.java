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
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.CompactionStats;
import io.cassandrareaper.core.DroppedMessages;
import io.cassandrareaper.core.GenericMetric;
import io.cassandrareaper.core.MetricsHistogram;
import io.cassandrareaper.core.Node;
import io.cassandrareaper.core.PercentRepairedMetric;
import io.cassandrareaper.core.RepairSchedule;
import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.core.StreamSession;
import io.cassandrareaper.core.ThreadPoolStat;
import io.cassandrareaper.management.ClusterFacade;
import io.cassandrareaper.storage.IDistributedStorage;
import io.cassandrareaper.storage.OpType;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.management.JMException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class MetricsService {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsService.class);

  private final AppContext context;
  private final ClusterFacade clusterFacade;
  private final ObjectMapper objectMapper;
  private final String localClusterName;
  private final RepairUnitService repairUnitService;

  private MetricsService(AppContext context, Supplier<ClusterFacade> clusterFacadeSupplier) throws ReaperException {
    this.context = context;
    this.clusterFacade = clusterFacadeSupplier.get();
    if (Boolean.TRUE.equals(context.config.isInSidecarMode())) {

      Node host = Node.builder()
          .withHostname(context.config.getEnforcedLocalNode().orElse("127.0.0.1"))
          .build();

      localClusterName = Cluster.toSymbolicName(clusterFacade.getClusterName(host));
    } else {
      localClusterName = null;
    }
    objectMapper = new ObjectMapper();
    objectMapper.registerModule(new Jdk8Module());
    this.repairUnitService = RepairUnitService.create(context);
  }

  @VisibleForTesting
  static MetricsService create(AppContext context, Supplier<ClusterFacade> supplier) throws ReaperException {
    return new MetricsService(context, supplier);
  }

  public static MetricsService create(AppContext context) throws ReaperException {
    return new MetricsService(context, () -> ClusterFacade.create(context));
  }

  public List<ThreadPoolStat> getTpStats(Node host) throws ReaperException {
    return clusterFacade.getTpStats(host);
  }

  public List<DroppedMessages> getDroppedMessages(Node host) throws ReaperException {
    return clusterFacade.getDroppedMessages(host);
  }

  public List<MetricsHistogram> getClientRequestLatencies(Node host) throws ReaperException {
    return clusterFacade.getClientRequestLatencies(host);
  }

  void grabAndStoreGenericMetrics(Optional<Node> maybeNode) throws ReaperException, InterruptedException, JMException {
    Preconditions.checkState(
        context.config.getDatacenterAvailability().isInCollocatedMode(),
        "grabAndStoreGenericMetrics() can only be called in collocated mode");

    Node node = getNode(maybeNode);

    List<GenericMetric> metrics = ClusterFacade.create(context).collectGenericMetrics(node);

    ((IDistributedStorage) context.storage).storeMetrics(metrics);

    LOG.debug("Grabbing and storing metrics for {}", node.getHostname());

  }

  void grabAndStoreCompactionStats(Optional<Node> maybeNode)
      throws JsonProcessingException, JMException, ReaperException {
    Preconditions.checkState(
        context.config.getDatacenterAvailability().isInCollocatedMode(),
        "grabAndStoreCompactionStats() can only be called in sidecar");

    Node node = getNode(maybeNode);

    CompactionStats compactionStats = ClusterFacade.create(context).listCompactionStatsDirect(node);

    ((IDistributedStorage) context.storage).getOperationsDao()
        .storeOperations(
            node.getClusterName(),
            OpType.OP_COMPACTION,
            node.getHostname(),
            objectMapper.writeValueAsString(compactionStats));

    LOG.debug("Grabbing and storing compaction stats for {}", node.getHostname());
  }

  void grabAndStoreActiveStreams(Optional<Node> maybeNode) throws JsonProcessingException, ReaperException {
    Preconditions.checkState(
        context.config.getDatacenterAvailability().isInCollocatedMode(),
        "grabAndStoreActiveStreams() can only be called in sidecar");

    Node node = getNode(maybeNode);

    List<StreamSession> activeStreams = ClusterFacade.create(context).listStreamsDirect(node);

    ((IDistributedStorage) context.storage).getOperationsDao()
        .storeOperations(
            node.getClusterName(),
            OpType.OP_STREAMING,
            node.getHostname(),
            objectMapper.writeValueAsString(activeStreams));

    LOG.debug("Grabbing and storing streams for {}", node.getHostname());
  }

  private Node getNode(Optional<Node> maybeNode) {
    return maybeNode.orElseGet(() ->
        Node.builder()
            .withHostname(context.getLocalNodeAddress())
            .withCluster(
                Cluster.builder()
                    .withName(localClusterName)
                    .withSeedHosts(ImmutableSet.of(context.getLocalNodeAddress()))
                    .build())
            .build());
  }

  public void grabAndStorePercentRepairedMetrics(Optional<Node> maybeNode, RepairSchedule sched)
      throws ReaperException {
    Node node = getNode(maybeNode);
    RepairUnit repairUnit = context.storage.getRepairUnitDao().getRepairUnit(sched.getRepairUnitId());
    Set<String> tables = this.repairUnitService.getTablesToRepair(node.getCluster().get(), repairUnit);

    // Collect percent repaired metrics for all tables in the keyspace
    List<GenericMetric> metrics = ClusterFacade.create(context).collectPercentRepairedMetrics(node,
            repairUnit.getKeyspaceName());
    LOG.debug("Grabbed the following percent repaired metrics: {}", metrics);

    // Metrics are filtered to retain only tables of interest, sorted and reduced to keep the smallest percent repaired
    Optional<GenericMetric> percentRepairedForSchedule = metrics.stream()
        .filter(metric -> tables.contains(metric.getMetricScope()))
        .sorted((metric1, metric2) -> Double.valueOf(metric1.getValue()).compareTo(Double.valueOf(metric2.getValue())))
        .reduce((metric1, metric2) -> metric1);
    if (percentRepairedForSchedule.isPresent()) {
      context.storage.storePercentRepairedMetric(
          PercentRepairedMetric.builder()
              .withCluster(repairUnit.getClusterName())
              .withKeyspaceName(repairUnit.getKeyspaceName())
              .withTableName(percentRepairedForSchedule.get().getMetricScope())
              .withNode(node.getHostname())
              .withRepairScheduleId(sched.getId())
              .withPercentRepaired((int) percentRepairedForSchedule.get().getValue())
              .build());
    }
  }
}