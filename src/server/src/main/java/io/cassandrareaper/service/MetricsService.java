/*
 * Copyright 2018-2018 The Last Pickle Ltd
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
import io.cassandrareaper.core.Compaction;
import io.cassandrareaper.core.DroppedMessages;
import io.cassandrareaper.core.GenericMetric;
import io.cassandrareaper.core.JmxStat;
import io.cassandrareaper.core.MetricsHistogram;
import io.cassandrareaper.core.Node;
import io.cassandrareaper.core.StreamSession;
import io.cassandrareaper.core.ThreadPoolStat;
import io.cassandrareaper.jmx.ClusterFacade;
import io.cassandrareaper.storage.IDistributedStorage;
import io.cassandrareaper.storage.OpType;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.management.JMException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class MetricsService {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsService.class);

  private static final String[] COLLECTED_METRICS = {
    "org.apache.cassandra.metrics:type=ThreadPools,path=request,*",
    "org.apache.cassandra.metrics:type=ThreadPools,path=internal,*",
    "org.apache.cassandra.metrics:type=ClientRequest,*",
    "org.apache.cassandra.metrics:type=DroppedMessage,*"
  };

  private final AppContext context;
  private final ClusterFacade clusterFacade;
  private final ObjectMapper objectMapper = new ObjectMapper();
  private final String localClusterName;

  private MetricsService(AppContext context, Supplier<ClusterFacade> clusterFacadeSupplier) throws ReaperException {
    this.context = context;
    this.clusterFacade = clusterFacadeSupplier.get();
    if (context.config.isInSidecarMode()) {

      Node host = Node.builder()
            .withHostname(context.config.getEnforcedLocalNode().orElse("127.0.0.1"))
            .build();

      localClusterName = Cluster.toSymbolicName(clusterFacade.getClusterName(host));
    } else {
      localClusterName = null;
    }
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

  public List<GenericMetric> convertToGenericMetrics(Map<String, List<JmxStat>> jmxStats, Node node) {
    List<GenericMetric> metrics = Lists.newArrayList();
    DateTime now = DateTime.now();
    for (Entry<String, List<JmxStat>> jmxStatEntry:jmxStats.entrySet()) {
      for (JmxStat jmxStat:jmxStatEntry.getValue()) {
        GenericMetric metric = GenericMetric.builder()
            .withClusterName(node.getClusterName())
            .withHost(node.getHostname())
            .withMetricDomain(jmxStat.getDomain())
            .withMetricType(jmxStat.getType())
            .withMetricScope(jmxStat.getScope())
            .withMetricName(jmxStat.getName())
            .withMetricAttribute(jmxStat.getAttribute())
            .withValue(jmxStat.getValue())
            .withTs(now)
            .build();

        metrics.add(metric);
      }
    }
    return metrics;
  }

  void grabAndStoreGenericMetrics() throws ReaperException, InterruptedException, JMException {
    Preconditions.checkState(
        context.config.isInSidecarMode(),
        "grabAndStoreGenericMetrics() can only be called in sidecar");

    Node node = Node.builder().withHostname(context.getLocalNodeAddress()).build();

    List<GenericMetric> metrics
        = convertToGenericMetrics(ClusterFacade.create(context).collectMetrics(node, COLLECTED_METRICS), node);

    for (GenericMetric metric:metrics) {
      ((IDistributedStorage)context.storage).storeMetric(metric);
    }
    LOG.debug("Grabbing and storing metrics for {}", context.getLocalNodeAddress());

  }

  void grabAndStoreActiveCompactions() throws JsonProcessingException, JMException, ReaperException {
    Preconditions.checkState(
        context.config.isInSidecarMode(),
        "grabAndStoreActiveCompactions() can only be called in sidecar");

    Node node = Node.builder().withHostname(context.getLocalNodeAddress()).build();
    List<Compaction> activeCompactions = ClusterFacade.create(context).listActiveCompactionsDirect(node);

    ((IDistributedStorage) context.storage)
        .storeOperations(
            localClusterName,
            OpType.OP_COMPACTION,
            context.getLocalNodeAddress(),
            objectMapper.writeValueAsString(activeCompactions));

    LOG.debug("Grabbing and storing compactions for {}", context.getLocalNodeAddress());
  }

  void grabAndStoreActiveStreams() throws JsonProcessingException, ReaperException {
    Preconditions.checkState(
        context.config.isInSidecarMode(),
        "grabAndStoreActiveStreams() can only be called in sidecar");

    Node node = Node.builder().withHostname(context.getLocalNodeAddress()).build();
    List<StreamSession> activeStreams = ClusterFacade.create(context).listStreamsDirect(node);

    ((IDistributedStorage) context.storage)
        .storeOperations(
            localClusterName,
            OpType.OP_STREAMING,context.getLocalNodeAddress(),
            objectMapper.writeValueAsString(activeStreams));

    LOG.debug("Grabbing and storing streams for {}", context.getLocalNodeAddress());
  }

}
