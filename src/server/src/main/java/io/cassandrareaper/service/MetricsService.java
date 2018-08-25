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
import io.cassandrareaper.core.DroppedMessages;
import io.cassandrareaper.core.GenericMetric;
import io.cassandrareaper.core.JmxStat;
import io.cassandrareaper.core.MetricsHistogram;
import io.cassandrareaper.core.Node;
import io.cassandrareaper.core.ThreadPoolStat;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.Lists;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class MetricsService {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsService.class);
  private static final String[] COLLECTED_METRICS
    = {"org.apache.cassandra.metrics:type=ThreadPools,path=request,*",
       "org.apache.cassandra.metrics:type=ThreadPools,path=internal,*",
       "org.apache.cassandra.metrics:type=ClientRequest,*",
       "org.apache.cassandra.metrics:type=DroppedMessage,*"};

  private final AppContext context;

  private MetricsService(AppContext context) {
    this.context = context;
  }

  public static MetricsService create(AppContext context) {
    return new MetricsService(context);
  }

  public List<ThreadPoolStat> getTpStats(Node host) throws ReaperException {
    return context.clusterProxy.getTpStats(host);
  }

  public List<DroppedMessages> getDroppedMessages(Node host) throws ReaperException {
    return context.clusterProxy.getDroppedMessages(host);
  }

  public List<MetricsHistogram> getClientRequestLatencies(Node host) throws ReaperException {
    return context.clusterProxy.getClientRequestLatencies(host);
  }

  public Map<String, List<JmxStat>> collectMetrics(Node node) throws ReaperException {
    return context.clusterProxy.collectMetrics(node, COLLECTED_METRICS);
  }

  public List<GenericMetric> convertToGenericMetrics(Map<String, List<JmxStat>> jmxStats, Node node) {
    List<GenericMetric> metrics = Lists.newArrayList();
    DateTime now = DateTime.now();
    for (Entry<String, List<JmxStat>> jmxStatEntry:jmxStats.entrySet()) {
      for (JmxStat jmxStat:jmxStatEntry.getValue()) {
        GenericMetric metric = GenericMetric.builder()
            .withClusterName(node.getCluster().getName())
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
}
