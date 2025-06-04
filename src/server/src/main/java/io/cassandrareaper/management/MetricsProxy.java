/*
 * Copyright 2023-2023 DataStax, Inc.
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

package io.cassandrareaper.management;

import com.google.common.collect.Lists;
import io.cassandrareaper.core.DroppedMessages;
import io.cassandrareaper.core.GenericMetric;
import io.cassandrareaper.core.JmxStat;
import io.cassandrareaper.core.MetricsHistogram;
import io.cassandrareaper.core.Node;
import io.cassandrareaper.core.ThreadPoolStat;
import io.cassandrareaper.management.http.HttpCassandraManagementProxy;
import io.cassandrareaper.management.http.HttpMetricsProxy;
import io.cassandrareaper.management.jmx.JmxCassandraManagementProxy;
import io.cassandrareaper.management.jmx.JmxMetricsProxy;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import javax.management.JMException;
import org.joda.time.DateTime;

public interface MetricsProxy {

  static MetricsProxy create(ICassandraManagementProxy proxy, Node node) {
    if (proxy == null) {
      throw new RuntimeException("ICassandraManagementProxy is null");
    }
    if (proxy instanceof JmxCassandraManagementProxy) {
      return JmxMetricsProxy.create((JmxCassandraManagementProxy) proxy, node);
    } else if (proxy instanceof HttpCassandraManagementProxy) {
      return HttpMetricsProxy.create((HttpCassandraManagementProxy) proxy, node);
    }
    throw new UnsupportedOperationException("Unknown ICassandraManagementProxy implementation");
  }

  List<GenericMetric> collectTpStats() throws JMException, IOException;

  List<GenericMetric> collectDroppedMessages() throws JMException, IOException;

  List<GenericMetric> collectLatencyMetrics() throws JMException, IOException;

  List<GenericMetric> collectGenericMetrics() throws JMException, IOException;

  List<GenericMetric> collectPercentRepairedMetrics(String keyspaceName)
      throws JMException, IOException;

  static List<GenericMetric> convertToGenericMetrics(
      Map<String, List<JmxStat>> jmxStats, Node node) {
    List<GenericMetric> metrics = Lists.newArrayList();
    DateTime now = DateTime.now();
    for (Entry<String, List<JmxStat>> jmxStatEntry : jmxStats.entrySet()) {
      for (JmxStat jmxStat : jmxStatEntry.getValue()) {
        GenericMetric metric =
            GenericMetric.builder()
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

  static ThreadPoolStat.Builder updateGenericMetricAttribute(
      GenericMetric stat, ThreadPoolStat.Builder builder) {
    switch (stat.getMetricName()) {
      case "MaxPoolSize":
        return builder.withMaxPoolSize((int) stat.getValue());
      case "TotalBlockedTasks":
        return builder.withTotalBlockedTasks((int) stat.getValue());
      case "PendingTasks":
        return builder.withPendingTasks((int) stat.getValue());
      case "CurrentlyBlockedTasks":
        return builder.withCurrentlyBlockedTasks((int) stat.getValue());
      case "CompletedTasks":
        return builder.withCompletedTasks((int) stat.getValue());
      case "ActiveTasks":
        return builder.withActiveTasks((int) stat.getValue());
      default:
        return builder;
    }
  }

  static DroppedMessages.Builder updateGenericMetricAttribute(
      GenericMetric stat, DroppedMessages.Builder builder) {
    switch (stat.getMetricAttribute()) {
      case "Count":
        return builder.withCount((int) stat.getValue());
      case "OneMinuteRate":
        return builder.withOneMinuteRate(stat.getValue());
      case "FiveMinuteRate":
        return builder.withFiveMinuteRate(stat.getValue());
      case "FifteenMinuteRate":
        return builder.withFifteenMinuteRate(stat.getValue());
      case "MeanRate":
        return builder.withMeanRate(stat.getValue());
      default:
        return builder;
    }
  }

  static MetricsHistogram.Builder updateGenericMetricAttribute(
      GenericMetric stat, MetricsHistogram.Builder builder) {
    switch (stat.getMetricAttribute()) {
      case "Count":
        return builder.withCount((int) stat.getValue());
      case "OneMinuteRate":
        return builder.withOneMinuteRate(stat.getValue());
      case "FiveMinuteRate":
        return builder.withFiveMinuteRate(stat.getValue());
      case "FifteenMinuteRate":
        return builder.withFifteenMinuteRate(stat.getValue());
      case "MeanRate":
        return builder.withMeanRate(stat.getValue());
      case "StdDev":
        return builder.withStdDev(stat.getValue());
      case "Min":
        return builder.withMin(stat.getValue());
      case "Max":
        return builder.withMax(stat.getValue());
      case "Mean":
        return builder.withMean(stat.getValue());
      case "50thPercentile":
        return builder.withP50(stat.getValue());
      case "75thPercentile":
        return builder.withP75(stat.getValue());
      case "95thPercentile":
        return builder.withP95(stat.getValue());
      case "98thPercentile":
        return builder.withP98(stat.getValue());
      case "99thPercentile":
        return builder.withP99(stat.getValue());
      case "999thPercentile":
        return builder.withP999(stat.getValue());
      default:
        return builder;
    }
  }
}
