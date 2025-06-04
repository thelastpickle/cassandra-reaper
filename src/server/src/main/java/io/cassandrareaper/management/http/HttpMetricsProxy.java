/*
 * Copyright 2020-2020 The Last Pickle Ltd
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

package io.cassandrareaper.management.http;

import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.GenericMetric;
import io.cassandrareaper.core.Node;
import io.cassandrareaper.management.MetricsProxy;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.management.JMException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class HttpMetricsProxy implements MetricsProxy {

  static final String THREAD_POOL_METRICS_PREFIX = "org_apache_cassandra_metrics_thread_pools";
  static final String TPSTATS_PENDING_METRIC_NAME =
      "org_apache_cassandra_metrics_thread_pools_pending_tasks";
  static final String DROPPED_MESSAGES_METRICS_PREFIX =
      "org_apache_cassandra_metrics_dropped_message";
  static final String CLIENT_REQUEST_LATENCY_METRICS_PREFIX =
      "org_apache_cassandra_metrics_client_request_latency";
  static final String PERCENT_REPAIRED_METRICS_PREFIX =
      "org_apache_cassandra_metrics_table_percent_repaired";
  private static final Logger LOG = LoggerFactory.getLogger(HttpMetricsProxy.class);
  private final HttpCassandraManagementProxy proxy;
  private final OkHttpClient httpClient;
  private final Node node;

  private HttpMetricsProxy(
      HttpCassandraManagementProxy proxy, Node node, Supplier<OkHttpClient> httpClientSupplier) {
    this.proxy = proxy;
    this.node = node;
    this.httpClient = httpClientSupplier.get();
  }

  public static HttpMetricsProxy create(HttpCassandraManagementProxy proxy, Node node) {
    return new HttpMetricsProxy(proxy, node, () -> new OkHttpClient.Builder().build());
  }

  @VisibleForTesting
  static HttpMetricsProxy create(
      HttpCassandraManagementProxy proxy, Node node, Supplier<OkHttpClient> httpClientSupplier) {
    return new HttpMetricsProxy(proxy, node, httpClientSupplier);
  }

  @Override
  public List<GenericMetric> collectTpStats() throws JMException, IOException {
    // Collect all metrics and filter out the ones that are not thread pool metrics
    return collectMetrics(THREAD_POOL_METRICS_PREFIX, Optional.empty());
  }

  @Override
  public List<GenericMetric> collectDroppedMessages() throws JMException, IOException {
    return collectMetrics(DROPPED_MESSAGES_METRICS_PREFIX, Optional.empty());
  }

  @Override
  public List<GenericMetric> collectLatencyMetrics() throws JMException, IOException {
    return collectMetrics(CLIENT_REQUEST_LATENCY_METRICS_PREFIX, Optional.empty());
  }

  @Override
  public List<GenericMetric> collectGenericMetrics() throws JMException, IOException {
    // Not mandatory for our first iteration.
    return Collections.emptyList();
  }

  @Override
  public List<GenericMetric> collectPercentRepairedMetrics(String keyspaceName)
      throws JMException, IOException {
    return collectMetrics(PERCENT_REPAIRED_METRICS_PREFIX, Optional.of(keyspaceName));
  }

  private List<GenericMetric> collectMetrics(String metricNamePrefix, Optional<String> keyspaceName)
      throws IOException {
    HttpUrl url =
        new HttpUrl.Builder()
            .scheme("http")
            .host(proxy.getHost())
            .port(proxy.getMetricsPort())
            .addPathSegment("metrics")
            .addQueryParameter("name", metricNamePrefix)
            .build();

    LOG.debug("Collecting metrics from {}", url);
    Request request = new Request.Builder().url(url).build();
    String metrics;
    try (Response response = this.httpClient.newCall(request).execute()) {
      if (!response.isSuccessful()) {
        throw new IOException("Unexpected code " + response);
      }
      metrics = response.body().string();
      LOG.debug("Got metrics: {}", metrics);
      return parsePrometheusMetrics(metricNamePrefix, metrics, keyspaceName);
    }
  }

  public List<GenericMetric> collectTpPendingTasks() throws ReaperException {
    // Collect metrics with the tpstats pending tasks name
    List<GenericMetric> genericMetrics;
    try {
      genericMetrics = collectMetrics(TPSTATS_PENDING_METRIC_NAME, Optional.empty());
    } catch (IOException e) {
      throw new ReaperException("Error collecting metrics", e);
    }
    return genericMetrics.stream()
        .filter(metric -> metric.getMetricType().equals("ThreadPools"))
        .collect(Collectors.toList());
  }

  @VisibleForTesting
  public List<GenericMetric> parsePrometheusMetrics(
      String metricPrefix, String metrics, Optional<String> keyspaceName) {
    List<GenericMetric> parsedMetrics = Lists.newArrayList();

    // Regex Pattern for Prometheus Metrics format
    Pattern pattern = Pattern.compile("(\\w+)(\\{.*\\})?\\s(\\d+\\.\\d+)");
    Matcher matcher = pattern.matcher(metrics);

    // Pattern to extract labels
    Pattern labelPattern = Pattern.compile("(\\w+)=\"(.*?)\"");

    while (matcher.find()) {
      String metricName = matcher.group(1);
      double value = Double.parseDouble(matcher.group(3));

      // check if there are any labels and parse them
      Map<String, String> labels = Maps.newHashMap();
      String labelString = matcher.group(2);
      if (labelString != null) {
        Matcher labelMatcher = labelPattern.matcher(labelString);
        while (labelMatcher.find()) {
          labels.put(labelMatcher.group(1), labelMatcher.group(2));
        }
      }
      Optional<GenericMetric> metric =
          parseMetric(this.node, metricName, labels, value, metricPrefix, keyspaceName);
      // add the stat to the list of stats for this metric
      if (metric.isPresent()) {
        parsedMetrics.add(metric.get());
      }
    }
    return parsedMetrics;
  }

  @VisibleForTesting
  String convertToUpperCamelCase(String str) {
    if (str == null || str.isEmpty()) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    String[] parts = str.split("_");
    for (String part : parts) {
      sb.append(part.substring(0, 1).toUpperCase());
      sb.append(part.substring(1).toLowerCase());
    }
    return sb.toString();
  }

  /**
   * Parses a metric and returns a GenericMetric object if it is a thread pool metric. Returns an
   * empty optional if the metric is not a thread pool metric.
   *
   * @param metricName the name of the metric
   * @param labels the labels associated with the metric
   * @param value the value of the metric
   * @return an optional GenericMetric object
   */
  @VisibleForTesting
  public Optional<GenericMetric> parseMetric(
      Node node,
      String metricName,
      Map<String, String> labels,
      double value,
      String metricPrefix,
      Optional<String> keyspaceName) {
    Pattern pattern = getMetricParsePattern(metricPrefix);
    Matcher matcher = pattern.matcher(metricName);

    if (metricMatches(matcher, labels, keyspaceName, metricPrefix)) {
      String name = getMetricNameFromMatcher(matcher, metricPrefix);
      String type = getMetricType(metricPrefix);
      String attribute = "Count";

      GenericMetric genericMetric =
          GenericMetric.builder()
              .withClusterName(node.getClusterName())
              .withHost(node.getHostname())
              .withMetricDomain("org.apache.cassandra.metrics")
              .withMetricType(type)
              .withMetricScope(labels.get(getMetricScope(metricPrefix)))
              .withMetricName(name)
              .withMetricAttribute(attribute)
              .withValue(value)
              .withTs(DateTime.now())
              .build();

      return Optional.of(genericMetric);
    }

    return Optional.empty();
  }

  private String getMetricType(String metricPrefix) {
    switch (metricPrefix) {
      case TPSTATS_PENDING_METRIC_NAME:
      case THREAD_POOL_METRICS_PREFIX:
        return "ThreadPools";
      case DROPPED_MESSAGES_METRICS_PREFIX:
        return "DroppedMessage";
      case CLIENT_REQUEST_LATENCY_METRICS_PREFIX:
        return "ClientRequest";
      case PERCENT_REPAIRED_METRICS_PREFIX:
        return "ColumnFamily";
      default:
        throw new IllegalArgumentException("Unsupported metric prefix: " + metricPrefix);
    }
  }

  private String getMetricScope(String metricPrefix) {
    switch (metricPrefix) {
      case TPSTATS_PENDING_METRIC_NAME:
      case THREAD_POOL_METRICS_PREFIX:
        return "pool_name";
      case DROPPED_MESSAGES_METRICS_PREFIX:
        return "message_type";
      case CLIENT_REQUEST_LATENCY_METRICS_PREFIX:
        return "request_type";
      case PERCENT_REPAIRED_METRICS_PREFIX:
        return "table";
      default:
        throw new IllegalArgumentException("Unsupported metric prefix: " + metricPrefix);
    }
  }

  private Pattern getMetricParsePattern(String metricPrefix) {
    if (PERCENT_REPAIRED_METRICS_PREFIX.equals(metricPrefix)
        || TPSTATS_PENDING_METRIC_NAME.equals(metricPrefix)) {
      return Pattern.compile(metricPrefix);
    }
    return Pattern.compile(metricPrefix + "_(.*)$");
  }

  private String getMetricNameFromMatcher(Matcher matcher, String metricPrefix) {
    if (PERCENT_REPAIRED_METRICS_PREFIX.equals(metricPrefix)) {
      return "PercentRepaired";
    }
    if (TPSTATS_PENDING_METRIC_NAME.equals(metricPrefix)) {
      return "PendingTasks";
    }
    return convertToUpperCamelCase(matcher.group(1));
  }

  private boolean metricMatches(
      Matcher matcher,
      Map<String, String> labels,
      Optional<String> keyspaceName,
      String metricPrefix) {
    if (matcher.find()) {
      if (PERCENT_REPAIRED_METRICS_PREFIX.equals(metricPrefix)) {
        return (keyspaceName.isPresent()) && (keyspaceName.get().equals(labels.get("keyspace")));
      }
      if (TPSTATS_PENDING_METRIC_NAME.equals(metricPrefix)) {
        return "CompactionExecutor".equals(labels.get("pool_name"));
      }
      return true;
    }
    // no match
    return false;
  }
}
