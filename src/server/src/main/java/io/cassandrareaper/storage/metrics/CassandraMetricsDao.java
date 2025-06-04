/*
 * Copyright 2016-2017 Spotify AB
 * Copyright 2016-2019 The Last Pickle Ltd
 * Copyright 2020-2020 DataStax, Inc.
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

package io.cassandrareaper.storage.metrics;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.common.collect.Lists;
import io.cassandrareaper.core.GenericMetric;
import io.cassandrareaper.core.PercentRepairedMetric;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class CassandraMetricsDao implements IMetricsDao, IDistributedMetrics {

  static final int METRICS_PARTITIONING_TIME_MINS = 10;
  private static final DateTimeFormatter TIME_BUCKET_FORMATTER =
      DateTimeFormat.forPattern("yyyyMMddHHmm");
  private final CqlSession session;
  private PreparedStatement getMetricsForHostPrepStmt;
  private PreparedStatement storeMetricsPrepStmt;
  private PreparedStatement storePercentRepairedForSchedulePrepStmt;
  private PreparedStatement getPercentRepairedForSchedulePrepStmt;

  public CassandraMetricsDao(CqlSession session) {

    this.session = session;
    prepareMetricStatements();
  }

  @SuppressWarnings("checkstyle:lineLength")
  void prepareMetricStatements() {
    storeMetricsPrepStmt =
        session.prepare(
            "INSERT INTO node_metrics_v3 (cluster, metric_domain, metric_type, time_bucket, "
                + "host, metric_scope, metric_name, ts, metric_attribute, value) "
                + "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
    getMetricsForHostPrepStmt =
        session.prepare(
            "SELECT cluster, metric_domain, metric_type, time_bucket, host, "
                + "metric_scope, metric_name, ts, metric_attribute, value "
                + "FROM node_metrics_v3 "
                + "WHERE metric_domain = ? and metric_type = ? and cluster = ? and time_bucket = ? and host = ?");

    storePercentRepairedForSchedulePrepStmt =
        session.prepare(
            "INSERT INTO percent_repaired_by_schedule"
                + " (cluster_name, repair_schedule_id, time_bucket, node, keyspace_name,"
                + " table_name, percent_repaired, ts)"
                + " values(?, ?, ?, ?, ?, ?, ?, ?)");

    getPercentRepairedForSchedulePrepStmt =
        session.prepare(
            "SELECT * FROM percent_repaired_by_schedule"
                + " WHERE cluster_name = ? and repair_schedule_id = ? AND time_bucket = ?");
  }

  @Override
  public List<GenericMetric> getMetrics(
      String clusterName,
      Optional<String> host,
      String metricDomain,
      String metricType,
      long since) {
    List<CompletionStage<AsyncResultSet>> futures = Lists.newArrayList();
    List<String> timeBuckets = Lists.newArrayList();
    long now = DateTime.now().getMillis();
    long startTime = since;

    // Compute the hourly buckets since the requested lower bound timestamp
    while (startTime < now) {
      timeBuckets.add(
          DateTime.now().withMillis(startTime).toString(TIME_BUCKET_FORMATTER).substring(0, 11)
              + "0");
      startTime += 600000;
    }

    for (String timeBucket : timeBuckets) {
      if (host.isPresent()) {
        // metric = ? and cluster = ? and time_bucket = ? and host = ? and ts >= ? and ts <= ?
        futures.add(
            session.executeAsync(
                getMetricsForHostPrepStmt.bind(
                    metricDomain, metricType, clusterName, timeBucket, host.get())));
      }
    }

    List<GenericMetric> metrics = Lists.newArrayList();
    for (CompletionStage<AsyncResultSet> future : futures) {
      AsyncResultSet results = future.toCompletableFuture().join();
      while (true) {
        for (Row row : results.currentPage()) {
          metrics.add(
              GenericMetric.builder()
                  .withClusterName(row.getString("cluster"))
                  .withHost(row.getString("host"))
                  .withMetricType(row.getString("metric_type"))
                  .withMetricScope(row.getString("metric_scope"))
                  .withMetricName(row.getString("metric_name"))
                  .withMetricAttribute(row.getString("metric_attribute"))
                  .withTs(new DateTime(row.getInstant("ts").toEpochMilli()))
                  .withValue(row.getDouble("value"))
                  .build());
        }
        if (!results.hasMorePages()) {
          break;
        }
        results = results.fetchNextPage().toCompletableFuture().join();
      }
      if (!metrics.isEmpty()) {
        break;
      }
    }
    return metrics;
  }

  @Override
  public void storeMetrics(List<GenericMetric> metrics) {
    Map<String, List<GenericMetric>> metricsPerPartition =
        metrics.stream()
            .collect(
                Collectors.groupingBy(
                    metric ->
                        metric.getClusterName()
                            + metric.getMetricDomain()
                            + metric.getMetricType()
                            + computeMetricsPartition(metric.getTs())
                                .toString(TIME_BUCKET_FORMATTER)
                            + metric.getHost()));

    for (Map.Entry<String, List<GenericMetric>> metricPartition : metricsPerPartition.entrySet()) {
      BatchStatement batch = BatchStatement.newInstance(BatchType.UNLOGGED);
      for (GenericMetric metric : metricPartition.getValue()) {
        batch.add(
            storeMetricsPrepStmt.bind(
                metric.getClusterName(),
                metric.getMetricDomain(),
                metric.getMetricType(),
                computeMetricsPartition(metric.getTs()).toString(TIME_BUCKET_FORMATTER),
                metric.getHost(),
                metric.getMetricScope(),
                metric.getMetricName(),
                Instant.ofEpochMilli(computeMetricsPartition(metric.getTs()).getMillis()),
                metric.getMetricAttribute(),
                metric.getValue()));
      }
      session.execute(batch);
    }
  }

  /**
   * Truncates a metric date time to the closest partition based on the definesd partition sizes
   *
   * @param metricTime the time of the metric
   * @return the time truncated to the closest partition
   */
  public DateTime computeMetricsPartition(DateTime metricTime) {
    return metricTime
        .withMinuteOfHour(
            (metricTime.getMinuteOfHour() / METRICS_PARTITIONING_TIME_MINS)
                * METRICS_PARTITIONING_TIME_MINS)
        .withSecondOfMinute(0)
        .withMillisOfSecond(0);
  }

  public void purgeMetrics() {}

  @Override
  public List<PercentRepairedMetric> getPercentRepairedMetrics(
      String clusterName, UUID repairScheduleId, Long since) {
    List<String> timeBuckets = Lists.newArrayList();
    long now = DateTime.now().getMillis();
    long startTime = since;

    // Compute the ten minutes buckets since the requested lower bound timestamp
    while (startTime <= now) {
      timeBuckets.add(
          DateTime.now().withMillis(startTime).toString(TIME_BUCKET_FORMATTER).substring(0, 11)
              + "0");
      startTime += 600000;
    }

    Collections.reverse(timeBuckets);

    List<CompletionStage<AsyncResultSet>> futures = Lists.newArrayList();
    for (String timeBucket : timeBuckets) {
      futures.add(
          session.executeAsync(
              getPercentRepairedForSchedulePrepStmt.bind(
                  clusterName, repairScheduleId, timeBucket)));
    }

    List<PercentRepairedMetric> metrics = Lists.newArrayList();
    long maxTimeBucket = 0;
    for (CompletionStage<AsyncResultSet> future : futures) {
      AsyncResultSet results = future.toCompletableFuture().join();
      while (true) {
        for (Row row : results.currentPage()) {
          if (Long.parseLong(row.getString("time_bucket")) >= maxTimeBucket) {
            // we only want metrics from the latest bucket
            metrics.add(
                PercentRepairedMetric.builder()
                    .withCluster(clusterName)
                    .withRepairScheduleId(row.getUuid("repair_schedule_id"))
                    .withKeyspaceName(row.getString("keyspace_name"))
                    .withTableName(row.getString("table_name"))
                    .withNode(row.getString("node"))
                    .withPercentRepaired(row.getInt("percent_repaired"))
                    .build());
            maxTimeBucket = Math.max(maxTimeBucket, Long.parseLong(row.getString("time_bucket")));
          }
        }
        if (!results.hasMorePages()) {
          break;
        }
        results = results.fetchNextPage().toCompletableFuture().join();
      }
      if (!metrics.isEmpty()) {
        break;
      }
    }

    return metrics;
  }

  @Override
  public void storePercentRepairedMetric(PercentRepairedMetric metric) {
    session.execute(
        storePercentRepairedForSchedulePrepStmt.bind(
            metric.getCluster(),
            metric.getRepairScheduleId(),
            DateTime.now().toString(TIME_BUCKET_FORMATTER).substring(0, 11) + "0",
            metric.getNode(),
            metric.getKeyspaceName(),
            metric.getTableName(),
            metric.getPercentRepaired(),
            Instant.now()));
  }
}
