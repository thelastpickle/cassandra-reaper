/*
 * Copyright 2017-2017 Spotify AB
 * Copyright 2017-2019 The Last Pickle Ltd
 * Copyright 2021-2021 DataStax, Inc.
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
import io.cassandrareaper.ReaperApplicationConfiguration.DatacenterAvailability;
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.Node;
import io.cassandrareaper.core.RepairSchedule;
import io.cassandrareaper.jmx.ClusterFacade;
import io.cassandrareaper.storage.IDistributedStorage;

import java.io.IOException;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.JMException;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Heart implements AutoCloseable {

  private static final AtomicBoolean GAUGES_REGISTERED = new AtomicBoolean(false);
  private static final Logger LOG = LoggerFactory.getLogger(Heart.class);
  private static final long DEFAULT_MAX_FREQUENCY = TimeUnit.SECONDS.toMillis(60);

  private final AtomicLong lastBeat = new AtomicLong(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1));
  private final AtomicLong lastMetricBeat = new AtomicLong(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1));
  private final AtomicLong lastPercentRepairedBeat = new AtomicLong(System.currentTimeMillis() - TimeUnit.HOURS
      .toMillis(1));
  private final ForkJoinPool forkJoinPool = new ForkJoinPool(64);
  private final AppContext context;
  private final MetricsService metricsService;
  private final long maxBeatFrequencyMillis;
  private final AtomicBoolean updatingNodeMetrics = new AtomicBoolean(false);

  private Heart(AppContext context, long maxBeatFrequency) throws ReaperException {
    this.context = context;
    this.maxBeatFrequencyMillis = maxBeatFrequency;
    this.metricsService = MetricsService.create(context);
  }

  public static Heart create(AppContext context) throws ReaperException {
    return new Heart(context, DEFAULT_MAX_FREQUENCY);
  }

  @VisibleForTesting
  static Heart create(AppContext context, long maxBeatFrequencyMillis) throws ReaperException {
    return new Heart(context, maxBeatFrequencyMillis);
  }

  /**
   * Performs the following operations as part of regular heartbeats.
   * - Store heart beat with timestamp in the running_reapers table (allows to count live reaper instances)
   * - In distributed modes, stores metrics in the backend such as tpstats, latencies and pending compactions
   *   as all instances can't reach all nodes through JMX (all dcAvailability modes but ALL).
   *   This is done for all nodes in all clusters that are managed by the Reaper instance.
   * - In sidecar mode, does the same as above but only for the local node
   * - In all cases, stores min(%repaired) for all tables that are part of an incremental repair schedule
   *
   */
  public synchronized void beat() {
    if ( context.storage instanceof IDistributedStorage ) {
      if (lastBeat.get() + maxBeatFrequencyMillis < System.currentTimeMillis()) {
        lastBeat.set(System.currentTimeMillis());
        ((IDistributedStorage) context.storage).saveHeartbeat();

        if (!context.isDistributed.get() && 1 < ((IDistributedStorage) context.storage).countRunningReapers()) {
          context.isDistributed.set(true);
        }
      }
    }

    if (isDistributedAndCollocated()) {
      // Metrics are always collected in distributed mode (tpstats, latencies, compactions, streaming, %repaired, ...)
      updateMetricsForCollocatedModes();
    } else {
      // In standalone/non collocated Reaper mode, only percent repaired metrics
      // are collected for incremental repair schedules
      if (!updatingNodeMetrics.getAndSet(true)) {
        forkJoinPool.submit(() -> {
          try (Timer.Context t0 = timer(context, "updatingNodeMetrics")) {
            ClusterFacade clusterFacade = ClusterFacade.create(context);
            Collection<Cluster> clusters = context.storage.getClusters();
            updateMetricsForClusters(clusterFacade, clusters);
          } finally {
            assert updatingNodeMetrics.get();
            updatingNodeMetrics.set(false);
          }
        });
      }
    }
  }

  AtomicBoolean isCurrentlyUpdatingNodeMetrics() {
    return new AtomicBoolean(updatingNodeMetrics.get());
  }

  @Override
  public void close() {
    try {
      forkJoinPool.shutdown();
      forkJoinPool.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException ignore) {
    } finally {
      forkJoinPool.shutdownNow();
    }
  }

  /**
   * Update metrics in the storage backend for all collocated modes
   */
  private void updateMetricsForCollocatedModes() {
    Preconditions.checkState(context.isDistributed.get(), "Only valid with multiple Reaper instances");

    Preconditions.checkState(
        context.config.getDatacenterAvailability().isInCollocatedMode(),
        "metrics are fetched directly in ALL mode");

    registerGauges();

    if (!updatingNodeMetrics.getAndSet(true)) {
      forkJoinPool.submit(() -> {
        try (Timer.Context t0 = timer(context, "updatingNodeMetrics")) {
          if (context.config.getDatacenterAvailability() != DatacenterAvailability.SIDECAR) {
            // In distributed modes other than SIDECAR, metrics are grabbed for all accessible nodes
            // in all managed clusters
            ClusterFacade clusterFacade = ClusterFacade.create(context);
            Collection<Cluster> clusters = context.storage.getClusters();
            forkJoinPool.submit(() -> {
              updateMetricsForClusters(clusterFacade, clusters);
            }).get();
          } else {
            // In SIDECAR mode we grab metrics for the local node only
            if (canPerformBeat(lastMetricBeat, maxBeatFrequencyMillis)) {
              metricsService.grabAndStoreGenericMetrics(Optional.empty());
              lastMetricBeat.set(System.currentTimeMillis());
            } else {
              LOG.trace("Not storing metrics yet... Last beat was {} and now is {}",
                  lastMetricBeat.get(),
                  System.currentTimeMillis());
            }
            updatePercentRepairedForNode(Optional.empty(), context.storage.getClusters().stream().findFirst().get());
            metricsService.grabAndStoreCompactionStats(Optional.empty());
            metricsService.grabAndStoreActiveStreams(Optional.empty());
          }
        } catch (ExecutionException | InterruptedException | RuntimeException
            | ReaperException | JMException | IOException ex) {
          LOG.warn("Failed metric collection during heartbeat", ex);
        } finally {
          assert updatingNodeMetrics.get();
          updatingNodeMetrics.set(false);
        }
      });
    }
  }

  /**
   * Updates the metrics in all modes for a given list of clusters, including the non collocated ones.
   * For non collocated modes, only percent repaired metrics will be extracted for existing incr repair schedules.
   *
   * @param clusterFacade A ClusterFacade object used to access the nodes via JMX.
   * @param clusters Active clusters managed by Reaper
   */
  private void updateMetricsForClusters(
      ClusterFacade clusterFacade,
      Collection<Cluster> clusters
  ) {
    clusters
        .parallelStream()
        .filter(cluster -> cluster.getState() == Cluster.State.ACTIVE)
        .forEach(cluster -> {
          try {
            clusterFacade.getLiveNodes(cluster)
                .parallelStream()
                .filter(hostname -> {
                  return context.jmxConnectionFactory
                      .getHostConnectionCounters()
                      .getSuccessfulConnections(hostname) >= 0;
                })
                .map(hostname -> Node.builder()
                    .withHostname(hostname)
                    .withCluster(cluster)
                    .build())
                .forEach(node -> {
                  try {
                    if (isDistributedAndCollocated()) {
                      // All metrics but percent repaired should be extracted only in distributed/collocated modes
                      updateMetricsForNode(node);
                    }
                    updatePercentRepairedForNode(Optional.of(node), cluster);
                  } catch (JMException | ReaperException | RuntimeException | IOException e) {
                    LOG.error("Couldn't extract metrics for node {} in cluster {}",
                        node.getHostname(), cluster.getName(), e);
                  } catch (InterruptedException e) {
                    LOG.error("Interrupted while extracting metrics for node {} in cluster {}",
                        node.getHostname(), cluster.getName(), e);
                  }
                });
          } catch (ReaperException e) {
            LOG.error("Couldn't list live nodes in cluster {}", cluster.getName(), e);
            e.printStackTrace();
          }
        });
  }

  /**
   * Retrieve percent repaired metrics for a specific node.
   * Such metrics will only be retrieved if an incremental repair schedule exists.
   *
   * @param node An optional node to grab the metrics from (the local node if not provided)
   * @param incrementalRepairSchedules a collection of incremental repair schedules
   */
  private void updatePercentRepairedForNode(
      Optional<Node> node,
      Cluster cluster
  ) {
    if (canPerformBeat(
        lastPercentRepairedBeat,
        TimeUnit.MINUTES.toMillis(context.config.getPercentRepairedCheckIntervalMinutes()))) {

      Collection<RepairSchedule> incrementalRepairSchedules
          = context.storage.getRepairSchedulesForCluster(cluster.getName(), true);

      incrementalRepairSchedules.stream().forEach(sched -> {
        try {
          metricsService.grabAndStorePercentRepairedMetrics(node, sched);
        } catch (ReaperException e) {
          if (node.isPresent()) {
            LOG.error("Couldn't extract % repaired metrics for node {} in cluster {}", node.get().getHostname(),
                node.get().getCluster().get().getName(), e);
          } else {
            LOG.error("Couldn't extract % repaired metrics for local node", e);
          }
        }
      });
      lastPercentRepairedBeat.set(System.currentTimeMillis());
    }
  }

  private void updateMetricsForNode(Node node)
      throws JsonProcessingException, JMException, ReaperException, InterruptedException {
    metricsService.grabAndStoreCompactionStats(Optional.of(node));
    metricsService.grabAndStoreActiveStreams(Optional.of(node));
    if (canPerformBeat(lastMetricBeat, maxBeatFrequencyMillis)) {
      metricsService.grabAndStoreGenericMetrics(Optional.of(node));
      lastMetricBeat.set(System.currentTimeMillis());
    }
  }

  private static Timer.Context timer(AppContext context, String... names) {
    return context.metricRegistry.timer(MetricRegistry.name(Heart.class, names)).time();
  }

  private void registerGauges() throws IllegalArgumentException {
    if (!GAUGES_REGISTERED.getAndSet(true)) {

      context.metricRegistry.register(
          MetricRegistry.name(Heart.class, "runningThreadCount"),
          (Gauge<Integer>) () -> forkJoinPool.getRunningThreadCount());

      context.metricRegistry.register(
          MetricRegistry.name(Heart.class, "activeThreadCount"),
          (Gauge<Integer>) () -> forkJoinPool.getActiveThreadCount());

      context.metricRegistry.register(
          MetricRegistry.name(Heart.class, "queuedTaskCount"),
          (Gauge<Long>) () -> forkJoinPool.getQueuedTaskCount());

      context.metricRegistry.register(
          MetricRegistry.name(Heart.class, "queuedSubmissionCount"),
          (Gauge<Integer>) () -> forkJoinPool.getQueuedSubmissionCount());
    }
  }

  private boolean canPerformBeat(AtomicLong lastBeat, long interval) {
    return lastBeat.get() + interval <= System.currentTimeMillis();
  }

  private boolean isDistributedAndCollocated() {
    return context.isDistributed.get() && context.config.getDatacenterAvailability().isInCollocatedMode();
  }
}