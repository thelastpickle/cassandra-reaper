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
import io.cassandrareaper.ReaperApplicationConfiguration.DatacenterAvailability;
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.Node;
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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Heart implements AutoCloseable {

  private static final AtomicBoolean GAUGES_REGISTERED = new AtomicBoolean(false);
  private static final Logger LOG = LoggerFactory.getLogger(Heart.class);
  private static final long DEFAULT_MAX_FREQUENCY = TimeUnit.SECONDS.toMillis(30);

  private final AtomicLong lastBeat = new AtomicLong(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1));
  private final AtomicLong lastMetricBeat = new AtomicLong(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1));
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

  public synchronized void beat() {
    assert context.storage instanceof IDistributedStorage : "only valid with IDistributedStorage backend";

    if (lastBeat.get() + maxBeatFrequencyMillis < System.currentTimeMillis()) {
      lastBeat.set(System.currentTimeMillis());
      ((IDistributedStorage) context.storage).saveHeartbeat();

      if (!context.isDistributed.get() && 1 < ((IDistributedStorage) context.storage).countRunningReapers()) {
        context.isDistributed.set(true);
      }
    }
    if (context.isDistributed.get() && context.config.getDatacenterAvailability().isInCollocatedMode()) {
      updateRequestedNodeMetrics();
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

  private void updateRequestedNodeMetrics() {
    Preconditions.checkState(context.isDistributed.get(), "Only valid with multiple Reaper instances");

    Preconditions.checkState(
        context.config.getDatacenterAvailability().isInCollocatedMode(),
        "metrics are fetched directly in ALL mode");

    registerGauges();

    if (!updatingNodeMetrics.getAndSet(true)) {
      forkJoinPool.submit(() -> {
        try (Timer.Context t0 = timer(context, "updatingNodeMetrics")) {
          ClusterFacade clusterFacade = ClusterFacade.create(context);
          Collection<Cluster> clusters = context.storage.getClusters();
          forkJoinPool.submit(() -> {
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
                            .withCluster(
                                Cluster.builder()
                                    .withName(cluster.getName())
                                    .withSeedHosts(ImmutableSet.of(hostname))
                                    .build())
                            .build())
                        .forEach(node -> {
                          try {
                            metricsService.grabAndStoreCompactionStats(Optional.of(node));
                            metricsService.grabAndStoreActiveStreams(Optional.of(node));
                            if (lastMetricBeat.get() + maxBeatFrequencyMillis <= System.currentTimeMillis()) {
                              metricsService.grabAndStoreGenericMetrics(Optional.of(node));
                              lastMetricBeat.set(System.currentTimeMillis());
                            }
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
          }).get();

          if (context.config.getDatacenterAvailability() == DatacenterAvailability.SIDECAR) {
            // In sidecar mode we store metrics in the db on a regular basis

            if (lastMetricBeat.get() + maxBeatFrequencyMillis <= System.currentTimeMillis()) {
              metricsService.grabAndStoreGenericMetrics(Optional.empty());
              lastMetricBeat.set(System.currentTimeMillis());
            } else {
              LOG.trace("Not storing metrics yet... Last beat was {} and now is {}",
                  lastMetricBeat.get(),
                  System.currentTimeMillis());
            }
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
}
