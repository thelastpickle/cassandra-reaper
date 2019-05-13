/*
 * Copyright 2017-2017 Spotify AB
 * Copyright 2017-2018 The Last Pickle Ltd
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
import io.cassandrareaper.ReaperApplicationConfiguration;
import io.cassandrareaper.ReaperApplicationConfiguration.DatacenterAvailability;
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.NodeMetrics;
import io.cassandrareaper.jmx.ClusterFacade;
import io.cassandrareaper.jmx.JmxProxy;
import io.cassandrareaper.storage.IDistributedStorage;
import io.cassandrareaper.storage.IStorage;

import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
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


final class Heart implements AutoCloseable {

  private static final AtomicBoolean GAUGES_REGISTERED = new AtomicBoolean(false);
  private static final Logger LOG = LoggerFactory.getLogger(Heart.class);
  private static final long DEFAULT_MAX_FREQUENCY = TimeUnit.SECONDS.toMillis(30);
  private static final String[] COLLECTED_METRICS
    = {"org.apache.cassandra.metrics:type=ThreadPools,path=request,*",
       "org.apache.cassandra.metrics:type=ThreadPools,path=internal,*",
       "org.apache.cassandra.metrics:type=ClientRequest,*",
       "org.apache.cassandra.metrics:type=DroppedMessage,*"};

  private final AtomicLong lastBeat = new AtomicLong(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1));
  private final ForkJoinPool forkJoinPool = new ForkJoinPool(64);
  private final AppContext context;
  private final MetricsService metricsService;
  private final long maxBeatFrequencyMillis;
  private final AtomicBoolean updatingNodeMetrics = new AtomicBoolean(false);

  private Heart(AppContext context, long maxBeatFrequency) throws ReaperException, InterruptedException {
    this.context = context;
    this.maxBeatFrequencyMillis = maxBeatFrequency;
    this.metricsService = MetricsService.create(context);
  }

  static Heart create(AppContext context) throws ReaperException, InterruptedException {
    return new Heart(context, DEFAULT_MAX_FREQUENCY);
  }

  @VisibleForTesting
  static Heart create(AppContext context, long maxBeatFrequencyMillis) throws ReaperException, InterruptedException {
    return new Heart(context, maxBeatFrequencyMillis);
  }

  synchronized void beat() {
    if (context.storage instanceof IDistributedStorage
        && lastBeat.get() + maxBeatFrequencyMillis < System.currentTimeMillis()) {

      lastBeat.set(System.currentTimeMillis());
      ((IDistributedStorage) context.storage).saveHeartbeat();
    }
  }

  synchronized void beatMetrics() {
    if (context.storage instanceof IDistributedStorage
            && ReaperApplicationConfiguration.DatacenterAvailability.EACH
                == context.config.getDatacenterAvailability()
        || ReaperApplicationConfiguration.DatacenterAvailability.SIDECAR
            == context.config.getDatacenterAvailability()) {
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
    Preconditions.checkArgument(context.storage instanceof IDistributedStorage);
    //IDistributedStorage storage = ((IDistributedStorage) context.storage);
    registerGauges();

    if (!updatingNodeMetrics.getAndSet(true)) {
      forkJoinPool.submit(() -> {
        try (Timer.Context t0 = timer(context, "updatingNodeMetrics")) {

          forkJoinPool.submit(() -> {
            context.repairManager.repairRunners.keySet()
                .parallelStream()
                .forEach(runId -> {

                  ((IDistributedStorage) context.storage).getNodeMetrics(runId)
                      .parallelStream()
                      .filter(metrics -> canAnswerToNodeMetricsRequest(metrics))
                      .forEach(req -> {

                        LOG.info("Got metric request for node {} in {}", req.getNode(), req.getCluster());
                        try (Timer.Context t1 = timer(
                            context,
                            req.getCluster().replace('.', '-'),
                            req.getNode().replace('.', '-'))) {

                          try {
                            grabAndStoreNodeMetrics(context.storage, runId, req);

                            LOG.info("Responded to metric request for node {}", req.getNode());
                          } catch (ReaperException | RuntimeException | InterruptedException ex) {
                            LOG.debug("failed seed connection in cluster " + req.getCluster(), ex);
                          } catch (JMException e) {
                            LOG.warn(
                                "failed querying JMX MBean for metrics on node {} of cluster {} due to {}",
                                req.getNode(), req.getCluster(), e.getMessage());
                          }
                        }
                      });
                });
          }).get();

          if (context.config.getDatacenterAvailability() == DatacenterAvailability.SIDECAR) {
            // In sidecar mode we store metrics in the db on a regular basis
            metricsService.grabAndStoreGenericMetrics();
            metricsService.grabAndStoreActiveCompactions();
            metricsService.grabAndStoreActiveStreams();
          }
        } catch (ExecutionException | InterruptedException | RuntimeException
            | ReaperException | JMException | JsonProcessingException ex) {
          LOG.warn("Failed metric collection during heartbeat", ex);
        } finally {
          assert updatingNodeMetrics.get();
          updatingNodeMetrics.set(false);
        }
      });
    }
  }

  /**
   * Checks if the local Reaper instance is supposed to answer a metrics request.
   * Requires to be in sidecar on the node for which metrics are requested, or to be in a different mode than ALL.
   * Also checks that the metrics record as requested set to true.
   *
   * @param metric a metric request
   * @return true if reaper should try to answer the metric request
   */
  private boolean canAnswerToNodeMetricsRequest(NodeMetrics metric) {
    return (context.config.getDatacenterAvailability() == DatacenterAvailability.SIDECAR
            && metric.getNode().equals(context.getLocalNodeAddress()))
        || (context.config.getDatacenterAvailability() != DatacenterAvailability.ALL
        && context.config.getDatacenterAvailability() != DatacenterAvailability.SIDECAR)
        && metric.isRequested();
  }

  private void grabAndStoreNodeMetrics(IStorage storage, UUID runId, NodeMetrics req)
      throws ReaperException, InterruptedException, JMException {
    Optional<Cluster> cluster = storage.getCluster(req.getCluster());
    Preconditions.checkArgument(cluster.isPresent(), "The cluster should be present in storage.");
    JmxProxy nodeProxy
        = ClusterFacade.create(context).connectAndAllowSidecar(cluster.get(), Arrays.asList(req.getNode()));

    ((IDistributedStorage) storage).storeNodeMetrics(
        runId,
        NodeMetrics.builder()
            .withNode(req.getNode())
            .withCluster(req.getCluster())
            .withDatacenter(req.getDatacenter())
            .withPendingCompactions(nodeProxy.getPendingCompactions())
            .withHasRepairRunning(nodeProxy.isRepairRunning())
            .withActiveAnticompactions(0) // for future use
            .build());
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
