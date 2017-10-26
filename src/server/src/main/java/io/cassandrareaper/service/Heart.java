/*
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
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.NodeMetrics;
import io.cassandrareaper.core.NodeMetricsRequest;
import io.cassandrareaper.jmx.JmxProxy;
import io.cassandrareaper.storage.IDistributedStorage;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import javax.management.AttributeNotFoundException;
import javax.management.MBeanException;
import javax.management.ReflectionException;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


final class Heart implements AutoCloseable {

  private static final AtomicBoolean GAUGES_REGISTERED = new AtomicBoolean(false);
  private static final Logger LOG = LoggerFactory.getLogger(Heart.class);
  private static final long DEFAULT_MAX_FREQUENCY = TimeUnit.MINUTES.toMillis(1);

  private final AtomicLong lastBeat = new AtomicLong(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1));
  private final ForkJoinPool forkJoinPool = new ForkJoinPool(64);
  private final long maxBeatFrequencyMillis;
  private final AtomicBoolean updatingNodeMetrics = new AtomicBoolean(false);

  private Heart(long maxBeatFrequency) {
    this.maxBeatFrequencyMillis = maxBeatFrequency;
  }

  static Heart create() {
    return new Heart(DEFAULT_MAX_FREQUENCY);
  }

  @VisibleForTesting
  static Heart create(long maxBeatFrequencyMillis) {
    return new Heart(maxBeatFrequencyMillis);
  }

  synchronized void beat(AppContext context) {
    if (context.storage instanceof IDistributedStorage
        && lastBeat.get() + maxBeatFrequencyMillis < System.currentTimeMillis()) {

      lastBeat.set(System.currentTimeMillis());
      ((IDistributedStorage) context.storage).saveHeartbeat();
    }
    if (context.storage instanceof IDistributedStorage
        && ReaperApplicationConfiguration.DatacenterAvailability.EACH
            == context.config.getDatacenterAvailability()) {
      updateAllRequestedNodeMetrics(context);
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

  private void updateAllRequestedNodeMetrics(AppContext context) {
    Preconditions.checkArgument(context.storage instanceof IDistributedStorage);
    IDistributedStorage storage = ((IDistributedStorage) context.storage);
    registerGauges(context);

    if (!updatingNodeMetrics.getAndSet(true)) {
      int jmxTimeoutSeconds = context.config.getJmxConnectionTimeoutInSeconds();

      forkJoinPool.submit(
          () -> {
            try (Timer.Context t0 = timer(context, "metrics", "all")) {

              Set<UUID> activeRepairRuns = context.repairManager.repairRunners.keySet();
              List<NodeMetricsRequest> requests =
                  activeRepairRuns
                      .stream()
                      .map(runId -> storage.listNodeMetricsRequests(runId))
                      .flatMap(Collection::stream)
                      .collect(Collectors.toList());

              forkJoinPool
                  .submit(
                      () -> {
                        requests
                            .parallelStream()
                            .forEach(
                                req -> {
                                  LOG.info("Got metric request for node {}", req.getHostAddress());
                                  try (Timer.Context t2 =
                                      timer(
                                          context,
                                          "metrics",
                                          req.getCluster(),
                                          req.getHostAddress())) {

                                    try (JmxProxy nodeProxy =
                                        context.jmxConnectionFactory.connect(
                                            req.getHostAddress(), jmxTimeoutSeconds)) {

                                      storage.storeNodeMetrics(
                                          NodeMetrics.builder()
                                              .withHostAddress(req.getHostAddress())
                                              .withDatacenter(nodeProxy.getDataCenter())
                                              .withPendingCompactions(
                                                  nodeProxy.getPendingCompactions())
                                              .withHasRepairRunning(nodeProxy.isRepairRunning())
                                              .withActiveAnticompactions(0) // for future use
                                              .build());

                                      ((IDistributedStorage) context.storage)
                                          .removeNodeMetricsRequests(
                                              req.getRunId(), req.getHostAddress());

                                      LOG.info(
                                          "Responded to metric request for node {}",
                                          req.getHostAddress());
                                    } catch (ReaperException
                                        | RuntimeException
                                        | InterruptedException ex) {
                                      LOG.debug(
                                          "failed seed connection in cluster " + req.getCluster(),
                                          ex);
                                    } catch (AttributeNotFoundException
                                        | MBeanException
                                        | ReflectionException e) {
                                      LOG.warn(
                                          "failed querying JMX MBean for metrics on node {} of cluster {}",
                                          req.getHostAddress(),
                                          req.getCluster(),
                                          e);
                                    }
                                  }
                                });
                      })
                  .get();
            } catch (ExecutionException | InterruptedException | RuntimeException ex) {
              LOG.warn("failed updateAllReachableNodeMetrics submission", ex);
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

  private void registerGauges(AppContext context) throws IllegalArgumentException {
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
