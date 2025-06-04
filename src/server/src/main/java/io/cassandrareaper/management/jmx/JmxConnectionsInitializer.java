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

package io.cassandrareaper.management.jmx;

import io.cassandrareaper.AppContext;
import io.cassandrareaper.ReaperApplicationConfiguration.DatacenterAvailability;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.management.ClusterFacade;
import io.cassandrareaper.management.ICassandraManagementProxy;
import io.cassandrareaper.storage.IDistributedStorage;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JmxConnectionsInitializer implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(JmxConnectionsInitializer.class);
  private final ExecutorService executor = Executors.newFixedThreadPool(10);

  private final AppContext context;

  private JmxConnectionsInitializer(AppContext context) {
    this.context = context;
  }

  public static JmxConnectionsInitializer create(AppContext context) {
    return new JmxConnectionsInitializer(context);
  }

  public void on(Cluster cluster) {
    if (context.storage instanceof IDistributedStorage
        && context.config.getDatacenterAvailability() != DatacenterAvailability.ALL
        && !context.config.isInSidecarMode()) {
      LOG.info("Initializing JMX seed list for cluster {}...", cluster.getName());
      List<Callable<Optional<String>>> jmxTasks = Lists.newArrayList();
      List<String> seedHosts = Lists.newArrayList();
      seedHosts.addAll(cluster.getSeedHosts());

      for (int i = 0; i < seedHosts.size(); i++) {
        jmxTasks.add(connectToJmx(cluster, Arrays.asList(seedHosts.get(i))));
        if (i % 10 == 0 || i == seedHosts.size() - 1) {
          tryConnectingToJmxSeeds(jmxTasks);
          jmxTasks = Lists.newArrayList();
        }
      }
    }
  }

  private Callable<Optional<String>> connectToJmx(Cluster cluster, List<String> endpoints) {
    return () -> {
      try {
        ClusterFacade.create(context).connectToManagementMechanism(cluster, endpoints);
        return Optional.of(endpoints.get(0));
      } catch (RuntimeException e) {
        LOG.info("failed to connect to hosts {} through JMX", endpoints.get(0), e);
        return Optional.empty();
      }
    };
  }

  private void tryConnectingToJmxSeeds(List<Callable<Optional<String>>> jmxTasks) {
    try {
      List<Future<Optional<String>>> endpointFutures =
          executor.invokeAll(
              jmxTasks,
              (int) ICassandraManagementProxy.DEFAULT_JMX_CONNECTION_TIMEOUT.getSeconds(),
              TimeUnit.SECONDS);

      for (Future<Optional<String>> endpointFuture : endpointFutures) {
        try {
          endpointFuture.get(
              (int) ICassandraManagementProxy.DEFAULT_JMX_CONNECTION_TIMEOUT.getSeconds(),
              TimeUnit.SECONDS);
        } catch (RuntimeException | ExecutionException | TimeoutException expected) {
          LOG.trace("Failed accessing one node through JMX", expected);
        }
      }
    } catch (InterruptedException e) {
      LOG.debug("Interrupted when trying to compile the list of nodes accessible through JMX", e);
    }
  }

  @Override
  public void close() throws RuntimeException {
    executor.shutdown();
  }
}
