/*
 * Copyright 2014-2017 Spotify AB
 * Copyright 2016-2018 The Last Pickle Ltd
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

package io.cassandrareaper.jmx;

import io.cassandrareaper.ReaperApplicationConfiguration.JmxCredentials;
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.Node;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.policies.EC2MultiRegionAddressTranslator;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JmxConnectionFactory {

  private static final Logger LOG = LoggerFactory.getLogger(JmxConnectionFactory.class);
  private static final ConcurrentMap<String, JmxProxy> JMX_CONNECTIONS = Maps.newConcurrentMap();
  private final MetricRegistry metricRegistry;
  private final HostConnectionCounters hostConnectionCounters;
  private Map<String, Integer> jmxPorts;
  private JmxCredentials jmxAuth;
  private Map<String, JmxCredentials> jmxCredentials;
  private EC2MultiRegionAddressTranslator addressTranslator;

  @VisibleForTesting
  public JmxConnectionFactory() {
    this.metricRegistry = new MetricRegistry();
    hostConnectionCounters = new HostConnectionCounters(metricRegistry);
    registerConnectionsGauge();
  }

  public JmxConnectionFactory(MetricRegistry metricRegistry) {
    this.metricRegistry = metricRegistry;
    hostConnectionCounters = new HostConnectionCounters(metricRegistry);
    registerConnectionsGauge();
  }

  private void registerConnectionsGauge() {
    try {
      if (!this.metricRegistry
          .getGauges()
          .containsKey(MetricRegistry.name(JmxConnectionFactory.class, "openJmxConnections"))) {
        this.metricRegistry.register(
            MetricRegistry.name(JmxConnectionFactory.class, "openJmxConnections"),
            (Gauge<Integer>) () -> JMX_CONNECTIONS.size());
      }
    } catch (IllegalArgumentException e) {
      LOG.warn("Cannot create openJmxConnections metric gauge", e);
    }
  }

  protected JmxProxy connectImpl(Node node, int connectionTimeout) throws ReaperException, InterruptedException {
    // use configured jmx port for host if provided
    String host = node.getHostname();
    if (jmxPorts != null && jmxPorts.containsKey(host) && !host.contains(":")) {
      host = host + ":" + jmxPorts.get(host);
    }

    String username = null;
    String password = null;
    if (getJmxCredentialsForCluster(node.getCluster().getName()).isPresent()) {
      username = getJmxCredentialsForCluster(node.getCluster().getName()).get().getUsername();
      password = getJmxCredentialsForCluster(node.getCluster().getName()).get().getPassword();
    }

    try {
      JmxConnectionProvider provider = new JmxConnectionProvider(
              host, username, password, connectionTimeout, this.metricRegistry);
      JMX_CONNECTIONS.computeIfAbsent(host, provider::apply);
      JmxProxy proxy = JMX_CONNECTIONS.get(host);
      if (!proxy.isConnectionAlive()) {
        LOG.info("Adding new JMX Proxy for host {}", host);
        JMX_CONNECTIONS.put(host, provider.apply(host)).close();
      }
      return JMX_CONNECTIONS.get(host);
    } catch (RuntimeException ex) {
      // unpack any exception behind JmxConnectionProvider.apply(..)
      if (ex.getCause() instanceof InterruptedException) {
        throw (InterruptedException) ex.getCause();
      } else {
        LOG.error("Failed creating a new JMX connection to {}", host, ex);
      }
      if (ex.getCause() instanceof ReaperException) {
        throw (ReaperException) ex.getCause();
      }
      throw ex;
    }
  }

  public JmxProxy connect(Node node, int connectionTimeout) throws ReaperException, InterruptedException {
    return connectImpl(node, connectionTimeout);
  }

  public final JmxProxy connectAny(Collection<Node> nodes, int connectionTimeout) throws ReaperException {

    Preconditions.checkArgument(
        null != nodes && !nodes.isEmpty(), "no hosts provided to connectAny");

    List<Node> nodeList = new ArrayList<>(nodes);
    Collections.shuffle(nodeList);

    for (int i = 0; i < 2; i++) {
      for (Node node : nodeList) {
        // First loop, we try the most accessible nodes, then second loop we try all nodes
        if (hostConnectionCounters.getSuccessfulConnections(node.getHostname()) >= 0 || 1 == i) {
          try {
            return connectImpl(node, connectionTimeout);
          } catch (ReaperException | RuntimeException e) {
            LOG.info("Unreachable host: {}: {}", e.getMessage(), e.getCause().getMessage());
            LOG.debug("Unreachable host: ", e);
          } catch (InterruptedException expected) {
            LOG.trace("Expected exception", expected);
          }
        }
      }
    }
    throw new ReaperException("no host could be reached through JMX");
  }

  public JmxProxy connectAny(Cluster cluster, int connectionTimeout) throws ReaperException {
    Set<Node> nodes = cluster
            .getSeedHosts()
            .stream()
            .map(host -> Node.builder().withCluster(cluster).withHostname(host).build())
            .collect(Collectors.toSet());

    if (nodes == null || nodes.isEmpty()) {
      throw new ReaperException("no seeds in cluster with name: " + cluster.getName());
    }
    return connectAny(nodes, connectionTimeout);
  }

  public final void setJmxAuth(JmxCredentials jmxAuth) {
    this.jmxAuth = jmxAuth;
  }

  public final void setJmxCredentials(Map<String, JmxCredentials> jmxCredentials) {
    this.jmxCredentials = jmxCredentials;
  }

  public final void setJmxPorts(Map<String, Integer> jmxPorts) {
    this.jmxPorts = jmxPorts;
  }

  public final void setAddressTranslator(EC2MultiRegionAddressTranslator addressTranslator) {
    this.addressTranslator = addressTranslator;
  }

  public final HostConnectionCounters getHostConnectionCounters() {
    return hostConnectionCounters;
  }

  public Optional<JmxCredentials> getJmxCredentialsForCluster(String clusterName) {
    Optional<JmxCredentials> jmxCreds = Optional.ofNullable(jmxAuth);
    if (jmxCredentials != null && jmxCredentials.containsKey(clusterName)) {
      jmxCreds = Optional.of(jmxCredentials.get(clusterName));
    }

    // As clusters get stored in the database with their "symbolic name" we have to look for that too
    if (jmxCredentials != null && jmxCredentials.containsKey(Cluster.toSymbolicName(clusterName))) {
      jmxCreds = Optional.of(jmxCredentials.get(Cluster.toSymbolicName(clusterName)));
    }

    return jmxCreds;
  }

  private class JmxConnectionProvider implements Function<String, JmxProxy> {

    private final String host;
    private final String username;
    private final String password;
    private final int connectionTimeout;
    private final MetricRegistry metricRegistry;

    JmxConnectionProvider(
        String host,
        String username,
        String password,
        int connectionTimeout,
        MetricRegistry metricRegistry) {
      this.host = host;
      this.username = username;
      this.password = password;
      this.connectionTimeout = connectionTimeout;
      this.metricRegistry = metricRegistry;
    }

    @Override
    public JmxProxy apply(String host) {
      Preconditions.checkArgument(host.equals(this.host));
      try {
        JmxProxy proxy = JmxProxyImpl.connect(
                host, username, password, addressTranslator, connectionTimeout, metricRegistry);
        hostConnectionCounters.incrementSuccessfulConnections(host);
        return proxy;
      } catch (ReaperException | InterruptedException ex) {
        hostConnectionCounters.decrementSuccessfulConnections(host);
        throw new RuntimeException(ex);
      }
    }
  }
}
