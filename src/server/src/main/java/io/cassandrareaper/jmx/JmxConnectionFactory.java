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

package io.cassandrareaper.jmx;

import io.cassandrareaper.ReaperApplicationConfiguration.JmxCredentials;
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Cluster;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.policies.EC2MultiRegionAddressTranslator;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
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

  protected JmxProxy connect(Optional<RepairStatusHandler> handler, String host, int connectionTimeout)
      throws ReaperException, InterruptedException {
    // use configured jmx port for host if provided
    if (jmxPorts != null && jmxPorts.containsKey(host) && !host.contains(":")) {
      host = host + ":" + jmxPorts.get(host);
    }

    String username = (jmxAuth != null) ? jmxAuth.getUsername() : null;
    String password = (jmxAuth != null) ? jmxAuth.getPassword() : null;

    try {
      JmxConnectionProvider provider =
          new JmxConnectionProvider(
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

  public final JmxProxy connect(String host, int connectionTimeout) throws ReaperException, InterruptedException {
    return connect(Optional.<RepairStatusHandler>absent(), host, connectionTimeout);
  }

  public final JmxProxy connectAny(
      Optional<RepairStatusHandler> handler,
      Collection<String> hosts,
      int connectionTimeout) throws ReaperException {

    Preconditions.checkArgument(null != hosts && !hosts.isEmpty(), "no hosts provided to connectAny");

    List<String> hostList = new ArrayList<>(hosts);
    Collections.shuffle(hostList);

    for (int i = 0; i < 2; i++) {
      for (String host : hostList) {
        assert null != host; // @todo remove the null check in the following if condition
        // First loop, we try the most accessible nodes, then second loop we try all nodes
        if (null != host && (hostConnectionCounters.getSuccessfulConnections(host) >= 0 || 1 == i)) {
          try {
            return connect(handler, host, connectionTimeout);
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

  public final JmxProxy connectAny(Cluster cluster, int connectionTimeout) throws ReaperException {
    Set<String> hosts = cluster.getSeedHosts();
    if (hosts == null || hosts.isEmpty()) {
      throw new ReaperException("no seeds in cluster with name: " + cluster.getName());
    }
    return connectAny(Optional.<RepairStatusHandler>absent(), hosts, connectionTimeout);
  }

  public final void setJmxPorts(Map<String, Integer> jmxPorts) {
    this.jmxPorts = jmxPorts;
  }

  public final void setJmxAuth(JmxCredentials jmxAuth) {
    this.jmxAuth = jmxAuth;
  }

  public final void setAddressTranslator(EC2MultiRegionAddressTranslator addressTranslator) {
    this.addressTranslator = addressTranslator;
  }

  public final HostConnectionCounters getHostConnectionCounters() {
    return hostConnectionCounters;
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
        JmxProxy proxy =
            JmxProxyImpl.connect(
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
