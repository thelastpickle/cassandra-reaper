/*
 * Copyright 2014-2017 Spotify AB
 * Copyright 2016-2019 The Last Pickle Ltd
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
import io.cassandrareaper.ReaperApplicationConfiguration.Jmxmp;
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.JmxCredentials;
import io.cassandrareaper.core.Node;
import io.cassandrareaper.crypto.Cryptograph;
import io.cassandrareaper.management.HostConnectionCounters;
import io.cassandrareaper.management.ICassandraManagementProxy;
import io.cassandrareaper.management.IManagementConnectionFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.policies.AddressTranslator;
import com.datastax.driver.core.policies.EC2MultiRegionAddressTranslator;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JmxManagementConnectionFactory implements IManagementConnectionFactory {

  private static final Logger LOG = LoggerFactory.getLogger(JmxManagementConnectionFactory.class);
  private static final ConcurrentMap<String, ICassandraManagementProxy> JMX_CONNECTIONS = Maps.newConcurrentMap();
  private final MetricRegistry metricRegistry;
  private final HostConnectionCounters hostConnectionCounters;
  private final AppContext context;
  private final Cryptograph cryptograph;
  private Map<String, Integer> jmxPorts;
  private JmxCredentials jmxAuth;
  private Jmxmp jmxmp;
  private Map<String, JmxCredentials> jmxCredentials;
  private AddressTranslator addressTranslator;
  private final Set<String> accessibleDatacenters = Sets.newHashSet();

  public JmxManagementConnectionFactory(AppContext context, Cryptograph cryptograph) {
    this.metricRegistry
        = context.metricRegistry == null ? new MetricRegistry() : context.metricRegistry;
    hostConnectionCounters = new HostConnectionCounters(metricRegistry);
    registerConnectionsGauge();
    this.context = context;
    this.cryptograph = cryptograph;
    // Originally came from initialize in ReaperApplication. But we may as well initialize it here.
    initialize(context, cryptograph);
  }

  private void registerConnectionsGauge() {
    try {
      if (!this.metricRegistry
          .getGauges()
          .containsKey(MetricRegistry.name(JmxManagementConnectionFactory.class, "openJmxConnections"))) {
        this.metricRegistry.register(
            MetricRegistry.name(JmxManagementConnectionFactory.class, "openJmxConnections"),
            (Gauge<Integer>) () -> JMX_CONNECTIONS.size());
      }
    } catch (IllegalArgumentException e) {
      LOG.warn("Cannot create openJmxConnections metric gauge", e);
    }
  }

  protected String determineHost(Node node) {
    String host = node.getHostname();
    if (jmxPorts != null && jmxPorts.containsKey(host) && !host.contains(":")) {
      host = host + ":" + jmxPorts.get(host);
      LOG.debug("Connecting to {} with specific port", host);
    } else if (JmxAddresses.isNumericIPv6Address(host)) {
      host = "[" + host + "]:" + node.getJmxPort();
      LOG.debug("Connecting to ipv6 {} with custom port", host);
    } else {
      host = host + ":" + node.getJmxPort();
      LOG.debug("Connecting to {} with custom port", host);
    }
    return host;
  }

  public void initialize(AppContext context, Cryptograph cryptograph) {
    Map<String, Integer> jmxPorts = context.config.getJmxPorts();
    if (jmxPorts != null) {
      LOG.debug("using JMX ports mapping: {}", jmxPorts);
      setJmxPorts(jmxPorts);
    }
    if (context.config.useAddressTranslator()) {
      setAddressTranslator(new EC2MultiRegionAddressTranslator());
    }
    if (context.config.getJmxAddressTranslator().isPresent()) {
      AddressTranslator addressTranslator = context.config.getJmxAddressTranslator().get().build();
      setAddressTranslator(addressTranslator);
    }
    if (context.config.getJmxmp() != null) {
      if (context.config.getJmxmp().isEnabled()) {
        LOG.info("JMXMP enabled");
      }
      setJmxmp(context.config.getJmxmp());
    }

    JmxCredentials jmxAuth = context.config.getJmxAuth();
    if (jmxAuth != null) {
      LOG.debug("using specified JMX credentials for authentication");
      setJmxAuth(jmxAuth);
    }

    Map<String, JmxCredentials> jmxCredentials = context.config.getJmxCredentials();
    if (jmxCredentials != null) {
      LOG.debug("using specified JMX credentials per cluster for authentication");
      setJmxCredentials(jmxCredentials);
    }
  }


  protected ICassandraManagementProxy connectImpl(Node node) throws ReaperException, InterruptedException {
    // use configured jmx port for host if provided
    String host = determineHost(node);

    Optional<JmxCredentials> jmxCredentials = getJmxCredentialsForCluster(node.getCluster());

    try {
      JmxConnectionProvider provider = new JmxConnectionProvider(
          host, jmxCredentials, context.config.getJmxConnectionTimeoutInSeconds(),
          this.metricRegistry, cryptograph, this.jmxmp);
      JMX_CONNECTIONS.computeIfAbsent(host, provider::apply);
      ICassandraManagementProxy proxy = JMX_CONNECTIONS.get(host);
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

  @VisibleForTesting
  public final ICassandraManagementProxy connectAny(Collection<Node> nodes) throws ReaperException {

    Preconditions.checkArgument(
        null != nodes && !nodes.isEmpty(), "no hosts provided to connectAny");

    List<Node> nodeList = new ArrayList<>(nodes);
    Collections.shuffle(nodeList);

    for (int i = 0; i < 2; i++) {
      for (Node node : nodeList) {
        // First loop, we try the most accessible nodes, then second loop we try all nodes
        if (getHostConnectionCounters().getSuccessfulConnections(node.getHostname()) >= 0 || 1 == i) {
          try {
            LOG.debug("Trying to connect to node {} with {} successful connections with i = {}",
                node.getHostname(), getHostConnectionCounters().getSuccessfulConnections(node.getHostname()), i);
            ICassandraManagementProxy cassandraManagementProxy = connectImpl(node);
            getHostConnectionCounters().incrementSuccessfulConnections(node.getHostname());
            if (getHostConnectionCounters().getSuccessfulConnections(node.getHostname()) > 0) {
              accessibleDatacenters.add(EndpointSnitchInfoProxy.create(cassandraManagementProxy).getDataCenter());
            }
            return cassandraManagementProxy;
          } catch (ReaperException | RuntimeException e) {
            getHostConnectionCounters().decrementSuccessfulConnections(node.getHostname());
            LOG.info("Unreachable host: ", e);
          } catch (InterruptedException expected) {
            LOG.trace("Expected exception", expected);
          }
        }
      }
    }
    throw new ReaperException("no host could be reached through JMX");
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

  public final void setAddressTranslator(AddressTranslator addressTranslator) {
    this.addressTranslator = addressTranslator;
  }

  public Jmxmp getJmxmp() {
    return jmxmp;
  }

  public void setJmxmp(Jmxmp jmxmp) {
    this.jmxmp = jmxmp;
  }

  public HostConnectionCounters getHostConnectionCounters() {
    return hostConnectionCounters;
  }

  public final Set<String> getAccessibleDatacenters() {
    return accessibleDatacenters;
  }

  public Optional<JmxCredentials> getJmxCredentialsForCluster(Optional<Cluster> cluster) {
    JmxCredentials credentials = cluster.flatMap(Cluster::getJmxCredentials).orElse(null);
    String clusterName = cluster.map(Cluster::getName).orElse("");

    if (credentials == null && jmxCredentials != null) {
      if (jmxCredentials.containsKey(clusterName)) {
        credentials = jmxCredentials.get(clusterName);
      } else if (jmxCredentials.containsKey(Cluster.toSymbolicName(clusterName))) {
        // As clusters get stored in the database with their "symbolic name" we have to look for that too
        credentials = jmxCredentials.get(Cluster.toSymbolicName(clusterName));
      }
    }

    if (credentials == null && jmxAuth != null) {
      credentials = jmxAuth;
    }

    return Optional.ofNullable(credentials);
  }

  private class JmxConnectionProvider implements Function<String, ICassandraManagementProxy> {

    private final String host;
    private final Optional<JmxCredentials> jmxCredentials;
    private final int connectionTimeout;
    private final MetricRegistry metricRegistry;
    private final Cryptograph cryptograph;
    private final Jmxmp jmxmp;

    JmxConnectionProvider(
        String host,
        Optional<JmxCredentials> jmxCredentials,
        int connectionTimeout,
        MetricRegistry metricRegistry,
        Cryptograph cryptograph,
        Jmxmp jmxmp) {
      this.host = host;
      this.jmxCredentials = jmxCredentials;
      this.connectionTimeout = connectionTimeout;
      this.metricRegistry = metricRegistry;
      this.cryptograph = cryptograph;
      this.jmxmp = jmxmp;
    }

    @Override
    public ICassandraManagementProxy apply(String host) {
      Preconditions.checkArgument(host.equals(this.host));
      try {
        ICassandraManagementProxy proxy = JmxCassandraManagementProxy.connect(
            host, jmxCredentials, addressTranslator, connectionTimeout, metricRegistry, cryptograph, jmxmp);
        return proxy;
      } catch (ReaperException | InterruptedException ex) {
        throw new RuntimeException(ex);
      }
    }
  }
}