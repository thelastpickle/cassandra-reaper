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

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JmxConnectionFactory {

  private static final Logger LOG = LoggerFactory.getLogger(JmxConnectionFactory.class);

  private final MetricRegistry metricRegistry;
  private final HostConnectionCounters hostConnectionCounters;
  private Map<String, Integer> jmxPorts;
  private JmxCredentials jmxAuth;
  private CustomEC2MultiRegionAddressTranslator addressTranslator;

  @VisibleForTesting
  public JmxConnectionFactory() {
    this.metricRegistry = new MetricRegistry();
    hostConnectionCounters = new HostConnectionCounters(metricRegistry);
  }

  public JmxConnectionFactory(MetricRegistry metricRegistry) {
    this.metricRegistry = metricRegistry;
    hostConnectionCounters = new HostConnectionCounters(metricRegistry);
  }

  protected JmxProxy connect(Optional<RepairStatusHandler> handler, String host, int connectionTimeout)
      throws ReaperException, InterruptedException {
    // use configured jmx port for host if provided
    if (jmxPorts != null && jmxPorts.containsKey(host) && !host.contains(":")) {
      host = host + ":" + jmxPorts.get(host);
    }

    String username = null;
    String password = null;
    if (jmxAuth != null) {
      username = jmxAuth.getUsername();
      password = jmxAuth.getPassword();
    }
    try {
      JmxProxy jmxProxy = JmxProxyImpl.connect(handler, host, username, password, addressTranslator, connectionTimeout);
      hostConnectionCounters.incrementSuccessfulConnections(host);
      return jmxProxy;
    } catch (ReaperException | RuntimeException e) {
      hostConnectionCounters.decrementSuccessfulConnections(host);
      throw e;
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

  public final void setAddressTranslator(CustomEC2MultiRegionAddressTranslator addressTranslator) {
    this.addressTranslator = addressTranslator;
  }

  public final HostConnectionCounters getHostConnectionCounters() {
    return hostConnectionCounters;
  }
}
