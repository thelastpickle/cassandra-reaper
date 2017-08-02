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
package com.spotify.reaper.cassandra;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.datastax.driver.core.policies.EC2MultiRegionAddressTranslator;
import com.spotify.reaper.ReaperApplicationConfiguration.JmxCredentials;
import com.spotify.reaper.ReaperException;
import com.spotify.reaper.core.Cluster;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JmxConnectionFactory {

  private static final ConcurrentMap<String, AtomicInteger> SUCCESSFULL_CONNECTIONS = Maps.newConcurrentMap();
  private static final Logger LOG = LoggerFactory.getLogger(JmxConnectionFactory.class);

  private Map<String, Integer> jmxPorts;
  private JmxCredentials jmxAuth;
  private EC2MultiRegionAddressTranslator addressTranslator;
  private boolean localMode = false;
  private MetricRegistry metricRegistry;

  public JmxProxy connect(Optional<RepairStatusHandler> handler, String host, int connectionTimeout)
      throws ReaperException {
    // use configured jmx port for host if provided
    if(localMode) {
      host = "127.0.0.1";
    }
    if (jmxPorts != null && jmxPorts.containsKey(host) && !host.contains(":")) {
      host = host + ":" + jmxPorts.get(host);
    }

    String username = null;
    String password = null;
    if(jmxAuth != null) {
      username = jmxAuth.getUsername();
      password = jmxAuth.getPassword();
    }
    return JmxProxy.connect(handler, host, username, password, addressTranslator, connectionTimeout);
  }

  public final JmxProxy connect(String host, int connectionTimeout) throws ReaperException {
    return connect(Optional.<RepairStatusHandler>absent(), host, connectionTimeout);
  }

  public final JmxProxy connectAny(Optional<RepairStatusHandler> handler, Collection<String> hosts, int connectionTimeout)
      throws ReaperException {
    if (hosts == null || hosts.isEmpty()) {
      throw new ReaperException("no hosts given for connectAny");
    }
    List<String> hostList = new ArrayList<>(hosts);
    Collections.shuffle(hostList);
    Iterator<String> hostIterator = hostList.iterator();

    for (int i=0 ; i<2 ; i++) {
      while (hostIterator.hasNext()) {
        String host = hostIterator.next();

        // First loop, we try the most accessible nodes, then second loop we try all nodes
        if(null != host && (SUCCESSFULL_CONNECTIONS.getOrDefault(host, new AtomicInteger(0)).get() >= 0 || 1 == i)) {
          try {
            JmxProxy jmxProxy = connect(handler, host, connectionTimeout);
            incrementSuccessfullConnections(host);
            return jmxProxy;
          } catch (ReaperException | RuntimeException e) {
            decrementSuccessfullConnections(host);
            LOG.info("Unreachable host", e);
          }
        }
      }
    }

    throw new ReaperException("no host could be reached through JMX");
  }

  public final JmxProxy connectAny(Cluster cluster, int connectionTimeout)
      throws ReaperException {
    Set<String> hosts = cluster.getSeedHosts();
    if (hosts == null || hosts.isEmpty()) {
      throw new ReaperException("no seeds in cluster with name: " + cluster.getName());
    }
    return connectAny(Optional.<RepairStatusHandler>absent(), hosts, connectionTimeout);
  }

  public void setJmxPorts(Map<String, Integer> jmxPorts) {
    this.jmxPorts = jmxPorts;
  }

  public void setJmxAuth(JmxCredentials jmxAuth) {
    this.jmxAuth = jmxAuth;
  }

  public void setAddressTranslator(EC2MultiRegionAddressTranslator addressTranslator) {
    this.addressTranslator = addressTranslator;
  }

  public void setLocalMode(boolean localMode) {
    this.localMode = localMode;
  }

  public void setMetricRegistry(MetricRegistry metricRegistry) {
    this.metricRegistry = metricRegistry;
  }

  private void incrementSuccessfullConnections(String host) {
    try {
      AtomicInteger successes = SUCCESSFULL_CONNECTIONS.putIfAbsent(host, new AtomicInteger(1));
      if(null != successes && successes.get() <= 20) {
        successes.incrementAndGet();
      }
      LOG.debug("Host {} has {} successfull connections", host, successes);
      if (null != metricRegistry) {
          metricRegistry.counter(MetricRegistry.name(JmxConnectionFactory.class, "connections", host.replace('.', '-'))).inc();
      }
    } catch(RuntimeException e) {
      LOG.warn("Could not increment JMX successfull connections counter for host {}", host, e);
    }
  }

  private void decrementSuccessfullConnections(String host) {
    try {
      AtomicInteger successes = SUCCESSFULL_CONNECTIONS.putIfAbsent(host, new AtomicInteger(-1));
      if(null != successes && successes.get() >= -5) {
        successes.decrementAndGet();
      }
      LOG.debug("Host {} has {} successfull connections", host, successes);
      if (null != metricRegistry) {
          metricRegistry.counter(MetricRegistry.name(JmxConnectionFactory.class, "connections", host.replace('.', '-'))).dec();
      }
    } catch(RuntimeException e) {
      LOG.warn("Could not decrement JMX successfull connections counter for host {}", host, e);
    }
  }
}
