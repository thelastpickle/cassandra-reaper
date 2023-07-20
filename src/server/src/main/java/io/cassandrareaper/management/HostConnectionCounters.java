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

package io.cassandrareaper.management;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class HostConnectionCounters {

  private static final Logger LOG = LoggerFactory.getLogger(HostConnectionCounters.class);

  private final ConcurrentMap<String, AtomicInteger> successfulConnections = Maps.newConcurrentMap();

  private final MetricRegistry metricRegistry;

  public HostConnectionCounters(MetricRegistry metricRegistry) {
    this.metricRegistry = metricRegistry;
  }

  public void incrementSuccessfulConnections(String host) {
    try {
      AtomicInteger successes = successfulConnections.putIfAbsent(host, new AtomicInteger(1));
      if (null != successes && successes.get() <= 20) {
        successes.incrementAndGet();
      }
      LOG.debug("Host {} has {} successful connections", host, successes);
      if (null != metricRegistry) {
        metricRegistry
            .counter(
                MetricRegistry.name(
                    ICassandraManagementProxy.class, "connections", host.replace('.', 'x').replaceAll("[^A-Za-z0-9]", "")))
            .inc();
      }
    } catch (RuntimeException e) {
      LOG.warn("Could not increment successful remote management connections counter for host {}", host, e);
    }
  }

  public void decrementSuccessfulConnections(String host) {
    try {
      AtomicInteger successes = successfulConnections.putIfAbsent(host, new AtomicInteger(-1));
      if (null != successes && successes.get() >= -5) {
        successes.decrementAndGet();
      }
      LOG.debug("Host {} has {} successful remote management connections", host, successes);
      if (null != metricRegistry) {
        metricRegistry
            .counter(
                MetricRegistry.name(
                    ICassandraManagementProxy.class, "connections", host.replace('.', 'x').replaceAll("[^A-Za-z0-9]", "")))
            .dec();
      }
    } catch (RuntimeException e) {
      LOG.warn("Could not decrement successful remote management connections counter for host {}", host, e);
    }
  }

  public int getSuccessfulConnections(String host) {
    return successfulConnections.getOrDefault(host, new AtomicInteger(0)).get();
  }

}