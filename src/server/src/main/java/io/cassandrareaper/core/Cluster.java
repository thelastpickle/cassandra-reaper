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

package io.cassandrareaper.core;

import java.util.Optional;
import java.util.Set;

import com.google.common.base.Preconditions;

public final class Cluster {

  public static final int DEFAULT_JMX_PORT = 7199;
  private final String name;
  private final Optional<String> partitioner; // Full name of the partitioner class
  private final Set<String> seedHosts;
  private final ClusterProperties properties;

  public Cluster(String name, Optional<String> partitioner, Set<String> seedHosts) {
    this(
        name,
        partitioner,
        seedHosts,
        ClusterProperties.builder().withJmxPort(DEFAULT_JMX_PORT).build());
  }

  public Cluster(
      String name, Optional<String> partitioner, Set<String> seedHosts, ClusterProperties properties) {
    this.name = toSymbolicName(name);
    this.partitioner = partitioner;
    this.seedHosts = seedHosts;
    this.properties = properties;
  }

  public static String toSymbolicName(String name) {
    Preconditions.checkNotNull(name, "cannot turn null into symbolic name");
    return name.toLowerCase().replaceAll("[^a-z0-9_\\-\\.]", "");
  }

  public String getName() {
    return name;
  }

  public Optional<String> getPartitioner() {
    return partitioner;
  }

  public Set<String> getSeedHosts() {
    return seedHosts;
  }

  public ClusterProperties getProperties() {
    return properties;
  }
}
