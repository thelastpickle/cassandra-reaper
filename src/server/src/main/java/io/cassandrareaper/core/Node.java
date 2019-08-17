/*
 * Copyright 2018-2019 The Last Pickle Ltd
 *
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


import java.util.Objects;
import java.util.Optional;

import com.google.common.base.Preconditions;

public final class Node {

  private final Optional<Cluster> cluster;
  private final String hostname;

  private Node(Builder builder) {
    this.cluster = Optional.ofNullable(builder.cluster);
    this.hostname = builder.hostname;
  }

  public static Builder builder() {
    return new Builder();
  }

  public Builder with() {
    Builder builder = builder().withHostname(hostname);
    if (cluster.isPresent()) {
      builder = builder.withCluster(cluster.get());
    }
    return builder;
  }

  public String getClusterName() {
    return cluster.isPresent() ? cluster.get().getName() : "";
  }

  public String getHostname() {
    return hostname;
  }

  public int getJmxPort() {
    return cluster.isPresent() ? cluster.get().getJmxPort() : Cluster.DEFAULT_JMX_PORT;
  }

  public String toString() {
    return hostname + (cluster.isPresent() ? "@" + cluster.get().getName() : "");
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    Node node = (Node) obj;
    return Objects.equals(cluster, node.cluster) && Objects.equals(hostname, node.hostname);
  }

  @Override
  public int hashCode() {
    return Objects.hash(cluster, hostname);
  }

  public static final class Builder {
    private Cluster cluster;
    private String hostname;

    private Builder() {}

    public Builder withCluster(Cluster cluster) {
      Preconditions.checkNotNull(cluster);
      this.cluster = cluster;
      return this;
    }

    public Builder withHostname(String hostname) {
      Preconditions.checkNotNull(hostname);
      this.hostname = hostname;
      return this;
    }

    public Node build() {
      Preconditions.checkNotNull(hostname);
      return new Node(this);
    }
  }
}
