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

package io.cassandrareaper.core;

import java.util.Collections;

public final class Node {

  private final Cluster cluster;
  private final String hostname;

  private Node(Builder builder) {
    this.cluster = builder.cluster;
    this.hostname = builder.hostname;
  }

  public Cluster getCluster() {
    return cluster;
  }

  public String getHostname() {
    return hostname;
  }

  public String toString() {
    return hostname + "@" + cluster.getName();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private Cluster cluster;
    private String hostname;

    private Builder() {}

    public Builder withCluster(Cluster cluster) {
      this.cluster = cluster;
      return this;
    }

    public Builder withClusterName(String clusterName) {
      this.cluster = new Cluster(clusterName, null, Collections.emptySet());
      return this;
    }

    public Builder withHostname(String hostname) {
      this.hostname = hostname;
      return this;
    }

    public Node build() {
      return new Node(this);
    }
  }
}
