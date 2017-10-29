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

import com.google.common.base.Preconditions;

public final class NodeMetrics {

  private final String node;
  private final String cluster;
  private final String datacenter;
  private final boolean requested;
  private final int pendingCompactions;
  private final boolean hasRepairRunning;
  private final int activeAnticompactions;

  private NodeMetrics(Builder builder) {
    this.node = builder.node;
    this.cluster = builder.cluster;
    this.datacenter = builder.datacenter;
    this.requested = builder.requested;
    this.pendingCompactions = builder.pendingCompactions;
    this.hasRepairRunning = builder.hasRepairRunning;
    this.activeAnticompactions = builder.activeAnticompactions;
  }

  public String getNode() {
    return node;
  }

  public String getCluster() {
    return cluster;
  }

  public String getDatacenter() {
    return datacenter;
  }

  /** If true indicates that metrics for this node have been requested.
   *<p/>
   * A reaper instance that can jmx access the node will do so on its next heartbeat.
   */
  public boolean isRequested() {
    return requested;
  }

  public int getPendingCompactions() {
    Preconditions.checkState(!requested, "cant get metrics on requested NodeMetrics instance");
    return pendingCompactions;
  }

  public boolean hasRepairRunning() {
    Preconditions.checkState(!requested, "cant get metrics on requested NodeMetrics instance");
    return hasRepairRunning;
  }

  public int getActiveAnticompactions() {
    Preconditions.checkState(!requested, "cant get metrics on requested NodeMetrics instance" );
    return activeAnticompactions;
  }

  /**
   * Creates builder to build {@link NodeMetrics}.
   *
   * @return created builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder to build {@link NodeMetrics}.
   */
  public static final class Builder {

    private String node;
    private String cluster;
    private String datacenter;
    private boolean requested = false;
    private int pendingCompactions = 0;
    private boolean hasRepairRunning = false;
    private int activeAnticompactions = 0;

    private Builder() {
    }

    public Builder withNode(String node) {
      this.node = node;
      return this;
    }

    public Builder withCluster(String cluster) {
      this.cluster = cluster;
      return this;
    }

    public Builder withDatacenter(String datacenter) {
      this.datacenter = datacenter;
      return this;
    }

    public Builder withRequested(boolean requested) {
      this.requested = requested;
      return this;
    }

    public Builder withPendingCompactions(int pendingCompactions) {
      this.pendingCompactions = pendingCompactions;
      return this;
    }

    public Builder withHasRepairRunning(boolean hasRepairRunning) {
      this.hasRepairRunning = hasRepairRunning;
      return this;
    }

    public Builder withActiveAnticompactions(int activeAnticompactions) {
      this.activeAnticompactions = activeAnticompactions;
      return this;
    }

    public NodeMetrics build() {
      Preconditions.checkNotNull(node, "node must be set");
      Preconditions.checkNotNull(cluster, "cluster must be set");
      Preconditions.checkNotNull(datacenter, "datacenter must be set");
      if (requested) {
        Preconditions.checkState(0 == pendingCompactions, "cant set metrics on requested NodeMetrics instance");
        Preconditions.checkState(!hasRepairRunning, "cant set metrics on requested NodeMetrics instance");
        Preconditions.checkState(0 == activeAnticompactions, "cant set metrics on requested NodeMetrics instance");
      }
      return new NodeMetrics(this);
    }
  }
}
