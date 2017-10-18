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

public final class NodeMetrics {

  private final String hostAddress;
  private final String datacenter;
  private final int pendingCompactions;
  private final boolean hasRepairRunning;
  private final int activeAnticompactions;

  private NodeMetrics(Builder builder) {
    this.hostAddress = builder.hostAddress;
    this.datacenter = builder.datacenter;
    this.pendingCompactions = builder.pendingCompactions;
    this.hasRepairRunning = builder.hasRepairRunning;
    this.activeAnticompactions = builder.activeAnticompactions;
  }

  public String getHostAddress() {
    return hostAddress;
  }

  public String getDatacenter() {
    return datacenter;
  }

  public int getPendingCompactions() {
    return pendingCompactions;
  }

  public boolean hasRepairRunning() {
    return hasRepairRunning;
  }

  public int getActiveAnticompactions() {
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

    private String hostAddress;
    private String datacenter;
    private int pendingCompactions;
    private boolean hasRepairRunning;
    private int activeAnticompactions;

    private Builder() {
    }

    public Builder withHostAddress(String hostAddress) {
      this.hostAddress = hostAddress;
      return this;
    }

    public Builder withDatacenter(String datacenter) {
      this.datacenter = datacenter;
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
      return new NodeMetrics(this);
    }
  }
}
