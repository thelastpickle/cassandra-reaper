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

import java.util.UUID;

public final class NodeMetricsRequest {

  private final String hostAddress;
  private final String cluster;
  private final UUID runId;
  private final UUID segmentId;

  private NodeMetricsRequest(Builder builder) {
    this.hostAddress = builder.hostAddress;
    this.cluster = builder.cluster;
    this.runId = builder.runId;
    this.segmentId = builder.segmentId;
  }

  public String getHostAddress() {
    return hostAddress;
  }

  public String getCluster() {
    return cluster;
  }

  public UUID getRunId() {
    return runId;
  }

  public UUID getSegmentId() {
    return segmentId;
  }

  /**
   * Creates builder to build {@link NodeMetricsRequest}.
   *
   * @return created builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder to build {@link NodeMetricsRequest}.
   */
  public static final class Builder {

    private String hostAddress;
    private String cluster;
    private UUID runId;
    private UUID segmentId;

    private Builder() {
    }

    public Builder withHostAddress(String hostAddress) {
      this.hostAddress = hostAddress;
      return this;
    }

    public Builder withCluster(String cluster) {
      this.cluster = cluster;
      return this;
    }

    public Builder withRunId(UUID runId) {
      this.runId = runId;
      return this;
    }

    public Builder withSegmentId(UUID segmentId) {
      this.segmentId = segmentId;
      return this;
    }

    public NodeMetricsRequest build() {
      return new NodeMetricsRequest(this);
    }
  }
}