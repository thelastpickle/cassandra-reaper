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
package com.spotify.reaper.core;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Set;

public class Cluster {

  @JsonProperty
  private final String name;

  @JsonProperty
  private final String partitioner; // Full name of the partitioner class

  @JsonProperty
  private final Set<String> seedHosts;

  public String getName() {
    return name;
  }

  public String getPartitioner() {
    return partitioner;
  }

  public Set<String> getSeedHosts() {
    return seedHosts;
  }

  private Cluster(Builder builder) {
    this.name = builder.name;
    this.partitioner = builder.partitioner;
    this.seedHosts = builder.seedHosts;
  }

  public static class Builder {

    public final String name;
    public final String partitioner;
    private final Set<String> seedHosts;

    public Builder(String name, String partitioner, Set<String> seedHosts) {
      this.name = name;
      this.partitioner = partitioner;
      this.seedHosts = seedHosts;
    }

    public Builder addSeedHost(String seedHost) {
      seedHosts.add(seedHost);
      return this;
    }

    public Cluster build() {
      return new Cluster(this);
    }
  }
}
