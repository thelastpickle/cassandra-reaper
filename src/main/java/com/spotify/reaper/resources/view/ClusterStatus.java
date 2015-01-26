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
package com.spotify.reaper.resources.view;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.reaper.core.Cluster;

import java.util.Collection;

/**
 * Contains the data to be shown when querying cluster status.
 */
public class ClusterStatus {

  @JsonProperty("cluster_name")
  private final String clusterName;

  @JsonProperty()
  private final String partitioner;

  @JsonProperty("seed_hosts")
  private final Collection<String> seedHosts;

  @JsonProperty("repair_runs")
  private Collection<Collection<Object>> repairRuns;

  @JsonProperty()
  private Collection<String> keyspaces;

  public ClusterStatus(Cluster cluster) {
    this.clusterName = cluster.getName();
    this.partitioner = cluster.getPartitioner();
    this.seedHosts = cluster.getSeedHosts();
  }

  public String getClusterName() {
    return clusterName;
  }

  public String getPartitioner() {
    return partitioner;
  }

  public Collection<String> getSeedHosts() {
    return seedHosts;
  }

  public Collection<Collection<Object>> getRepairRuns() {
    return repairRuns;
  }

  public void setRepairRunIds(Collection<Collection<Object>> repairRuns) {
    this.repairRuns = repairRuns;
  }

  public void setKeyspaces(Collection<String> keyspaces) {
    this.keyspaces = keyspaces;
  }
}
