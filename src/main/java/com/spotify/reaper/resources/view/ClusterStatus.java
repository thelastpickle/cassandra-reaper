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

import jersey.repackaged.com.google.common.collect.Lists;

import java.util.Collection;
import java.util.Set;

public class ClusterStatus {

  @JsonProperty
  public final String name;
  @JsonProperty("seed_hosts")
  public final Set<String> seedHosts;
  @JsonProperty("repair_runs")
  public final Collection<RepairRunStatus> repairRuns;
  @JsonProperty("repair_schedules")
  public final Collection<RepairScheduleStatus> repairSchedules;
  @JsonProperty("nodes_status")
  public final NodesStatus nodesStatus;
  
  public ClusterStatus(Cluster cluster, Collection<RepairRunStatus> repairRuns,
      Collection<RepairScheduleStatus> repairSchedules, NodesStatus nodesStatus) {
    this.name = cluster.getName();
    this.seedHosts = cluster.getSeedHosts();
    this.repairRuns = repairRuns;
    this.repairSchedules = repairSchedules;
    this.nodesStatus = nodesStatus;
  }
}
