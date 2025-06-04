/*
 * Copyright 2014-2017 Spotify AB
 * Copyright 2016-2019 The Last Pickle Ltd
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

package io.cassandrareaper.resources.view;

import io.cassandrareaper.core.Cluster;

import java.util.Collection;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;

public final class ClusterStatus {

  @JsonProperty public final String name;

  @JsonProperty("jmx_username")
  public final String jmxUsername;

  @JsonProperty("jmx_password_is_set")
  public final Boolean jmxPasswordIsSet;

  @JsonProperty("seed_hosts")
  public final Set<String> seedHosts;

  @JsonProperty("repair_runs")
  public final Collection<RepairRunStatus> repairRuns;

  @JsonProperty("repair_schedules")
  public final Collection<RepairScheduleStatus> repairSchedules;

  @JsonProperty("nodes_status")
  public final NodesStatus nodesStatus;

  public ClusterStatus(
      Cluster cluster,
      String jmxUsername,
      Boolean jmxPasswordIsSet,
      Collection<RepairRunStatus> repairRuns,
      Collection<RepairScheduleStatus> repairSchedules,
      NodesStatus nodesStatus) {

    this.name = cluster.getName();
    this.jmxUsername = jmxUsername;
    this.jmxPasswordIsSet = jmxPasswordIsSet;
    this.seedHosts = cluster.getSeedHosts();
    this.repairRuns = repairRuns;
    this.repairSchedules = repairSchedules;
    this.nodesStatus = nodesStatus;
  }
}
