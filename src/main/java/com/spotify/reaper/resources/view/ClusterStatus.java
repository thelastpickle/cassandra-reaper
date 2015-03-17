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

import java.util.Collection;

public class ClusterStatus {

  @JsonProperty
  public final String name;
  @JsonProperty("repair_runs")
  public final Collection<RepairRunStatus> repairRuns;

  public ClusterStatus(String name, Collection<RepairRunStatus> repairRuns) {
    this.name = name;
    this.repairRuns = repairRuns;
  }
}
