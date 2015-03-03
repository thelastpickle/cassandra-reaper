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
package com.spotify.reaper.resources.view.hierarchy;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Sets;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.reaper.AppContext;
import com.spotify.reaper.core.Cluster;
import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.core.RepairSegment;
import com.spotify.reaper.core.RepairUnit;
import com.spotify.reaper.resources.ClusterResource;
import com.spotify.reaper.resources.view.RepairRunStatus;
import com.spotify.reaper.storage.MemoryStorage;

import org.apache.cassandra.repair.RepairParallelism;
import org.joda.time.DateTime;

import java.util.Collection;
import java.util.Collections;

import javax.annotation.Nullable;
import javax.ws.rs.core.Response;

import javafx.util.Pair;

public class ClusterOverview {

  public static void main(String[] args) {
    AppContext context = new AppContext();
    context.storage = new MemoryStorage();

    // Insert example data
    context.storage.addCluster(new Cluster("example", "ExamplePartitioner",
        Sets.newHashSet("host1", "host1")));
    RepairUnit unit = context.storage.addRepairUnit(new RepairUnit.Builder("example", "exampleKS",
        Sets.newHashSet("exampleCF1", "exampleCF2")));
    RepairRun run = context.storage.addRepairRun(new RepairRun.Builder("example", unit.getId(),
        DateTime.now(), 0.1337, 1, RepairParallelism.DATACENTER_AWARE));
    context.storage.addRepairSegments(
        Collections.singleton(
            new RepairSegment.Builder(run.getId(), null, unit.getId()).state(RepairSegment.State.RUNNING)),
        run.getId());

    // print the view
    ClusterResource clusterResource = new ClusterResource(context);
    Response response = clusterResource.getClusterOverview("example");
    try {
      System.out.println(new ObjectMapper().writeValueAsString(response.getEntity()));
    } catch (JsonProcessingException e) {
      e.printStackTrace();
    }
  }

  @JsonProperty("cluster_name")
  private final String clusterName;

  @JsonProperty("repair_runs")
  private final Collection<RepairRunStatus> repairRuns;

  public ClusterOverview(Cluster cluster,
      Collection<Pair<RepairRun, Pair<RepairUnit, Integer>>> repairRuns) {
    clusterName = cluster.getName();
    this.repairRuns = Collections2.transform(repairRuns,
        new Function<Pair<RepairRun, Pair<RepairUnit, Integer>>, RepairRunStatus>() {
          @Nullable
          @Override
          public RepairRunStatus apply(Pair<RepairRun, Pair<RepairUnit, Integer>> input) {
            return new RepairRunStatus(input.getKey(), input.getValue().getKey(),
                input.getValue().getValue());
          }
        });
  }

}
