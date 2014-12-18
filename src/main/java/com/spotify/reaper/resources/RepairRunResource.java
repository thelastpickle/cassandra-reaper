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
package com.spotify.reaper.resources;

import com.spotify.reaper.core.ColumnFamily;
import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.core.RepairSegment;
import com.spotify.reaper.resources.view.RepairRunStatus;
import com.spotify.reaper.storage.IStorage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/repair_run")
@Produces(MediaType.APPLICATION_JSON)
public class RepairRunResource {

  private static final Logger LOG = LoggerFactory.getLogger(RepairRunResource.class);

  private final IStorage storage;

  public RepairRunResource(IStorage storage) {
    this.storage = storage;
  }

  @GET
  @Path("/{id}")
  public Response getRepairRun(@PathParam("id") Long repairRunId) {
    LOG.info("get repair_run called with: id = {}", repairRunId);
    RepairRun repairRun = storage.getRepairRun(repairRunId);
    if (null == repairRun) {
      return Response.status(404)
          .entity("repair run \"" + repairRunId + "\" does not exist").build();
    }
    return Response.ok().entity(getRepairRunStatus(repairRun)).build();
  }

  @GET
  @Path("/cluster/{cluster_name}")
  public Response getRepairRunsForCluster(@PathParam("cluster_name") String clusterName) {
    LOG.info("get repair run for cluster called with: cluster_name = {}", clusterName);
    Collection<RepairRun> repairRuns = storage.getRepairRunsForCluster(clusterName);
    Collection<RepairRunStatus> repairRunViews = new ArrayList<>();
    for (RepairRun repairRun : repairRuns) {
      repairRunViews.add(getRepairRunStatus(repairRun));
    }
    return Response.ok().entity(repairRunViews).build();
  }

  private RepairRunStatus getRepairRunStatus(RepairRun repairRun) {
    ColumnFamily columnFamily = storage.getColumnFamily(repairRun.getColumnFamilyId());
    RepairRunStatus repairRunStatus = new RepairRunStatus(repairRun, columnFamily);
    if (repairRun.getRunState() != RepairRun.RunState.NOT_STARTED) {
      int segmentsRepaired =
          storage.getSegmentAmountForRepairRun(repairRun.getId(), RepairSegment.State.DONE);
      repairRunStatus.setSegmentsRepaired(segmentsRepaired);
    }
    return repairRunStatus;
  }

  // We probably don't want to create repair runs with this resource,
  // but actually only by posting the table resource.
  // Get here is used only for providing visibility to what is going on with the run.
}
