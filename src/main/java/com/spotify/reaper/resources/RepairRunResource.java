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

import static com.google.common.base.Preconditions.checkNotNull;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.apache.cassandra.repair.RepairParallelism;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.spotify.reaper.AppContext;
import com.spotify.reaper.ReaperApplication;
import com.spotify.reaper.ReaperException;
import com.spotify.reaper.core.Cluster;
import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.core.RepairRun.RunState;
import com.spotify.reaper.core.RepairSegment;
import com.spotify.reaper.core.RepairUnit;
import com.spotify.reaper.resources.view.RepairRunStatus;
import java.util.UUID;

@Path("/repair_run")
@Produces(MediaType.APPLICATION_JSON)
public class RepairRunResource {

  private static final Logger LOG = LoggerFactory.getLogger(RepairRunResource.class);

  private final AppContext context;

  public RepairRunResource(AppContext context) {
    this.context = context;
  }

  /**
   * Endpoint used to create a repair run. Does not allow triggering the run.
   * triggerRepairRun() must be called to initiate the repair.
   * Creating a repair run includes generating the repair segments.
   *
   * Notice that query parameter "tables" can be a single String, or a
   * comma-separated list of table names. If the "tables" parameter is omitted, and only the
   * keyspace is defined, then created repair run will target all the tables in the keyspace.
   *
   * @return repair run ID in case of everything going well,
   * and a status code 500 in case of errors.
   */
  @POST
  public Response addRepairRun(
      @Context UriInfo uriInfo,
      @QueryParam("clusterName") Optional<String> clusterName,
      @QueryParam("keyspace") Optional<String> keyspace,
      @QueryParam("tables") Optional<String> tableNamesParam,
      @QueryParam("owner") Optional<String> owner,
      @QueryParam("cause") Optional<String> cause,
      @QueryParam("segmentCount") Optional<Integer> segmentCount,
      @QueryParam("repairParallelism") Optional<String> repairParallelism,
      @QueryParam("intensity") Optional<String> intensityStr,
      @QueryParam("incrementalRepair") Optional<String> incrementalRepairStr
  ) {
    LOG.info("add repair run called with: clusterName = {}, keyspace = {}, tables = {}, owner = {},"
             + " cause = {}, segmentCount = {}, repairParallelism = {}, intensity = {}, incrementalRepair = {}",
             clusterName, keyspace, tableNamesParam, owner, cause, segmentCount, repairParallelism,
             intensityStr, incrementalRepairStr);
    try {
      Response possibleFailedResponse = RepairRunResource.checkRequestForAddRepair(
          context, clusterName, keyspace, owner, segmentCount, repairParallelism, intensityStr, incrementalRepairStr);
      if (null != possibleFailedResponse) {
        return possibleFailedResponse;
      }

      Double intensity;
      if (intensityStr.isPresent()) {
        intensity = Double.parseDouble(intensityStr.get());
      } else {
        intensity = context.config.getRepairIntensity();
        LOG.debug("no intensity given, so using default value: {}", intensity);
      }

      Boolean incrementalRepair;
      if (incrementalRepairStr.isPresent()) {
    	  incrementalRepair = Boolean.parseBoolean(incrementalRepairStr.get());
      } else {
    	  incrementalRepair = context.config.getIncrementalRepair();
        LOG.debug("no incremental repair given, so using default value: {}", incrementalRepair);
      }

      
      int segments = context.config.getSegmentCount();
      if (!incrementalRepair) {
	      if (segmentCount.isPresent()) {
	        LOG.debug("using given segment count {} instead of configured value {}",
	                  segmentCount.get(), context.config.getSegmentCount());
	        segments = segmentCount.get();
	      }
      } else {
    	  // hijack the segment count in case of incremental repair
    	  // since unit subrange incremental repairs are highly inefficient... 
    	  segments = -1;
      }
      
      
      

      Cluster cluster = context.storage.getCluster(Cluster.toSymbolicName(clusterName.get())).get();
      Set<String> tableNames;
      try {
        tableNames = CommonTools.getTableNamesBasedOnParam(context, cluster,
                                                           keyspace.get(), tableNamesParam);
      } catch (IllegalArgumentException ex) {
        LOG.error(ex.getMessage(), ex);
        return Response.status(Response.Status.NOT_FOUND).entity(ex.getMessage()).build();
      }

      RepairUnit theRepairUnit =
          CommonTools.getNewOrExistingRepairUnit(context, cluster, keyspace.get(), tableNames, incrementalRepair);

      if (theRepairUnit.getIncrementalRepair() != incrementalRepair) {
    	  return Response.status(Response.Status.BAD_REQUEST).entity(
                  "A repair run already exist for the same cluster/keyspace/table but with a different incremental repair value." 
                  + "Requested value: " + incrementalRepair + " | Existing value: " + theRepairUnit.getIncrementalRepair()).build();
      }
      
      RepairParallelism parallelism = context.config.getRepairParallelism();
      if (repairParallelism.isPresent()) {
        LOG.debug("using given repair parallelism {} instead of configured value {}",
                  repairParallelism.get(), context.config.getRepairParallelism());
        parallelism = RepairParallelism.valueOf(repairParallelism.get().toUpperCase());
      }
      
      if (incrementalRepair) {
        parallelism = RepairParallelism.PARALLEL;
      }
      
      RepairRun newRepairRun = CommonTools.registerRepairRun(
          context, cluster, theRepairUnit, cause, owner.get(), segments,
          parallelism, intensity);

      return Response.created(buildRepairRunURI(uriInfo, newRepairRun))
          .entity(new RepairRunStatus(newRepairRun, theRepairUnit, 0)).build();

    } catch (ReaperException e) {
      LOG.error(e.getMessage(), e);
      return Response.status(500).entity(e.getMessage()).build();
    }
  }

  /**
   * @return Response instance in case there is a problem, or null if everything is ok.
   */
  @Nullable
  public static Response checkRequestForAddRepair(
      AppContext context, Optional<String> clusterName, Optional<String> keyspace,
      Optional<String> owner, Optional<Integer> segmentCount,
      Optional<String> repairParallelism, Optional<String> intensityStr, Optional<String> incrementalRepairStr) {
    if (!clusterName.isPresent()) {
      return Response.status(Response.Status.BAD_REQUEST).entity(
          "missing query parameter \"clusterName\"").build();
    }
    if (!keyspace.isPresent()) {
      return Response.status(Response.Status.BAD_REQUEST).entity(
          "missing query parameter \"keyspace\"").build();
    }
    if (!owner.isPresent()) {
      return Response.status(Response.Status.BAD_REQUEST).entity(
          "missing query parameter \"owner\"").build();
    }
    if (segmentCount.isPresent() && (segmentCount.get() < 1 || segmentCount.get() > 100000)) {
      return Response.status(Response.Status.BAD_REQUEST).entity(
          "invalid query parameter \"segmentCount\", maximum value is 100000").build();
    }
    if (repairParallelism.isPresent()) {
      try {
        ReaperApplication.checkRepairParallelismString(repairParallelism.get());
      } catch (ReaperException ex) {
        LOG.error(ex.getMessage(), ex);
        return Response.status(Response.Status.BAD_REQUEST).entity(ex.getMessage()).build();
      }
    }
    if (intensityStr.isPresent()) {
      try {
        Double intensity = Double.parseDouble(intensityStr.get());
        if (intensity <= 0.0 || intensity > 1.0) {
          return Response.status(Response.Status.BAD_REQUEST).entity(
              "query parameter \"intensity\" must be in half closed range (0.0, 1.0]: "
              + intensityStr.get()).build();
        }
      } catch (NumberFormatException ex) {
        LOG.error(ex.getMessage(), ex);
        return Response.status(Response.Status.BAD_REQUEST).entity(
            "invalid value for query parameter \"intensity\": " + intensityStr.get()).build();
      }
    }
    if (incrementalRepairStr.isPresent() && (!incrementalRepairStr.get().toUpperCase().contentEquals("TRUE" ) && !incrementalRepairStr.get().toUpperCase().contentEquals("FALSE")) ) {
      return Response.status(Response.Status.BAD_REQUEST).entity(
              "invalid query parameter \"incrementalRepair\", expecting [True,False]").build();
    }
    Optional<Cluster> cluster =
        context.storage.getCluster(Cluster.toSymbolicName(clusterName.get()));
    if (!cluster.isPresent()) {
      return Response.status(Response.Status.NOT_FOUND).entity(
          "No cluster found with name \"" + clusterName.get()
          + "\", did you register your cluster first?").build();
    }
    return null;
  }

  /**
   * Modifies a state of the repair run. <p/> Currently supports NOT_STARTED|PAUSED -> RUNNING and
   * RUNNING -> PAUSED.
   *
   * @return OK if all goes well NOT_MODIFIED if new state is the same as the old one, and 501
   * (NOT_IMPLEMENTED) if transition is not supported.
   * @throws ReaperException 
   */
  @PUT
  @Path("/{id}")
  public Response modifyRunState(
      @Context UriInfo uriInfo,
      @PathParam("id") UUID repairRunId,
      @QueryParam("state") Optional<String> state) throws ReaperException {

    LOG.info("modify repair run state called with: id = {}, state = {}", repairRunId, state);

    if (!state.isPresent()) {
      return Response.status(Response.Status.BAD_REQUEST.getStatusCode())
          .entity("\"state\" argument missing").build();
    }

    Optional<RepairRun> repairRun = context.storage.getRepairRun(repairRunId);
    if (!repairRun.isPresent()) {
      return Response.status(Response.Status.NOT_FOUND).entity("repair run with id "
                                                               + repairRunId + " not found")
          .build();
    }
    
    Optional<RepairUnit> repairUnit =
        context.storage.getRepairUnit(repairRun.get().getRepairUnitId());
    if (!repairUnit.isPresent()) {
      String errMsg = "repair unit with id " + repairRun.get().getRepairUnitId() + " not found";
      LOG.error(errMsg);
      return Response.status(Response.Status.NOT_FOUND).entity(errMsg).build();
    }

    // Check that no other repair run exists with a status different than DONE for this repair unit
    Collection<RepairRun> repairRuns = context.storage.getRepairRunsForUnit(repairRun.get().getRepairUnitId());
    
    for(RepairRun run:repairRuns){
    	if(!run.getId().equals(repairRunId) && run.getRunState().equals(RunState.RUNNING)){
    		String errMsg = "repair unit already has run " + run.getId() + " in RUNNING state";
		    LOG.error(errMsg);
		    return Response.status(Response.Status.CONFLICT).entity(errMsg).build();
    	}
    }
    	    
    
    int segmentsRepaired =
        context.storage.getSegmentAmountForRepairRunWithState(repairRunId, RepairSegment.State.DONE);

    RepairRun.RunState newState;
    try {
      newState = RepairRun.RunState.valueOf(state.get().toUpperCase());
    } catch (IllegalArgumentException ex) {
      return Response.status(Response.Status.BAD_REQUEST.getStatusCode())
          .entity("invalid \"state\" argument: " + state.get()).build();
    }
    RepairRun.RunState oldState = repairRun.get().getRunState();

    if (oldState == newState) {
      return Response.ok("given \"state\" is same as the current run state").build();
    }

    if (isStarting(oldState, newState)) {
      return startRun(repairRun.get(), repairUnit.get(), segmentsRepaired);
    } else if (isPausing(oldState, newState)) {
      return pauseRun(repairRun.get(), repairUnit.get(), segmentsRepaired);
    } else if (isResuming(oldState, newState) || isRetrying(oldState, newState)) {
      return resumeRun(repairRun.get(), repairUnit.get(), segmentsRepaired);
    } else if (isAborting(oldState, newState)) {
      return abortRun(repairRun.get(), repairUnit.get(), segmentsRepaired);
    } else {
      String errMsg = String.format("Transition %s->%s not supported.", oldState.toString(),
                                    newState.toString());
      LOG.error(errMsg);
      return Response.status(Response.Status.BAD_REQUEST).entity(errMsg).build();
    }
  }

  private boolean isStarting(RepairRun.RunState oldState, RepairRun.RunState newState) {
    return oldState == RepairRun.RunState.NOT_STARTED && newState == RepairRun.RunState.RUNNING;
  }

  private boolean isPausing(RepairRun.RunState oldState, RepairRun.RunState newState) {
    return oldState == RepairRun.RunState.RUNNING && newState == RepairRun.RunState.PAUSED;
  }

  private boolean isResuming(RepairRun.RunState oldState, RepairRun.RunState newState) {
    return oldState == RepairRun.RunState.PAUSED && newState == RepairRun.RunState.RUNNING;
  }

  private boolean isRetrying(RepairRun.RunState oldState, RepairRun.RunState newState) {
    return oldState == RepairRun.RunState.ERROR && newState == RepairRun.RunState.RUNNING;
  }

  private boolean isAborting(RepairRun.RunState oldState, RepairRun.RunState newState) {
    return oldState != RepairRun.RunState.ERROR && newState == RepairRun.RunState.ABORTED;
  }

  private Response startRun(RepairRun repairRun, RepairUnit repairUnit, int segmentsRepaired) throws ReaperException {
    LOG.info("Starting run {}", repairRun.getId());
    RepairRun newRun = context.repairManager.startRepairRun(context, repairRun);
    return Response.status(Response.Status.OK).entity(
        new RepairRunStatus(newRun, repairUnit, segmentsRepaired))
        .build();
  }

  private Response pauseRun(RepairRun repairRun, RepairUnit repairUnit, int segmentsRepaired) throws ReaperException {
    LOG.info("Pausing run {}", repairRun.getId());
    RepairRun newRun = context.repairManager.pauseRepairRun(context, repairRun);
    return Response.ok().entity(new RepairRunStatus(newRun, repairUnit, segmentsRepaired)).build();
  }

  private Response resumeRun(RepairRun repairRun, RepairUnit repairUnit, int segmentsRepaired) throws ReaperException {
    LOG.info("Resuming run {}", repairRun.getId());
    RepairRun newRun = context.repairManager.startRepairRun(context, repairRun);
    return Response.ok().entity(new RepairRunStatus(newRun, repairUnit, segmentsRepaired)).build();
  }

  private Response abortRun(RepairRun repairRun, RepairUnit repairUnit, int segmentsRepaired) throws ReaperException {
    LOG.info("Aborting run {}", repairRun.getId());
    RepairRun newRun = context.repairManager.abortRepairRun(context, repairRun);
    return Response.ok().entity(new RepairRunStatus(newRun, repairUnit, segmentsRepaired)).build();
  }

  /**
   * @return detailed information about a repair run.
   */
  @GET
  @Path("/{id}")
  public Response getRepairRun(@PathParam("id") UUID repairRunId) {
    LOG.debug("get repair_run called with: id = {}", repairRunId);
    Optional<RepairRun> repairRun = context.storage.getRepairRun(repairRunId);
    if (repairRun.isPresent()) {
      return Response.ok().entity(getRepairRunStatus(repairRun.get())).build();
    } else {
      return Response.status(404).entity(
          "repair run with id " + repairRunId + " doesn't exist").build();
    }
  }

  /**
   * @return all know repair runs for a cluster.
   */
  @GET
  @Path("/cluster/{cluster_name}")
  public Response getRepairRunsForCluster(@PathParam("cluster_name") String clusterName) {
    LOG.debug("get repair run for cluster called with: cluster_name = {}", clusterName);
    Collection<RepairRun> repairRuns = context.storage.getRepairRunsForCluster(clusterName);
    Collection<RepairRunStatus> repairRunViews = new ArrayList<>();
    for (RepairRun repairRun : repairRuns) {
      repairRunViews.add(getRepairRunStatus(repairRun));
    }
    return Response.ok().entity(repairRunViews).build();
  }

  /**
   * @return only a status of a repair run, not the entire repair run info.
   */
  private RepairRunStatus getRepairRunStatus(RepairRun repairRun) {
    Optional<RepairUnit> repairUnit = context.storage.getRepairUnit(repairRun.getRepairUnitId());
    Preconditions.checkState(repairUnit.isPresent(), "no repair unit found with id: %s", repairRun.getRepairUnitId());
    int segmentsRepaired =
        context.storage.getSegmentAmountForRepairRunWithState(repairRun.getId(),
            RepairSegment.State.DONE);
    return new RepairRunStatus(repairRun, repairUnit.get(), segmentsRepaired);
  }

  /**
   * Crafts an URI used to identify given repair run.
   *
   * @return The created resource URI.
   */
  private URI buildRepairRunURI(UriInfo uriInfo, RepairRun repairRun) {
    String newRepairRunPathPart = "repair_run/" + repairRun.getId();
    URI runUri = null;
    try {
      runUri = new URL(uriInfo.getBaseUri().toURL(), newRepairRunPathPart).toURI();
    } catch (MalformedURLException | URISyntaxException e) {
      LOG.error(e.getMessage(), e);
    }
    checkNotNull(runUri, "failed to build repair run uri");
    return runUri;
  }

  /**
   * @param state comma-separated list of states to return. These states must match names of
   * {@link com.spotify.reaper.core.RepairRun.RunState}.
   * @return All repair runs in the system if the param is absent, repair runs with state included
   *   in the state parameter otherwise. If the state parameter contains non-existing run states,
   *   BAD_REQUEST response is returned.
   */
  @GET
  public Response listRepairRuns(@QueryParam("state") Optional<String> state) {
    try {
      List<RepairRunStatus> runStatuses = Lists.newArrayList();
      Set desiredStates = splitStateParam(state);
      if (desiredStates == null) {
        return Response.status(Response.Status.BAD_REQUEST).build();
      }
      Collection<RepairRun> runs;
      
      Collection<Cluster> clusters = context.storage.getClusters();
      for (Cluster cluster : clusters) {
        runs = context.storage.getRepairRunsForCluster(cluster.getName());
        runStatuses.addAll(getRunStatuses(runs, desiredStates));
      }
      
      return Response.status(Response.Status.OK).entity(runStatuses).build();
    } catch (Exception e) {
      LOG.error("Failed listing cluster statuses", e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }
  
  private List<RepairRunStatus> getRunStatuses(Collection<RepairRun> runs, Set desiredStates) throws ReaperException {
    List<RepairRunStatus> runStatuses = Lists.newArrayList();
    for (RepairRun run : runs) {
      if (!desiredStates.isEmpty() && !desiredStates.contains(run.getRunState().name())) {
        continue;
      }
      Optional<RepairUnit> runsUnit = context.storage.getRepairUnit(run.getRepairUnitId());
      if (runsUnit.isPresent()) {
        int segmentsRepaired = run.getSegmentCount();
        if (!run.getRunState().equals(RepairRun.RunState.DONE)) {
          segmentsRepaired = context.storage.getSegmentAmountForRepairRunWithState(run.getId(),
                RepairSegment.State.DONE);
        }
      
        runStatuses.add(new RepairRunStatus(run, runsUnit.get(), segmentsRepaired));
      } else {
        String errMsg =
            String.format("Found repair run %d with no associated repair unit", run.getId());
        LOG.error(errMsg);
        throw new ReaperException("Internal server error : " + errMsg);
      }
    }
    
    return runStatuses;
  }

  @VisibleForTesting
  public Set splitStateParam(Optional<String> state) {
    if (state.isPresent()) {
      Iterable<String> chunks = CommonTools.COMMA_SEPARATED_LIST_SPLITTER.split(state.get());
      for (String chunk : chunks) {
        try {
          RepairRun.RunState.valueOf(chunk.toUpperCase());
        } catch (IllegalArgumentException e) {
          LOG.warn("Listing repair runs called with erroneous states: {}", state.get(), e);
          return null;
        }
      }
      return Sets.newHashSet(chunks);
    } else {
      return Sets.newHashSet();
    }
  }

  /**
   * Delete a RepairRun object with given id.
   *
   * Repair run can be only deleted when it is not running.
   * When Repair run is deleted, all the related RepairSegment instances will be deleted also.
   *
   * @param runId  The id for the RepairRun instance to delete.
   * @param owner  The assigned owner of the deleted resource. Must match the stored one.
   * @return The deleted RepairRun instance, with state overwritten to string "DELETED".
   */
  @DELETE
  @Path("/{id}")
  public Response deleteRepairRun(@PathParam("id") UUID runId,
                                  @QueryParam("owner") Optional<String> owner) {
    LOG.info("delete repair run called with runId: {}, and owner: {}", runId, owner);
    if (!owner.isPresent()) {
      return Response.status(Response.Status.BAD_REQUEST).entity(
          "required query parameter \"owner\" is missing").build();
    }
    Optional<RepairRun> runToDelete = context.storage.getRepairRun(runId);
    if (!runToDelete.isPresent()) {
      return Response.status(Response.Status.NOT_FOUND).entity(
          "Repair run with id \"" + runId + "\" not found").build();
    }
    if (runToDelete.get().getRunState() == RepairRun.RunState.RUNNING) {
      return Response.status(Response.Status.FORBIDDEN).entity(
          "Repair run with id \"" + runId
          + "\" is currently running, and must be stopped before deleting").build();
    }
    if (!runToDelete.get().getOwner().equalsIgnoreCase(owner.get())) {
      return Response.status(Response.Status.FORBIDDEN).entity(
          "Repair run with id \"" + runId + "\" is not owned by the user you defined: "
          + owner.get()).build();
    }
    if (context.storage.getSegmentAmountForRepairRunWithState(runId, RepairSegment.State.RUNNING) > 0) {
      return Response.status(Response.Status.FORBIDDEN).entity(
          "Repair run with id \"" + runId
          + "\" has a running segment, which must be waited to finish before deleting").build();
    }
    // Need to get the RepairUnit before it's possibly deleted.
    Optional<RepairUnit> unitPossiblyDeleted =
        context.storage.getRepairUnit(runToDelete.get().getRepairUnitId());
    int segmentsRepaired =
        context.storage.getSegmentAmountForRepairRunWithState(runId, RepairSegment.State.DONE);
    Optional<RepairRun> deletedRun = context.storage.deleteRepairRun(runId);
    if (deletedRun.isPresent()) {
      RepairRunStatus repairRunStatus =
          new RepairRunStatus(deletedRun.get(), unitPossiblyDeleted.get(), segmentsRepaired);
      return Response.ok().entity(repairRunStatus).build();
    }
    return Response.serverError().entity("delete failed for repair run with id \""
                                         + runId + "\"").build();
  }

}
