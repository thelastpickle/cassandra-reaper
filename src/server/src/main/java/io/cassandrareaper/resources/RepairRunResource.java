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

package io.cassandrareaper.resources;

import io.cassandrareaper.AppContext;
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.Intensity;
import io.cassandrareaper.core.RepairRun;
import io.cassandrareaper.core.RepairRun.RunState;
import io.cassandrareaper.core.RepairSegment;
import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.core.Result;
import io.cassandrareaper.resources.view.RepairRunStatus;
import io.cassandrareaper.service.RepairRunService;
import io.cassandrareaper.service.RepairUnitService;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;

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

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.cassandra.repair.RepairParallelism;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

@Path("/repair_run")
@Produces(MediaType.APPLICATION_JSON)
public final class RepairRunResource {

  private static final Logger LOG = LoggerFactory.getLogger(RepairRunResource.class);

  private final AppContext context;
  private final RepairUnitService repairUnitService;
  private final RepairRunService repairRunService;

  public RepairRunResource(AppContext context) {
    this.context = context;
    this.repairUnitService = RepairUnitService.create(context);
    this.repairRunService = RepairRunService.create(context);
  }

  /**
   * Endpoint used to create a repair run. Does not allow triggering the run. triggerRepairRun()
   * must be called to initiate the repair. Creating a repair run includes generating the repair
   * segments.
   *
   * <p>Notice that query parameter "tables" can be a single String, or a comma-separated list of
   * table names. If the "tables" parameter is omitted, and only the keyspace is defined, then
   * created repair run will target all the tables in the keyspace.
   *
   * @return repair run ID in case of everything going well, and a status code 500 in case of
   *     errors.
   */
  @POST
  public Response addRepairRun(
      @Context UriInfo uriInfo,
      @QueryParam("clusterName") Optional<String> clusterName,
      @QueryParam("keyspace") Optional<String> keyspace,
      @QueryParam("tables") Optional<String> tableNamesParam,
      @QueryParam("owner") Optional<String> owner,
      @QueryParam("cause") Optional<String> cause,
      @QueryParam("segmentCount") Optional<Integer> segmentCountPerNode,
      @QueryParam("repairParallelism") Optional<String> repairParallelism,
      @QueryParam("intensity") Optional<String> intensityStr,
      @QueryParam("incrementalRepair") Optional<String> incrementalRepairStr,
      @QueryParam("nodes") Optional<String> nodesToRepairParam,
      @QueryParam("datacenters") Optional<String> datacentersToRepairParam,
      @QueryParam("blacklistedTables") Optional<String> blacklistedTableNamesParam) {

    try {
      final Response possibleFailedResponse = RepairRunResource.checkRequestForAddRepair(
          context,
          clusterName,
          keyspace,
          owner,
          segmentCountPerNode,
          repairParallelism,
          intensityStr,
          incrementalRepairStr,
          nodesToRepairParam,
          datacentersToRepairParam);
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

      int segments = context.config.getSegmentCountPerNode();
      if (!incrementalRepair) {
        if (segmentCountPerNode.isPresent()) {
          LOG.debug(
              "using given segment count {} instead of configured value {}",
              segmentCountPerNode.get(),
              context.config.getSegmentCount());
          segments = segmentCountPerNode.get();
        }
      } else {
        // hijack the segment count in case of incremental repair
        segments = -1;
      }

      final Cluster cluster = context.storage.getCluster(Cluster.toSymbolicName(clusterName.get())).get();
      Set<String> tableNames;
      try {
        tableNames = repairRunService.getTableNamesBasedOnParam(cluster, keyspace.get(), tableNamesParam);
      } catch (IllegalArgumentException ex) {
        LOG.error(ex.getMessage(), ex);
        return Response.status(Response.Status.NOT_FOUND).entity(ex.getMessage()).build();
      }

      Set<String> blacklistedTableNames;
      try {
        blacklistedTableNames
            = repairRunService.getTableNamesBasedOnParam(cluster, keyspace.get(), blacklistedTableNamesParam);
      } catch (IllegalArgumentException ex) {
        LOG.error(ex.getMessage(), ex);
        return Response.status(Response.Status.NOT_FOUND).entity(ex.getMessage()).build();
      }

      final Set<String> nodesToRepair;
      try {
        nodesToRepair = repairRunService.getNodesToRepairBasedOnParam(cluster, nodesToRepairParam);
      } catch (IllegalArgumentException ex) {
        LOG.error(ex.getMessage(), ex);
        return Response.status(Response.Status.NOT_FOUND).entity(ex.getMessage()).build();
      }

      final Set<String> datacentersToRepair;
      try {
        datacentersToRepair = RepairRunService
            .getDatacentersToRepairBasedOnParam(cluster, datacentersToRepairParam);

      } catch (IllegalArgumentException ex) {
        LOG.error(ex.getMessage(), ex);
        return Response.status(Response.Status.NOT_FOUND).entity(ex.getMessage()).build();
      }

      final RepairUnit theRepairUnit =
          repairUnitService.getNewOrExistingRepairUnit(
              cluster,
              keyspace.get(),
              tableNames,
              incrementalRepair,
              nodesToRepair,
              datacentersToRepair,
              blacklistedTableNames);

      if (theRepairUnit.getIncrementalRepair().booleanValue() != incrementalRepair) {
        return Response.status(Response.Status.BAD_REQUEST)
            .entity(
                "A repair run already exist for the same cluster/keyspace/table"
                + " but with a different incremental repair value. Requested value: "
                + incrementalRepair
                + " | Existing value: "
                + theRepairUnit.getIncrementalRepair())
            .build();
      }

      RepairParallelism parallelism = context.config.getRepairParallelism();
      if (repairParallelism.isPresent()) {
        LOG.debug(
            "using given repair parallelism {} instead of configured value {}",
            repairParallelism.get(),
            context.config.getRepairParallelism());

        parallelism = RepairParallelism.valueOf(repairParallelism.get().toUpperCase());
      }

      if (incrementalRepair) {
        parallelism = RepairParallelism.PARALLEL;
      }

      final RepairRun newRepairRun =
          repairRunService.registerRepairRun(
              cluster,
              theRepairUnit,
              cause,
              owner.get(),
              0,
              segments,
              parallelism,
              intensity);

      return Response.created(buildRepairRunUri(uriInfo, newRepairRun))
          .entity(new RepairRunStatus(newRepairRun, theRepairUnit, 0))
          .build();

    } catch (ReaperException e) {
      LOG.error(e.getMessage(), e);
      return Response.status(500).entity(e.getMessage()).build();
    }
  }

  /**
   * @return Response instance in case there is a problem, or null if everything is ok.
   */
  @Nullable
  static Response checkRequestForAddRepair(
      AppContext context,
      Optional<String> clusterName,
      Optional<String> keyspace,
      Optional<String> owner,
      Optional<Integer> segmentCountPerNode,
      Optional<String> repairParallelism,
      Optional<String> intensityStr,
      Optional<String> incrementalRepairStr,
      Optional<String> nodesStr,
      Optional<String> datacentersStr) {

    if (!clusterName.isPresent()) {
      return Response.status(Response.Status.BAD_REQUEST).entity("missing query parameter \"clusterName\"").build();
    }
    if (!keyspace.isPresent()) {
      return Response.status(Response.Status.BAD_REQUEST).entity("missing query parameter \"keyspace\"").build();
    }
    if (!owner.isPresent()) {
      return Response.status(Response.Status.BAD_REQUEST).entity("missing query parameter \"owner\"").build();
    }
    if (segmentCountPerNode.isPresent()
        && (segmentCountPerNode.get() < 0 || segmentCountPerNode.get() > 1000)) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity("invalid query parameter \"segmentCountPerNode\", maximum value is 100000")
          .build();
    }
    if (repairParallelism.isPresent()) {
      try {
        checkRepairParallelismString(repairParallelism.get());
      } catch (ReaperException ex) {
        LOG.error(ex.getMessage(), ex);
        return Response.status(Response.Status.BAD_REQUEST).entity(ex.getMessage()).build();
      }
    }

    if (intensityStr.isPresent()) {
      final Result<Intensity, String> intensityCreateResult = Intensity.tryCreate(intensityStr.get());
      if (intensityCreateResult.isFailed()) {
        return Response
            .status(Response.Status.BAD_REQUEST)
            .entity(intensityCreateResult.getFailure())
            .build();
      }
    }

    if (incrementalRepairStr.isPresent()
        && (!incrementalRepairStr.get().toUpperCase().contentEquals("TRUE")
        && !incrementalRepairStr.get().toUpperCase().contentEquals("FALSE"))) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity("invalid query parameter \"incrementalRepair\", expecting [True,False]")
          .build();
    }
    final Optional<Cluster> cluster = context.storage.getCluster(Cluster.toSymbolicName(clusterName.get()));
    if (!cluster.isPresent()) {
      return Response.status(Response.Status.NOT_FOUND)
          .entity("No cluster found with name \"" + clusterName.get() + "\", did you register your cluster first?")
          .build();
    }

    if (!datacentersStr.or("").isEmpty() && !nodesStr.or("").isEmpty()) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(
              "Parameters \"datacenters\" and \"nodes\" are mutually exclusive. Please fill just one between the two.")
          .build();
    }

    return null;
  }

  /**
   * Modifies a state of the repair run.
   *
   * <p>
   * Currently supports NOT_STARTED|PAUSED -> RUNNING and RUNNING -> PAUSED.
   *
   * @return OK if all goes well NOT_MODIFIED if new state is the same as the old one, and 501 (NOT_IMPLEMENTED) if
   *        transition is not supported.
   */
  @PUT
  @Path("/{id}/state/{state}")
  public Response modifyRunState(
      @Context UriInfo uriInfo,
      @PathParam("id") UUID repairRunId,
      @PathParam("state") Optional<String> stateStr)
      throws ReaperException {

    LOG.info("modify repair run state called with: id = {}, state = {}", repairRunId, stateStr);

    final Result<String, Response> validateStateExistResult = validateStateExist(stateStr);
    if (validateStateExistResult.isFailed()) {
      return validateStateExistResult.getFailure();
    }

    final Result<RepairRun, Response> repairRunGetResult = tryGetRepairRun(repairRunId);
    if (repairRunGetResult.isFailed()) {
      return repairRunGetResult.getFailure();
    }

    final RepairRun repairRun = repairRunGetResult.getSuccess();

    final Result<RepairUnit, Response> repairUnitGetResult = tryGetRepairUnit(repairRun);
    if (repairRunGetResult.isFailed()) {
      return repairRunGetResult.getFailure();
    }

    final Result<?, Response> validateRepairRunsResult = validateRepairRuns(repairRun);
    if (validateRepairRunsResult.isFailed()) {
      return validateRepairRunsResult.getFailure();
    }

    final String state = validateStateExistResult.getSuccess();
    final Result<RunState, Response> validateStateResult = validateState(state);
    if (validateStateResult.isFailed()) {
      return validateStateResult.getFailure();
    }

    final RunState oldState = repairRun.getRunState();
    final RunState newState = validateStateResult.getSuccess();

    if (oldState == newState) {
      return Response
          .status(Response.Status.NOT_MODIFIED)
          .entity("given \"state\" is same as the current run state")
          .build();
    }

    final int segmentsRepaired = getSegmentAmountForRepairRun(repairRunId);
    final RepairUnit repairUnit = repairUnitGetResult.getSuccess();

    if (isStarting(oldState, newState)) {
      return startRun(repairRun, repairUnit, segmentsRepaired);
    } else if (isPausing(oldState, newState)) {
      return pauseRun(repairRun, repairUnit, segmentsRepaired);
    } else if (isResuming(oldState, newState) || isRetrying(oldState, newState)) {
      return resumeRun(repairRun, repairUnit, segmentsRepaired);
    } else if (isAborting(oldState, newState)) {
      return abortRun(repairRun, repairUnit, segmentsRepaired);
    } else {
      final String errMsg = String.format("Transition %s->%s not supported.", oldState.toString(), newState.toString());
      LOG.error(errMsg);
      return Response.status(Response.Status.METHOD_NOT_ALLOWED).entity(errMsg).build();
    }
  }

  /**
   * Check that no other repair run exists with a status different than DONE for this repair uni
   */
  @Nullable
  private Result<?, Response> validateRepairRuns(RepairRun repairRun) {
    final UUID repairRunId = repairRun.getId();
    final Collection<RepairRun> repairRuns = context.storage.getRepairRunsForUnit(repairRun.getRepairUnitId());

    for (final RepairRun run : repairRuns) {

      if (!run.getId().equals(repairRunId) && run.getRunState().equals(RunState.RUNNING)) {
        final String errMsg = "repair unit already has run " + run.getId() + " in RUNNING state";
        LOG.error(errMsg);
        final Response response = Response
            .status(Response.Status.CONFLICT)
            .entity(errMsg)
            .build();

        return Result.fail(response);
      }
    }

    return Result.success();
  }

  private int getSegmentAmountForRepairRun(UUID repairRunId) {
    return context.storage.getSegmentAmountForRepairRunWithState(repairRunId, RepairSegment.State.DONE);
  }

  /**
   * Modifies a state of the repair run.
   *
   * <p>
   * Currently supports NOT_STARTED|PAUSED -> RUNNING and RUNNING -> PAUSED.
   *
   * @return OK if all goes well NOT_MODIFIED if new state is the same as the old one, and 501 (NOT_IMPLEMENTED) if
   *        transition is not supported.
   */
  @PUT
  @Path("/{id}/intensity/{intensity}")
  public Response modifyRunIntensity(
      @Context UriInfo uriInfo,
      @PathParam("id") UUID repairRunId,
      @PathParam("intensity") Optional<String> intensityStr)
      throws ReaperException {

    final Result<String, Response> validateIntensityExistResult = validateIntensityExist(intensityStr);
    if (validateIntensityExistResult.isFailed()) {
      return validateIntensityExistResult.getFailure();
    }

    final String intensityValue = validateIntensityExistResult.getSuccess();
    final Result<Intensity, String> intensityCreateResult = Intensity.tryCreate(intensityValue);
    if (intensityCreateResult.isFailed()) {
      return Response
          .status(Response.Status.BAD_REQUEST)
          .entity(intensityCreateResult.getFailure())
          .build();
    }

    final Result<RepairRun, Response> repairRunGetResult = tryGetRepairRun(repairRunId);
    if (repairRunGetResult.isFailed()) {
      return repairRunGetResult.getFailure();
    }

    final RepairRun repairRun = repairRunGetResult.getSuccess();

    final Result<?, Response> validateRepairRunsResult = validateRepairRuns(repairRun);
    if (validateRepairRunsResult.isFailed()) {
      return validateRepairRunsResult.getFailure();
    }

    final Result<RepairUnit, Response> repairUnitGetResult = tryGetRepairUnit(repairRun);
    if (repairRunGetResult.isFailed()) {
      return repairRunGetResult.getFailure();
    }

    if (repairRun.getRunState() != RunState.PAUSED) {
      return Response
          .status(Response.Status.CONFLICT)
          .entity("intensity can be modified when repair run already paused")
          .build();
    }

    final Intensity intensity = intensityCreateResult.getSuccess();
    final int segmentsRepaired = getSegmentAmountForRepairRun(repairRunId);
    final RepairUnit repairUnit = repairUnitGetResult.getSuccess();

    return updateRepairRunIntensity(repairRun, repairUnit, segmentsRepaired, intensity.get());
  }

  /**
   * MOVED_PERMANENTLY to PUT repair_run/{id}/state/{state}
   */
  @PUT
  @Path("/{id}")
  @Deprecated
  public Response oldModifyRunState(
      @Context UriInfo uriInfo,
      @PathParam("id") UUID repairRunId,
      @QueryParam("state") Optional<String> stateStr)
      throws ReaperException {

    final Result<String, Response> validateStateExistResult = validateStateExist(stateStr);
    if (validateStateExistResult.isFailed()) {
      return validateStateExistResult.getFailure();
    }

    final Result<RepairRun, Response> repairRunGetResult = tryGetRepairRun(repairRunId);
    if (repairRunGetResult.isFailed()) {
      return repairRunGetResult.getFailure();
    }

    final RepairRun repairRun = repairRunGetResult.getSuccess();
    final String state = validateStateExistResult.getSuccess();

    final URI redirectUri = uriInfo
        .getRequestUriBuilder()
        .replacePath(String.format("repair_run/%s/state/%s", repairRun.getId().toString(), state))
        .replaceQuery("")
        .build();

    return Response.seeOther(redirectUri).build();
  }

  private Result<String, Response> validateStateExist(Optional<String> state) {
    return validateValueExist(state, "state");
  }

  private Result<String, Response> validateIntensityExist(Optional<String> intensity) {
    return validateValueExist(intensity, "intensity");
  }

  private Result<String, Response> validateValueExist(Optional<String> value, String argumentName) {
    if (value.isPresent()) {
      return Result.success(value.get());
    }

    final Response response = Response
        .status(Response.Status.BAD_REQUEST.getStatusCode())
        .entity(String.format("\"%s\" argument missing", argumentName))
        .build();
    return Result.fail(response);
  }

  private Result<RunState, Response> validateState(String stateStr) {
    try {
      final RunState state = RunState.valueOf(stateStr.toUpperCase());
      return Result.success(state);
    } catch (IllegalArgumentException ex) {
      final Response response = Response.status(Response.Status.BAD_REQUEST.getStatusCode())
          .entity("invalid \"state\" argument: " + stateStr)
          .build();
      return Result.fail(response);
    }
  }

  private Result<RepairRun, Response> tryGetRepairRun(UUID repairRunId) {
    final Optional<RepairRun> repairRun = context.storage.getRepairRun(repairRunId);

    if (repairRun.isPresent()) {
      return Result.success(repairRun.get());
    }

    final Response response = Response
        .status(Response.Status.NOT_FOUND)
        .entity("repair run with id " + repairRunId + " doesn't exist")
        .build();
    return Result.fail(response);
  }

  private Result<RepairUnit, Response> tryGetRepairUnit(RepairRun repairRun) {
    final Optional<RepairUnit> repairUnit = context.storage.getRepairUnit(repairRun.getRepairUnitId());
    if (repairUnit.isPresent()) {
      return Result.success(repairUnit.get());
    }

    final String errMsg = "repair unit with id " + repairRun.getRepairUnitId() + " not found";
    LOG.error(errMsg);
    final Response response = Response
        .status(Response.Status.NOT_FOUND)
        .entity(errMsg)
        .build();

    return Result.fail(response);

  }

  private static boolean isStarting(RepairRun.RunState oldState, RepairRun.RunState newState) {
    return oldState == RepairRun.RunState.NOT_STARTED && newState == RepairRun.RunState.RUNNING;
  }

  private static boolean isPausing(RepairRun.RunState oldState, RepairRun.RunState newState) {
    return oldState == RepairRun.RunState.RUNNING && newState == RepairRun.RunState.PAUSED;
  }

  private static boolean isResuming(RepairRun.RunState oldState, RepairRun.RunState newState) {
    return oldState == RepairRun.RunState.PAUSED && newState == RepairRun.RunState.RUNNING;
  }

  private static boolean isRetrying(RepairRun.RunState oldState, RepairRun.RunState newState) {
    return oldState == RepairRun.RunState.ERROR && newState == RepairRun.RunState.RUNNING;
  }

  private static boolean isAborting(RepairRun.RunState oldState, RepairRun.RunState newState) {
    return oldState != RepairRun.RunState.ERROR && newState == RepairRun.RunState.ABORTED;
  }

  private Response startRun(RepairRun repairRun, RepairUnit repairUnit, int segmentsRepaired) throws ReaperException {
    LOG.info("Starting run {}", repairRun.getId());
    final RepairRun newRun = context.repairManager.startRepairRun(repairRun);
    return Response.status(Response.Status.OK)
        .entity(new RepairRunStatus(newRun, repairUnit, segmentsRepaired))
        .build();
  }

  private Response pauseRun(RepairRun repairRun, RepairUnit repairUnit, int segmentsRepaired) throws ReaperException {
    LOG.info("Pausing run {}", repairRun.getId());
    final RepairRun newRun = context.repairManager.pauseRepairRun(repairRun);
    return Response.ok().entity(new RepairRunStatus(newRun, repairUnit, segmentsRepaired)).build();
  }

  private Response resumeRun(RepairRun repairRun, RepairUnit repairUnit, int segmentsRepaired) throws ReaperException {
    LOG.info("Resuming run {}", repairRun.getId());
    final RepairRun newRun = context.repairManager.startRepairRun(repairRun);
    return Response.ok().entity(new RepairRunStatus(newRun, repairUnit, segmentsRepaired)).build();
  }

  private Response abortRun(RepairRun repairRun, RepairUnit repairUnit, int segmentsRepaired) throws ReaperException {
    LOG.info("Aborting run {}", repairRun.getId());
    final RepairRun newRun = context.repairManager.abortRepairRun(repairRun);
    return Response.ok().entity(new RepairRunStatus(newRun, repairUnit, segmentsRepaired)).build();
  }

  private Response updateRepairRunIntensity(
          RepairRun repairRun,
          RepairUnit repairUnit,
          int segmentsRepaired,
          double intensity) throws ReaperException {
    LOG.info("Editing run {}", repairRun.getId());
    final RepairRun newRun = context.repairManager.updateRepairRunIntensity(repairRun, intensity);
    return Response.ok().entity(new RepairRunStatus(newRun, repairUnit, segmentsRepaired)).build();
  }

  /**
   * @return detailed information about a repair run.
   */
  @GET
  @Path("/{id}")
  public Response getRepairRun(
      @PathParam("id") UUID repairRunId) {

    LOG.debug("get repair_run called with: id = {}", repairRunId);

    final Result<RepairRun, Response> repairRunGetResult = tryGetRepairRun(repairRunId);
    if (repairRunGetResult.isFailed()) {
      return repairRunGetResult.getFailure();
    }

    final RepairRun repairRun = repairRunGetResult.getSuccess();
    return Response
        .ok()
        .entity(getRepairRunStatus(repairRun))
        .build();
  }

  /**
   * @return all know repair runs for a cluster.
   */
  @GET
  @Path("/cluster/{cluster_name}")
  public Response getRepairRunsForCluster(
      @PathParam("cluster_name") String clusterName) {

    LOG.debug("get repair run for cluster called with: cluster_name = {}", clusterName);
    final Collection<RepairRun> repairRuns = context.storage.getRepairRunsForCluster(clusterName);
    final Collection<RepairRunStatus> repairRunViews = new ArrayList<>();
    for (final RepairRun repairRun : repairRuns) {
      repairRunViews.add(getRepairRunStatus(repairRun));
    }
    return Response.ok().entity(repairRunViews).build();
  }

  /**
   * @return only a status of a repair run, not the entire repair run info.
   */
  private RepairRunStatus getRepairRunStatus(RepairRun repairRun) {
    final Optional<RepairUnit> repairUnit = context.storage.getRepairUnit(repairRun.getRepairUnitId());
    Preconditions.checkState(repairUnit.isPresent(), "no repair unit found with id: %s", repairRun.getRepairUnitId());
    final int segmentsRepaired
        = getSegmentAmountForRepairRun(repairRun.getId());
    return new RepairRunStatus(repairRun, repairUnit.get(), segmentsRepaired);
  }

  /**
   * Crafts an URI used to identify given repair run.
   *
   * @return The created resource URI.
   */
  private URI buildRepairRunUri(UriInfo uriInfo, RepairRun repairRun) {
    final String newRepairRunPathPart = "repair_run/" + repairRun.getId();
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
   * @param state comma-separated list of states to return. These states must match names of {@link
   *     io.cassandrareaper.core.RepairRun.RunState}.
   * @return All repair runs in the system if the param is absent, repair runs with state included in the state
   *       parameter otherwise.
   *        If the state parameter contains non-existing run states, BAD_REQUEST response is returned.
   */
  @GET
  public Response listRepairRuns(
      @QueryParam("state") Optional<String> state) {

    try {
      final List<RepairRunStatus> runStatuses = Lists.newArrayList();
      final Set desiredStates = splitStateParam(state);
      if (desiredStates == null) {
        return Response.status(Response.Status.BAD_REQUEST).build();
      }
      Collection<RepairRun> runs;

      final Collection<Cluster> clusters = context.storage.getClusters();
      for (final Cluster cluster : clusters) {
        runs = context.storage.getRepairRunsForCluster(cluster.getName());
        runStatuses.addAll(getRunStatuses(runs, desiredStates));
      }

      return Response.status(Response.Status.OK).entity(runStatuses).build();
    } catch (ReaperException e) {
      LOG.error("Failed listing cluster statuses", e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  private List<RepairRunStatus> getRunStatuses(Collection<RepairRun> runs, Set desiredStates) throws ReaperException {
    final List<RepairRunStatus> runStatuses = Lists.newArrayList();
    for (final RepairRun run : runs) {
      if (!desiredStates.isEmpty() && !desiredStates.contains(run.getRunState().name())) {
        continue;
      }
      final Optional<RepairUnit> runsUnit = context.storage.getRepairUnit(run.getRepairUnitId());
      if (runsUnit.isPresent()) {
        int segmentsRepaired = run.getSegmentCount();
        if (!run.getRunState().equals(RepairRun.RunState.DONE)) {
          segmentsRepaired
              = getSegmentAmountForRepairRun(run.getId());
        }

        runStatuses.add(new RepairRunStatus(run, runsUnit.get(), segmentsRepaired));
      } else {
        final String errMsg = String.format("Found repair run %s with no associated repair unit", run.getId());
        LOG.error(errMsg);
        throw new ReaperException("Internal server error : " + errMsg);
      }
    }

    return runStatuses;
  }

  static Set splitStateParam(Optional<String> state) {
    if (state.isPresent()) {
      final Iterable<String> chunks = RepairRunService.COMMA_SEPARATED_LIST_SPLITTER.split(state.get());
      for (final String chunk : chunks) {
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
   * <p>
   * Repair run can be only deleted when it is not running. When Repair run is deleted, all the related RepairSegmen
   * instances will be deleted also.
   *
   * @param runId The id for the RepairRun instance to delete.
   * @param owner The assigned owner of the deleted resource. Must match the stored one.
   * @return The deleted RepairRun instance, with state overwritten to string "DELETED".
   */
  @DELETE
  @Path("/{id}")
  public Response deleteRepairRun(
      @PathParam("id") UUID runId,
      @QueryParam("owner") Optional<String> owner) {

    LOG.info("delete repair run called with runId: {}, and owner: {}", runId, owner);
    if (!owner.isPresent()) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity("required query parameter \"owner\" is missing")
          .build();
    }
    final Optional<RepairRun> runToDelete = context.storage.getRepairRun(runId);
    if (runToDelete.isPresent()) {
      if (runToDelete.get().getRunState() == RepairRun.RunState.RUNNING) {
        return Response.status(Response.Status.FORBIDDEN)
            .entity("Repair run with id \"" + runId + "\" is currently running, and must be stopped before deleting")
            .build();
      }
      if (!runToDelete.get().getOwner().equalsIgnoreCase(owner.get())) {
        return Response.status(Response.Status.FORBIDDEN)
            .entity("Repair run with id \"" + runId + "\" is not owned by the user you defined: " + owner.get())
            .build();
      }
      if (context.storage.getSegmentAmountForRepairRunWithState(runId, RepairSegment.State.RUNNING) > 0) {
        return Response.status(Response.Status.FORBIDDEN)
            .entity(
                "Repair run with id \""
                + runId
                + "\" has a running segment, which must be waited to finish before deleting")
            .build();
      }
      // Need to get the RepairUnit before it's possibly deleted.
      final Optional<RepairUnit> unitPossiblyDeleted
          = context.storage.getRepairUnit(runToDelete.get().getRepairUnitId());

      final int segmentsRepaired
          = getSegmentAmountForRepairRun(runId);

      final Optional<RepairRun> deletedRun = context.storage.deleteRepairRun(runId);
      if (deletedRun.isPresent()) {
        final RepairRunStatus repairRunStatus
            = new RepairRunStatus(deletedRun.get(), unitPossiblyDeleted.get(), segmentsRepaired);

        return Response.ok().entity(repairRunStatus).build();
      }
    }
    try {
      // safety clean, in case of zombie segments
      context.storage.deleteRepairRun(runId);
    } catch (RuntimeException ignore) { }
    return Response.status(Response.Status.NOT_FOUND).entity("Repair run with id \"" + runId + "\" not found").build();
  }

  private static void checkRepairParallelismString(String repairParallelism) throws ReaperException {
    try {
      RepairParallelism.valueOf(repairParallelism.toUpperCase());
    } catch (IllegalArgumentException ex) {
      throw new ReaperException(
          "invalid repair parallelism given \""
          + repairParallelism
          + "\", must be one of: "
          + Arrays.toString(RepairParallelism.values()),
          ex);
    }
  }
}
