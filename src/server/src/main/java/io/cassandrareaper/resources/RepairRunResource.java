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

package io.cassandrareaper.resources;

import io.cassandrareaper.AppContext;
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.RepairRun;
import io.cassandrareaper.core.RepairRun.RunState;
import io.cassandrareaper.core.RepairSegment;
import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.management.ClusterFacade;
import io.cassandrareaper.resources.view.RepairRunStatus;
import io.cassandrareaper.service.PurgeService;
import io.cassandrareaper.service.RepairRunService;
import io.cassandrareaper.service.RepairUnitService;
import io.cassandrareaper.storage.repairrun.IRepairRunDao;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.validation.ValidationException;
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
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.cassandra.repair.RepairParallelism;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Math.min;


@Path("/repair_run")
@Produces(MediaType.APPLICATION_JSON)
public final class RepairRunResource {

  private static final Logger LOG = LoggerFactory.getLogger(RepairRunResource.class);

  private final AppContext context;
  private final RepairUnitService repairUnitService;
  private final RepairRunService repairRunService;

  private final IRepairRunDao repairRunDao;

  public RepairRunResource(AppContext context, IRepairRunDao repairRunDao) {
    this.context = context;
    this.repairUnitService = RepairUnitService.create(context);
    this.repairRunService = RepairRunService.create(context, repairRunDao);
    this.repairRunDao = repairRunDao;
  }

  /**
   * @return Response instance in case there is a problem, or null if everything is ok.
   * @throws ReaperException any caught runtime exception that can be re-thrown
   */
  @Nullable
  static Response checkRequestForAddRepair(
      AppContext context,
      Optional<String> clusterName,
      Optional<String> keyspace,
      Optional<String> tableNamesParam,
      Optional<String> owner,
      Optional<Integer> segmentCountPerNode,
      Optional<String> repairParallelism,
      Optional<String> intensityStr,
      Optional<String> incrementalRepairStr,
      Optional<String> nodesStr,
      Optional<String> datacentersStr,
      Optional<String> blacklistedTableNamesParam,
      Optional<Integer> repairThreadCountStr,
      Optional<String> forceParam,
      Optional<Integer> timeoutParam) throws ReaperException {

    if (!clusterName.isPresent()) {
      return createMissingArgumentResponse("clusterName");
    }
    if (!keyspace.isPresent()) {
      return createMissingArgumentResponse("keyspace");
    }
    if (!owner.isPresent()) {
      return createMissingArgumentResponse("owner");
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
      try {
        // @todo all BAD_REQUEST responses should be instead thrown ValidationExceptions, so this method returns void
        parseIntensity(intensityStr.get());
      } catch (ValidationException ex) {
        return Response.status(Status.BAD_REQUEST).entity(ex.getMessage()).build();
      }
    }

    if (incrementalRepairStr.isPresent()
        && (!incrementalRepairStr.get().toUpperCase().contentEquals("TRUE")
        && !incrementalRepairStr.get().toUpperCase().contentEquals("FALSE"))) {

      return Response.status(Response.Status.BAD_REQUEST)
          .entity("invalid query parameter \"incrementalRepair\", expecting [True,False]")
          .build();
    }
    try {
      Cluster cluster = context.storage.getClusterDao().getCluster(Cluster.toSymbolicName(clusterName.get()));

      if (!datacentersStr.orElse("").isEmpty() && !nodesStr.orElse("").isEmpty()) {
        return Response.status(Response.Status.BAD_REQUEST)
            .entity("Parameters \"datacenters\" and \"nodes\" are mutually exclusive.")
            .build();
      }

      if (incrementalRepairStr.isPresent() && "true".equalsIgnoreCase(incrementalRepairStr.get())) {
        try {
          String version = ClusterFacade.create(context).getCassandraVersion(cluster);
          if (null != version && version.startsWith("2.0")) {
            String msg = "Incremental repair does not work with Cassandra versions before 2.1";
            return Response.status(Response.Status.BAD_REQUEST).entity(msg).build();
          }
        } catch (ReaperException e) {
          String msg = String.format("find version of cluster %s failed", cluster.getName());
          LOG.error(msg, e);
          return Response.serverError().entity(msg).build();
        }
      }
    } catch (IllegalArgumentException ex) {
      return Response.status(Response.Status.NOT_FOUND)
          .entity("No cluster found with name \"" + clusterName.get() + "\"")
          .build();
    }

    if (tableNamesParam.isPresent() && blacklistedTableNamesParam.isPresent()) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity("invalid to specify a table list and a blacklist")
          .build();
    }

    if (forceParam.isPresent()
        && (!forceParam.get().toUpperCase().contentEquals("TRUE")
        && !forceParam.get().toUpperCase().contentEquals("FALSE"))) {

      return Response.status(Response.Status.BAD_REQUEST)
          .entity("invalid query parameter \"force\", expecting [True,False]")
          .build();
    }

    if (timeoutParam.isPresent()
        && timeoutParam.get() == 0) {

      return Response.status(Response.Status.BAD_REQUEST)
          .entity("invalid query parameter \"timeout\", should be higher than 0")
          .build();
    }

    return null;
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

  /**
   * Crafts an URI used to identify given repair run.
   *
   * @return The created resource URI.
   */
  private static URI buildRepairRunUri(UriInfo uriInfo, RepairRun repairRun) {
    return uriInfo.getBaseUriBuilder().path("repair_run").path(repairRun.getId().toString()).build();
  }

  static Set<String> splitStateParam(Optional<String> state) {
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

  private static Response createMissingArgumentResponse(String argumentName) {
    return Response.status(Status.BAD_REQUEST).entity(argumentName + " argument missing").build();
  }

  private static RunState parseRunState(String input) throws ValidationException {
    try {
      return RunState.valueOf(input.toUpperCase());
    } catch (IllegalArgumentException ex) {
      throw new ValidationException("invalid \"state\" argument: " + input, ex);
    }
  }

  private static double parseIntensity(String input) throws ValidationException {
    try {
      double intensity = Double.parseDouble(input);
      if (intensity <= 0.0 || intensity > 1.0) {
        throw new ValidationException("query parameter \"intensity\" must be in range (0.0, 1.0]: " + input);
      }
      return intensity;
    } catch (NumberFormatException ex) {
      throw new ValidationException("invalid value for query parameter \"intensity\": " + input, ex);
    }
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
      @QueryParam("segmentCountPerNode") Optional<Integer> segmentCountPerNode,
      @QueryParam("repairParallelism") Optional<String> repairParallelism,
      @QueryParam("intensity") Optional<String> intensityStr,
      @QueryParam("incrementalRepair") Optional<String> incrementalRepairStr,
      @QueryParam("nodes") Optional<String> nodesToRepairParam,
      @QueryParam("datacenters") Optional<String> datacentersToRepairParam,
      @QueryParam("blacklistedTables") Optional<String> blacklistedTableNamesParam,
      @QueryParam("repairThreadCount") Optional<Integer> repairThreadCountParam,
      @QueryParam("force") Optional<String> forceParam,
      @QueryParam("timeout") Optional<Integer> timeoutParam) {

    try {
      final Response possibleFailedResponse
          = RepairRunResource.checkRequestForAddRepair(
          context,
          clusterName,
          keyspace,
          tableNamesParam,
          owner,
          segmentCountPerNode,
          repairParallelism,
          intensityStr,
          incrementalRepairStr,
          nodesToRepairParam,
          datacentersToRepairParam,
          blacklistedTableNamesParam,
          repairThreadCountParam,
          forceParam,
          timeoutParam);

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
      boolean incrementalRepair;
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
      final Cluster cluster = context.storage.getClusterDao().getCluster(Cluster.toSymbolicName(clusterName.get()));
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
            .getDatacentersToRepairBasedOnParam(datacentersToRepairParam);

      } catch (IllegalArgumentException ex) {
        LOG.error(ex.getMessage(), ex);
        return Response.status(Response.Status.NOT_FOUND).entity(ex.getMessage()).build();
      }
      int timeout = timeoutParam.orElse(context.config.getHangingRepairTimeoutMins());
      boolean force = (forceParam.isPresent() ? Boolean.parseBoolean(forceParam.get()) : false);

      RepairUnit.Builder builder = RepairUnit.builder()
          .clusterName(cluster.getName())
          .keyspaceName(keyspace.get())
          .columnFamilies(tableNames)
          .incrementalRepair(incrementalRepair)
          .nodes(nodesToRepair)
          .datacenters(datacentersToRepair)
          .blacklistedTables(blacklistedTableNames)
          .repairThreadCount(repairThreadCountParam.orElse(context.config.getRepairThreadCount()))
          .timeout(timeout);

      final Optional<RepairUnit> maybeTheRepairUnit = repairUnitService.getOrCreateRepairUnit(cluster, builder, force);
      if (maybeTheRepairUnit.isPresent()) {
        RepairUnit theRepairUnit = maybeTheRepairUnit.get();
        if (theRepairUnit.getIncrementalRepair() != incrementalRepair) {
          String msg = String.format(
              "A repair unit %s already exist for the same cluster/keyspace/tables"
                  + " but with a different incremental repair value. Requested value %s | Existing value: %s",
              theRepairUnit.getId(),
              incrementalRepair,
              theRepairUnit.getIncrementalRepair());

          return Response.status(Response.Status.CONFLICT).entity(msg).build();
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

        final RepairRun newRepairRun = repairRunService.registerRepairRun(
            cluster,
            theRepairUnit,
            cause,
            owner.get(),
            segments,
            parallelism,
            intensity,
            false);

        return Response.created(buildRepairRunUri(uriInfo, newRepairRun))
            .entity(new RepairRunStatus(newRepairRun, theRepairUnit, 0))
            .build();
      } else {
        return Response.status(Response.Status.CONFLICT)
            .entity("An existing repair unit conflicts with your repair run.")
            .build();
      }

    } catch (ReaperException e) {
      LOG.error(e.getMessage(), e);
      return Response.serverError().entity(e.getMessage()).build();
    }
  }

  /**
   * Modifies a state of the repair run.
   *
   * <p>Currently supports NOT_STARTED|PAUSED to RUNNING and RUNNING to PAUSED.
   *
   * @return OK if all goes well NOT_MODIFIED if new state is the same as the old one, and 409
   *     (CONFLICT) if transition is not supported.
   */
  @PUT
  @Path("/{id}/state/{state}")
  public Response modifyRunState(
      @Context UriInfo uriInfo,
      @PathParam("id") UUID repairRunId,
      @PathParam("state") Optional<String> stateStr)
      throws ReaperException {

    LOG.info("modify repair run state called with: id = {}, state = {}", repairRunId, stateStr);
    try {
      if (!stateStr.isPresent()) {
        return createMissingArgumentResponse("state");
      }

      Optional<RepairRun> repairRun = repairRunDao.getRepairRun(repairRunId);
      if (!repairRun.isPresent()) {
        return Response.status(Status.NOT_FOUND).entity("repair run " + repairRunId + " doesn't exist").build();
      }

      final RepairRun.RunState newState = parseRunState(stateStr.get());
      final RunState oldState = repairRun.get().getRunState();
      if (oldState == newState) {
        String msg = "given \"state\" " + stateStr + " is same as the current run state";
        return Response.noContent().entity(msg).location(buildRepairRunUri(uriInfo, repairRun.get())).build();
      }
      if ((isStarting(oldState, newState) || isResuming(oldState, newState) || isRetrying(oldState, newState))
          && isUnitAlreadyRepairing(repairRun.get())) {

        String errMsg = "repair unit already has run " + repairRun.get().getRepairUnitId() + " in RUNNING state";
        LOG.error(errMsg);
        return Response.status(Status.CONFLICT).entity(errMsg).build();
      }

      if (isStarting(oldState, newState)) {
        return startRun(uriInfo, repairRun.get());
      } else if (isPausing(oldState, newState)) {
        return pauseRun(uriInfo, repairRun.get());
      } else if (isResuming(oldState, newState) || isRetrying(oldState, newState)) {
        return resumeRun(uriInfo, repairRun.get());
      } else if (isAborting(oldState, newState)) {
        return abortRun(uriInfo, repairRun.get());
      } else {
        String errMsg = String.format("Transition %s->%s not supported.", oldState.toString(), newState.toString());
        LOG.error(errMsg);
        return Response.status(Status.CONFLICT).entity(errMsg).build();
      }
    } catch (ValidationException ex) {
      return Response.status(Status.BAD_REQUEST).entity(ex.getMessage()).build();
    }
  }

  /**
   * Modifies the intensity of the repair run.
   *
   * @return OK if all goes well NOT_MODIFIED if new state is the same as the old one, and 409
   *     (CONFLICT) if transition is not supported.
   */
  @PUT
  @Path("/{id}/intensity/{intensity}")
  public Response modifyRunIntensity(
      @Context UriInfo uriInfo,
      @PathParam("id") UUID repairRunId,
      @PathParam("intensity") Optional<String> intensityStr)
      throws ReaperException {

    LOG.info("modify repair run intensity called with: id = {}, state = {}", repairRunId, intensityStr);
    try {
      if (!intensityStr.isPresent()) {
        return createMissingArgumentResponse("intensity");
      }
      final double intensity = parseIntensity(intensityStr.get());

      Optional<RepairRun> repairRun = repairRunDao.getRepairRun(repairRunId);
      if (!repairRun.isPresent()) {
        return Response.status(Status.NOT_FOUND).entity("repair run " + repairRunId + " doesn't exist").build();
      }

      if (RunState.PAUSED != repairRun.get().getRunState() && RunState.NOT_STARTED != repairRun.get().getRunState()) {
        return Response.status(Status.CONFLICT).entity("repair run must first be paused").build();
      }

      return updateRunIntensity(uriInfo, repairRun.get(), intensity);
    } catch (ValidationException ex) {
      return Response.status(Response.Status.BAD_REQUEST).entity(ex.getMessage()).build();
    }
  }

  private boolean isUnitAlreadyRepairing(RepairRun repairRun) {
    return repairRunDao.getRepairRunsForUnit(repairRun.getRepairUnitId()).stream()
        .anyMatch((run) -> (!run.getId().equals(repairRun.getId()) && run.getRunState().equals(RunState.RUNNING)));
  }

  private int getSegmentAmountForRepairRun(UUID repairRunId) {
    return context.storage.getRepairSegmentDao().getSegmentAmountForRepairRunWithState(repairRunId,
        RepairSegment.State.DONE);
  }

  private Response startRun(UriInfo uriInfo, RepairRun repairRun) throws ReaperException {
    LOG.info("Starting run {}", repairRun.getId());
    final RepairRun newRun = context.repairManager.startRepairRun(repairRun);
    return Response.ok().location(buildRepairRunUri(uriInfo, newRun)).build();
  }

  private Response pauseRun(UriInfo uriInfo, RepairRun repairRun) throws ReaperException {
    LOG.info("Pausing run {}", repairRun.getId());
    final RepairRun newRun = context.repairManager.pauseRepairRun(repairRun);
    return Response.ok().location(buildRepairRunUri(uriInfo, newRun)).build();
  }

  private Response resumeRun(UriInfo uriInfo, RepairRun repairRun) throws ReaperException {
    LOG.info("Resuming run {}", repairRun.getId());
    final RepairRun newRun = context.repairManager.startRepairRun(repairRun);
    return Response.ok().location(buildRepairRunUri(uriInfo, newRun)).build();
  }

  private Response abortRun(UriInfo uriInfo, RepairRun repairRun) throws ReaperException {
    LOG.info("Aborting run {}", repairRun.getId());
    final RepairRun newRun = context.repairManager.abortRepairRun(repairRun);
    return Response.ok().location(buildRepairRunUri(uriInfo, newRun)).build();
  }

  private Response updateRunIntensity(UriInfo uriInfo, RepairRun run, double intensity)
      throws ReaperException {

    LOG.info("Editing run {}", run.getId());
    RepairRun newRun = context.repairManager.updateRepairRunIntensity(run, intensity);
    return Response.ok().location(buildRepairRunUri(uriInfo, newRun)).build();
  }

  /**
   * @return detailed information about a repair run.
   */
  @GET
  @Path("/{id}")
  public Response getRepairRun(
      @PathParam("id") UUID repairRunId) {

    LOG.debug("get repair_run called with: id = {}", repairRunId);
    final Optional<RepairRun> repairRun = repairRunDao.getRepairRun(repairRunId);
    if (repairRun.isPresent()) {
      RepairRunStatus repairRunStatus = getRepairRunStatus(repairRun.get());
      return Response.ok().entity(repairRunStatus).build();
    } else {
      return Response.status(404).entity("repair run " + repairRunId + " doesn't exist").build();
    }
  }

  /**
   * @return list the segments of a repair run.
   */
  @GET
  @Path("/{id}/segments")
  public Response getRepairRunSegments(@PathParam("id") UUID repairRunId) {

    LOG.debug("get repair_run called with: id = {}", repairRunId);
    final Optional<RepairRun> repairRun = repairRunDao.getRepairRun(repairRunId);
    if (repairRun.isPresent()) {
      Collection<RepairSegment> segments = context.storage.getRepairSegmentDao().getRepairSegmentsForRun(repairRunId);
      return Response.ok().entity(segments).build();
    } else {
      return Response.status(404).entity("repair run " + repairRunId + " doesn't exist").build();
    }
  }

  /**
   * @return Aborts a running segment.
   */
  @POST
  @Path("/{id}/segments/abort/{segment_id}")
  public Response abortRepairRunSegment(@PathParam("id") UUID repairRunId, @PathParam("segment_id") UUID segmentId) {
    LOG.debug("abort segment called with: run id = {} and segment id = {}", repairRunId, segmentId);
    final Optional<RepairRun> repairRun = repairRunDao.getRepairRun(repairRunId);
    if (repairRun.isPresent()) {

      if (RepairRun.RunState.RUNNING == repairRun.get().getRunState()
          || RepairRun.RunState.PAUSED == repairRun.get().getRunState()) {

        try {
          RepairSegment segment = context.repairManager.abortSegment(repairRunId, segmentId);
          return Response.ok().entity(segment).build();
        } catch (ReaperException ex) {
          return Response.status(Response.Status.CONFLICT)
              .entity("Cannot abort segment " + ex.getLocalizedMessage())
              .build();
        }
      } else {
        return Response.status(Response.Status.CONFLICT)
            .entity("Cannot abort segment on repair run with status " + repairRun.get().getRunState())
            .build();
      }
    } else {
      return Response.status(404).entity("repair run " + repairRunId + " doesn't exist").build();
    }
  }

  /**
   * @return all know repair runs for a cluster.
   */
  @GET
  @Path("/cluster/{clusterName}")
  public Response getRepairRunsForCluster(
      @PathParam("clusterName") String clusterName,
      @QueryParam("limit") Optional<Integer> limit) {

    LOG.debug("get repair run for cluster called with: cluster_name = {}", clusterName);
    final Collection<RepairRun> repairRuns = repairRunDao
        .getRepairRunsForClusterPrioritiseRunning(clusterName, limit);
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
    RepairUnit repairUnit = context.storage.getRepairUnitDao().getRepairUnit(repairRun.getRepairUnitId());
    int segmentsRepaired = getSegmentAmountForRepairRun(repairRun.getId());
    return new RepairRunStatus(repairRun, repairUnit, segmentsRepaired);
  }

  /**
   * @param state    comma-separated list of states to return. These states must match names of {@link
   *                 io.cassandrareaper.core.RepairRun.RunState}.
   * @param cluster  only return repair runs belonging to this cluster
   * @param keyspace only return repair runs belonging to this keyspace
   * @return All repair runs in the system if the param is absent, repair runs with state included in the state
   *     parameter otherwise.
   *     If the state parameter contains non-existing run states, BAD_REQUEST response is returned.
   */
  @GET
  public Response listRepairRuns(
      @QueryParam("state") Optional<String> state,
      @QueryParam("cluster_name") Optional<String> cluster,
      @QueryParam("keyspace_name") Optional<String> keyspace,
      @QueryParam("limit") Optional<Integer> limit) {

    try {
      final Set<String> desiredStates = splitStateParam(state);
      if (desiredStates == null) {
        return Response.status(Response.Status.BAD_REQUEST).build();
      }

      Collection<Cluster> clusters = cluster.isPresent() && !cluster.get().equals("all")
          ? Collections.singleton(context.storage.getClusterDao().getCluster(cluster.get()))
          : context.storage.getClusterDao().getClusters();

      List<RepairRun> repairRuns = Lists.newArrayList();
      clusters.forEach(clstr -> repairRuns.addAll(
          repairRunDao.getRepairRunsForClusterPrioritiseRunning(clstr.getName(), limit))
      );
      List<RepairRunStatus> runStatuses = Lists.newArrayList();
      RepairRunService.sortByRunState(repairRuns);
      runStatuses.addAll(
          getRunStatuses(
              repairRuns.subList(0, min(repairRuns.size(), limit.orElse(1000))), desiredStates)
              .stream()
              .filter((run) -> !keyspace.isPresent()
                  || ((RepairRunStatus) run).getKeyspaceName().equals(keyspace.get()))
              .collect(Collectors.toList()));
      return Response.ok().entity(runStatuses).build();
    } catch (IllegalArgumentException e) {
      return Response.serverError().entity("Failed find cluster " + cluster.get()).build();
    }
  }

  private List<RepairRunStatus> getRunStatuses(Collection<RepairRun> runs, Set<String> desiredStates) {
    final List<RepairRunStatus> runStatuses = Lists.newArrayList();
    for (final RepairRun run : runs) {
      if (!desiredStates.isEmpty() && !desiredStates.contains(run.getRunState().name())) {
        continue;
      }
      RepairUnit runsUnit = context.storage.getRepairUnitDao().getRepairUnit(run.getRepairUnitId());
      int segmentsRepaired = run.getSegmentCount();
      if (!run.getRunState().equals(RepairRun.RunState.DONE)) {
        segmentsRepaired = getSegmentAmountForRepairRun(run.getId());
      }
      runStatuses.add(new RepairRunStatus(run, runsUnit, segmentsRepaired));
    }

    return runStatuses;
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
    final Optional<RepairRun> runToDelete = repairRunDao.getRepairRun(runId);
    if (runToDelete.isPresent()) {
      if (RepairRun.RunState.RUNNING == runToDelete.get().getRunState()) {
        return Response.status(Response.Status.CONFLICT)
            .entity("Repair run with id \"" + runId + "\" is currently running, and must be stopped before deleting")
            .build();
      }
      if (!runToDelete.get().getOwner().equalsIgnoreCase(owner.get())) {
        String msg = String.format("Repair run %s is not owned by the user you defined %s", runId, owner.get());
        return Response.status(Response.Status.CONFLICT).entity(msg).build();
      }
      if (context.storage.getRepairSegmentDao().getSegmentAmountForRepairRunWithState(runId,
          RepairSegment.State.RUNNING) > 0) {
        String msg = String.format("Repair run %s has running segments, which must finish before deleting", runId);
        return Response.status(Response.Status.CONFLICT).entity(msg).build();
      }
      repairRunDao.deleteRepairRun(runId);
      return Response.accepted().build();
    }
    try {
      // safety clean, in case of zombie segments
      repairRunDao.deleteRepairRun(runId);
    } catch (RuntimeException ignore) {
    }
    return Response.status(Response.Status.NOT_FOUND).entity("Repair run %s" + runId + " not found").build();
  }

  @POST
  @Path("/purge")
  public Response purgeRepairRuns() throws ReaperException {
    int purgedRepairs = PurgeService.create(context, repairRunDao).purgeDatabase();
    return Response.ok().entity(purgedRepairs).build();
  }
}