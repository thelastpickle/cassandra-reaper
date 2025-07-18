/*
 * Copyright 2015-2017 Spotify AB
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
import io.cassandrareaper.core.EditableRepairSchedule;
import io.cassandrareaper.core.PercentRepairedMetric;
import io.cassandrareaper.core.RepairSchedule;
import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.resources.view.RepairRunStatus;
import io.cassandrareaper.resources.view.RepairScheduleStatus;
import io.cassandrareaper.service.RepairRunService;
import io.cassandrareaper.service.RepairScheduleService;
import io.cassandrareaper.service.RepairUnitService;
import io.cassandrareaper.storage.repairrun.IRepairRunDao;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.dropwizard.jersey.PATCH;
import io.dropwizard.jersey.validation.ValidationErrorMessage;
import jakarta.annotation.security.RolesAllowed;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;
import org.apache.cassandra.repair.RepairParallelism;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/repair_schedule")
@Produces(MediaType.APPLICATION_JSON)
public final class RepairScheduleResource {

  private static final Logger LOG = LoggerFactory.getLogger(RepairScheduleResource.class);

  private final AppContext context;
  private final RepairUnitService repairUnitService;
  private final RepairScheduleService repairScheduleService;
  private final RepairRunService repairRunService;

  private final IRepairRunDao repairRunDao;

  public RepairScheduleResource(AppContext context, IRepairRunDao repairRunDao) {
    this.context = context;
    this.repairUnitService = RepairUnitService.create(context);
    this.repairScheduleService = RepairScheduleService.create(context, repairRunDao);
    this.repairRunService = RepairRunService.create(context, repairRunDao);
    this.repairRunDao = repairRunDao;
  }

  private static boolean isPausing(RepairSchedule.State oldState, RepairSchedule.State newState) {
    return oldState == RepairSchedule.State.ACTIVE && newState == RepairSchedule.State.PAUSED;
  }

  private static boolean isResuming(RepairSchedule.State oldState, RepairSchedule.State newState) {
    return oldState == RepairSchedule.State.PAUSED && newState == RepairSchedule.State.ACTIVE;
  }

  /**
   * Crafts an URI used to identify given repair schedule.
   *
   * @return The created resource URI.
   */
  private static URI buildRepairScheduleUri(UriInfo uriInfo, RepairSchedule repairSchedule) {
    return uriInfo
        .getBaseUriBuilder()
        .path("repair_schedule")
        .path(repairSchedule.getId().toString())
        .build();
  }

  /**
   * Utility method to apply any valid parameters to an existing RepairSchedule. This method assumes
   * that any non-null parameter provided is valid and should be applied.
   *
   * @param repairSchedule - The schedule object to be updated
   * @param owner - The owner value to be used in the update
   * @param repairParallelism - the parallelism value to be used in the update
   * @param intensity - The intensity value to be used in the update
   * @param scheduleDaysBetween - The days between value to be used in the update
   * @param segmentCountPerNode - The segments per node value to be used in the update
   * @param adaptive - Whether or not the schedule is adaptive
   * @param percentUnrepairedThreshold - Threshold of unrepaired percentage that triggers a repair
   */
  protected static RepairSchedule applyRepairPatchParams(
      final RepairSchedule repairSchedule,
      final String owner,
      final RepairParallelism repairParallelism,
      final Double intensity,
      final Integer scheduleDaysBetween,
      final Integer segmentCountPerNode,
      final Boolean adaptive,
      final Integer percentUnrepairedThreshold) {
    if (repairSchedule == null) {
      return null;
    }

    // Apply any valid incoming values to the schedule
    return repairSchedule
        .with()
        .owner(owner != null ? owner.trim() : repairSchedule.getOwner())
        .repairParallelism(
            repairParallelism != null ? repairParallelism : repairSchedule.getRepairParallelism())
        .intensity(intensity != null ? intensity : repairSchedule.getIntensity())
        .daysBetween(
            scheduleDaysBetween != null ? scheduleDaysBetween : repairSchedule.getDaysBetween())
        .segmentCountPerNode(
            segmentCountPerNode != null
                ? segmentCountPerNode
                : repairSchedule.getSegmentCountPerNode())
        .percentUnrepairedThreshold(percentUnrepairedThreshold)
        .adaptive(adaptive != null ? adaptive : false)
        .build(repairSchedule.getId());
  }

  @PATCH
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("/{id}")
  @RolesAllowed({"operator"})
  public Response patchRepairSchedule(
      @Context UriInfo uriInfo,
      @PathParam("id") UUID repairScheduleId,
      @NotNull @Valid EditableRepairSchedule editableRepairSchedule) {
    if (repairScheduleId == null) {
      ValidationErrorMessage errorMessage =
          new ValidationErrorMessage(
              ImmutableList.copyOf(Lists.newArrayList("id must not be null or empty")));
      return Response.status(400).entity(errorMessage).build();
    }

    // When executed through DropWizard the validation will prevent this from ever being reached
    // but to protect against an NPE if the behavior is ever changed, do a quick check of the param
    if (editableRepairSchedule == null) {
      ValidationErrorMessage errorMessage =
          new ValidationErrorMessage(
              ImmutableList.copyOf(Lists.newArrayList("request body must not be null or empty")));
      return Response.status(400).entity(errorMessage).build();
    }

    // Try to find the schedule to be updated
    Optional<RepairSchedule> repairScheduleWrapper =
        context.storage.getRepairScheduleDao().getRepairSchedule(repairScheduleId);
    // See if we found the schedule
    RepairSchedule repairSchedule = repairScheduleWrapper.orElse(null);
    if (repairSchedule == null) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }

    // Apply any valid incoming values to the schedule
    RepairSchedule patchedRepairSchedule =
        applyRepairPatchParams(
            repairSchedule,
            editableRepairSchedule.getOwner(),
            editableRepairSchedule.getRepairParallelism(),
            editableRepairSchedule.getIntensity(),
            editableRepairSchedule.getDaysBetween(),
            editableRepairSchedule.getSegmentCountPerNode(),
            editableRepairSchedule.getAdaptive(),
            editableRepairSchedule.getPercentUnrepairedThreshold());

    // Attempt to update the schedule
    boolean updated =
        context.storage.getRepairScheduleDao().updateRepairSchedule(patchedRepairSchedule);
    if (updated) {
      return Response.status(Response.Status.OK)
          .entity(getRepairScheduleStatus(patchedRepairSchedule))
          .build();
    } else {
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  /**
   * Endpoint used to create a repair schedule. Does not allow triggering the run. Repair schedule
   * will create new repair runs based on the schedule.
   *
   * <p>Notice that query parameter "tables" can be a single String, or a comma-separated list of
   * table names. If the "tables" parameter is omitted, and only the keyspace is defined, then
   * created repair runs will target all the tables in the keyspace.
   *
   * @return created repair schedule data as JSON.
   */
  @POST
  @RolesAllowed({"operator"})
  public Response addRepairSchedule(
      @Context UriInfo uriInfo,
      @QueryParam("clusterName") Optional<String> clusterName,
      @QueryParam("keyspace") Optional<String> keyspace,
      @QueryParam("tables") Optional<String> tableNamesParam,
      @QueryParam("owner") Optional<String> owner,
      @QueryParam("segmentCountPerNode") Optional<Integer> segmentCountPerNode,
      @QueryParam("repairParallelism") Optional<String> repairParallelism,
      @QueryParam("intensity") Optional<String> intensityStr,
      @QueryParam("incrementalRepair") Optional<String> incrementalRepairStr,
      @QueryParam("subrangeIncrementalRepair") Optional<String> subrangeIncrementalRepairStr,
      @QueryParam("scheduleDaysBetween") Optional<Integer> scheduleDaysBetween,
      @QueryParam("scheduleTriggerTime") Optional<String> scheduleTriggerTime,
      @QueryParam("nodes") Optional<String> nodesToRepairParam,
      @QueryParam("datacenters") Optional<String> datacentersToRepairParam,
      @QueryParam("blacklistedTables") Optional<String> blacklistedTableNamesParam,
      @QueryParam("repairThreadCount") Optional<Integer> repairThreadCountParam,
      @QueryParam("force") Optional<String> forceParam,
      @QueryParam("timeout") Optional<Integer> timeoutParam,
      @QueryParam("adaptive") Optional<String> adaptiveParam,
      @QueryParam("percentUnrepairedThreshold") Optional<Integer> percentUnrepairedParam) {

    try {
      Response possibleFailResponse =
          RepairRunResource.checkRequestForAddRepair(
              context,
              clusterName,
              keyspace,
              tableNamesParam,
              owner,
              segmentCountPerNode,
              repairParallelism,
              intensityStr,
              incrementalRepairStr,
              subrangeIncrementalRepairStr,
              nodesToRepairParam,
              datacentersToRepairParam,
              blacklistedTableNamesParam,
              repairThreadCountParam,
              forceParam,
              timeoutParam);

      if (null != possibleFailResponse) {
        return possibleFailResponse;
      }

      DateTime nextActivation;
      try {
        nextActivation = getNextActivationTime(scheduleTriggerTime);
        if (nextActivation.isBefore(DateTime.now().minusMinutes(15))) {
          return Response.status(Response.Status.BAD_REQUEST)
              .entity(
                  "given schedule_trigger_time is too far in the past: "
                      + RepairRunStatus.dateTimeToIso8601(nextActivation))
              .build();
        }
      } catch (IllegalArgumentException ex) {
        LOG.info("cannot parse data string: {}", scheduleTriggerTime.get(), ex);
        return Response.status(Response.Status.BAD_REQUEST)
            .entity("invalid schedule_trigger_time")
            .build();
      }

      if (!scheduleDaysBetween.isPresent()) {
        return Response.status(Response.Status.BAD_REQUEST)
            .entity("missing required parameter: scheduleDaysBetween")
            .build();
      }

      Cluster cluster =
          context.storage.getClusterDao().getCluster(Cluster.toSymbolicName(clusterName.get()));

      Set<String> tableNames;
      try {
        tableNames =
            repairRunService.getTableNamesBasedOnParam(cluster, keyspace.get(), tableNamesParam);
      } catch (IllegalArgumentException ex) {
        LOG.error(ex.getMessage(), ex);
        return Response.status(Response.Status.NOT_FOUND).entity(ex.getMessage()).build();
      }

      Set<String> blacklistedTableNames;
      try {
        blacklistedTableNames =
            repairRunService.getTableNamesBasedOnParam(
                cluster, keyspace.get(), blacklistedTableNamesParam);
      } catch (IllegalArgumentException ex) {
        LOG.error(ex.getMessage(), ex);
        return Response.status(Response.Status.NOT_FOUND).entity(ex.getMessage()).build();
      }

      final Set<String> nodesToRepair;
      try {
        nodesToRepair = repairRunService.getNodesToRepairBasedOnParam(cluster, nodesToRepairParam);
      } catch (final IllegalArgumentException ex) {
        LOG.error(ex.getMessage(), ex);
        return Response.status(Response.Status.NOT_FOUND).entity(ex.getMessage()).build();
      }

      final Set<String> datacentersToRepair =
          RepairRunService.getDatacentersToRepairBasedOnParam(datacentersToRepairParam);

      boolean incremental =
          isIncrementalRepair(incrementalRepairStr)
              || isIncrementalRepair(subrangeIncrementalRepairStr);
      RepairParallelism parallelism = context.config.getRepairParallelism();
      if (repairParallelism.isPresent()) {
        LOG.debug(
            "using given repair parallelism {} over configured value {}",
            repairParallelism.get(),
            parallelism);
        parallelism = RepairParallelism.valueOf(repairParallelism.get().toUpperCase());
      }

      if (!parallelism.equals(RepairParallelism.PARALLEL) && incremental) {
        return Response.status(Response.Status.BAD_REQUEST)
            .entity("Incremental repairs only supports PARALLEL parallelism mode.")
            .build();
      }

      if (percentUnrepairedParam.orElse(-1) > 0 && !incremental) {
        return Response.status(Response.Status.BAD_REQUEST)
            .entity(
                "Triggering schedules on % unrepaired threshold is only allowed for incremental repairs.")
            .build();
      }

      // explicitly force a schedule even if the schedule conflicts
      boolean force = (forceParam.isPresent() ? Boolean.parseBoolean(forceParam.get()) : false);

      int timeout = timeoutParam.orElse(context.config.getHangingRepairTimeoutMins());
      boolean adaptive =
          (adaptiveParam.isPresent() ? Boolean.parseBoolean(adaptiveParam.get()) : false);
      boolean subrangeIncremental = isIncrementalRepair(subrangeIncrementalRepairStr);
      RepairUnit.Builder unitBuilder =
          RepairUnit.builder()
              .clusterName(cluster.getName())
              .keyspaceName(keyspace.get())
              .columnFamilies(tableNames)
              .incrementalRepair(incremental)
              .subrangeIncrementalRepair(subrangeIncremental)
              .nodes(nodesToRepair)
              .datacenters(datacentersToRepair)
              .blacklistedTables(blacklistedTableNames)
              .repairThreadCount(
                  repairThreadCountParam.orElse(context.config.getRepairThreadCount()))
              .timeout(timeout);

      return addRepairSchedule(
          cluster,
          unitBuilder,
          getDaysBetween(scheduleDaysBetween),
          owner.get(),
          parallelism,
          uriInfo,
          incremental,
          nextActivation,
          getSegmentCount(segmentCountPerNode),
          getIntensity(intensityStr),
          force,
          adaptive,
          percentUnrepairedParam.orElse(-1));

    } catch (ReaperException e) {
      LOG.error(e.getMessage(), e);
      return Response.serverError().entity(e.getMessage()).build();
    }
  }

  private Response addRepairSchedule(
      Cluster cluster,
      RepairUnit.Builder unitBuilder,
      int days,
      String owner,
      RepairParallelism parallel,
      UriInfo uriInfo,
      boolean incremental,
      DateTime next,
      int segments,
      Double intensity,
      boolean force,
      boolean adaptive,
      int percentUnrepairedThreshold) {

    Optional<RepairSchedule> conflictingRepairSchedule =
        repairScheduleService.identicalRepairUnit(cluster, unitBuilder);

    if (conflictingRepairSchedule.isPresent()) {
      return Response.noContent()
          .location(buildRepairScheduleUri(uriInfo, conflictingRepairSchedule.get()))
          .build();
    }

    conflictingRepairSchedule =
        repairScheduleService.conflictingRepairSchedule(cluster, unitBuilder);

    if (conflictingRepairSchedule.isPresent()) {
      RepairSchedule existingSchedule = conflictingRepairSchedule.get();

      if (existingSchedule.getDaysBetween() == days
          && existingSchedule.getOwner().equals(owner)
          && existingSchedule.getRepairParallelism() == parallel) {

        return Response.noContent()
            .location(buildRepairScheduleUri(uriInfo, existingSchedule))
            .build();
      }

      if (!force) {
        String msg =
            String.format(
                "A repair schedule already exists for cluster \"%s\", keyspace \"%s\", and column families: %s",
                cluster.getName(), unitBuilder.keyspaceName, unitBuilder.columnFamilies);

        return Response.status(Response.Status.CONFLICT)
            .location(buildRepairScheduleUri(uriInfo, existingSchedule))
            .entity(msg)
            .build();
      }
    }

    Optional<RepairUnit> maybeUnit =
        repairUnitService.getOrCreateRepairUnit(cluster, unitBuilder, force);

    if (maybeUnit.isPresent()) {
      RepairUnit unit = maybeUnit.get();
      Preconditions.checkState(
          unit.getIncrementalRepair() == incremental
              || unit.getSubrangeIncrementalRepair() == incremental,
          "%s!=%s",
          unit.getIncrementalRepair(),
          incremental);
      Preconditions.checkState(
          (percentUnrepairedThreshold > 0 && incremental) || percentUnrepairedThreshold <= 0,
          "Setting a % repaired threshold can only be done on incremental schedules");

      RepairSchedule newRepairSchedule =
          repairScheduleService.storeNewRepairSchedule(
              cluster,
              unit,
              days,
              next,
              owner,
              segments,
              parallel,
              intensity,
              force,
              adaptive,
              percentUnrepairedThreshold);

      return Response.created(buildRepairScheduleUri(uriInfo, newRepairSchedule)).build();
    }

    return Response.status(Response.Status.NO_CONTENT)
        .entity(
            "Repair schedule couldn't be created as an existing repair unit seems to conflict with it.")
        .build();
  }

  private int getDaysBetween(Optional<Integer> scheduleDaysBetween) {
    int daysBetween = context.config.getScheduleDaysBetween();
    if (scheduleDaysBetween.isPresent()) {
      LOG.debug(
          "using given schedule days between {} instead of configured value {}",
          scheduleDaysBetween.get(),
          context.config.getScheduleDaysBetween());
      daysBetween = scheduleDaysBetween.get();
    }
    return daysBetween;
  }

  private int getSegmentCount(Optional<Integer> segmentCount) {
    int segments = 0;
    if (segmentCount.isPresent()) {
      LOG.debug("using given segment count {}", segmentCount.get());
      segments = segmentCount.get();
    }
    return segments;
  }

  private Boolean isIncrementalRepair(Optional<String> incrementalRepairStr) {
    Boolean incrementalRepair;
    if (incrementalRepairStr.isPresent()) {
      incrementalRepair = Boolean.parseBoolean(incrementalRepairStr.get());
    } else {
      incrementalRepair = context.config.getIncrementalRepair();
      LOG.debug("no incremental repair given, so using default value: {}", incrementalRepair);
    }
    return incrementalRepair;
  }

  private Double getIntensity(Optional<String> intensityStr) throws NumberFormatException {
    Double intensity;
    if (intensityStr.isPresent()) {
      intensity = Double.parseDouble(intensityStr.get());
    } else {
      intensity = context.config.getRepairIntensity();
      LOG.debug("no intensity given, so using default value: {}", intensity);
    }
    return intensity;
  }

  /**
   * Modifies a state of the repair schedule.
   *
   * <p>Currently supports PAUSED to ACTIVE and ACTIVE to PAUSED.
   *
   * @return OK if all goes well, NO_CONTENT if new state is the same as the old one, and 400
   *     (BAD_REQUEST) if transition is not supported.
   */
  @PUT
  @Path("/{id}")
  @RolesAllowed({"operator"})
  public Response modifyState(
      @Context UriInfo uriInfo,
      @PathParam("id") UUID repairScheduleId,
      @QueryParam("state") Optional<String> state) {

    LOG.info(
        "modify repair schedule state called with: id = {}, state = {}", repairScheduleId, state);

    if (!state.isPresent()) {
      return Response.status(Response.Status.BAD_REQUEST.getStatusCode())
          .entity("\"state\" argument missing")
          .build();
    }

    Optional<RepairSchedule> repairSchedule =
        context.storage.getRepairScheduleDao().getRepairSchedule(repairScheduleId);
    if (!repairSchedule.isPresent()) {
      return Response.status(Response.Status.NOT_FOUND)
          .entity("repair schedule with id " + repairScheduleId + " not found")
          .build();
    }

    RepairUnit repairUnit =
        context.storage.getRepairUnitDao().getRepairUnit(repairSchedule.get().getRepairUnitId());
    RepairSchedule.State newState;
    try {
      newState = RepairSchedule.State.valueOf(state.get().toUpperCase());
    } catch (IllegalArgumentException ex) {
      LOG.error(ex.getMessage(), ex);
      return Response.status(Response.Status.BAD_REQUEST.getStatusCode())
          .entity("invalid \"state\" argument: " + state.get())
          .build();
    }
    RepairSchedule.State oldState = repairSchedule.get().getState();
    if (oldState == newState) {
      return Response.noContent()
          .location(buildRepairScheduleUri(uriInfo, repairSchedule.get()))
          .build();
    }

    if (isPausing(oldState, newState)) {
      return pauseSchedule(repairSchedule.get(), uriInfo);
    } else if (isResuming(oldState, newState)) {
      return resumeSchedule(repairSchedule.get(), uriInfo);
    } else {
      String errMsg =
          String.format(
              "Transition %s->%s not supported.", oldState.toString(), newState.toString());
      LOG.error(errMsg);
      return Response.status(Response.Status.BAD_REQUEST).entity(errMsg).build();
    }
  }

  private Response pauseSchedule(RepairSchedule repairSchedule, UriInfo uriInfo) {
    LOG.info("Pausing schedule {}", repairSchedule.getId());
    context.schedulingManager.pauseRepairSchedule(repairSchedule);
    return Response.ok().location(buildRepairScheduleUri(uriInfo, repairSchedule)).build();
  }

  private Response resumeSchedule(RepairSchedule repairSchedule, UriInfo uriInfo) {
    LOG.info("Resuming schedule {}", repairSchedule.getId());
    context.schedulingManager.resumeRepairSchedule(repairSchedule);
    return Response.ok().location(buildRepairScheduleUri(uriInfo, repairSchedule)).build();
  }

  /**
   * @return detailed information about a repair schedule.
   */
  @GET
  @Path("/{id}")
  @RolesAllowed({"user", "operator"})
  public Response getRepairSchedule(@PathParam("id") UUID repairScheduleId) {
    LOG.debug("get repair_schedule called with: id = {}", repairScheduleId);
    Optional<RepairSchedule> repairSchedule =
        context.storage.getRepairScheduleDao().getRepairSchedule(repairScheduleId);
    if (repairSchedule.isPresent()) {
      return Response.ok().entity(getRepairScheduleStatus(repairSchedule.get())).build();
    } else {
      return Response.status(404)
          .entity("repair schedule with id " + repairScheduleId + " doesn't exist")
          .build();
    }
  }

  /**
   * Force start a repair from a schedule.
   *
   * @return detailed information about a repair schedule.
   */
  @POST
  @Path("/start/{id}")
  @RolesAllowed({"operator"})
  public Response startRepairSchedule(@PathParam("id") UUID repairScheduleId) {
    LOG.debug("start repair_schedule called with: id = {}", repairScheduleId);
    Optional<RepairSchedule> repairSchedule =
        context.storage.getRepairScheduleDao().getRepairSchedule(repairScheduleId);
    if (repairSchedule.isPresent()) {
      RepairSchedule newSchedule =
          repairSchedule.get().with().nextActivation(DateTime.now()).build(repairScheduleId);

      context.storage.getRepairScheduleDao().updateRepairSchedule(newSchedule);
      return Response.ok().entity(getRepairScheduleStatus(newSchedule)).build();
    } else {
      return Response.status(404)
          .entity("repair schedule with id " + repairScheduleId + " doesn't exist")
          .build();
    }
  }

  /**
   * @param clusterName The cluster_name for which the repair schedule belongs to.
   * @return all know repair schedules for a cluster.
   */
  @GET
  @Path("/cluster/{cluster_name}")
  @RolesAllowed({"user", "operator"})
  public Response getRepairSchedulesForCluster(@PathParam("cluster_name") String clusterName) {
    LOG.debug("get repair schedules for cluster called with: cluster_name = {}", clusterName);
    Collection<RepairSchedule> repairSchedules =
        context.storage.getRepairScheduleDao().getRepairSchedulesForCluster(clusterName);
    Collection<RepairScheduleStatus> repairScheduleViews = new ArrayList<>();
    for (RepairSchedule repairSchedule : repairSchedules) {
      repairScheduleViews.add(getRepairScheduleStatus(repairSchedule));
    }
    return Response.ok().entity(repairScheduleViews).build();
  }

  /**
   * @return RepairSchedule status for viewing
   */
  private RepairScheduleStatus getRepairScheduleStatus(RepairSchedule repairSchedule) {
    RepairUnit repairUnit =
        context.storage.getRepairUnitDao().getRepairUnit(repairSchedule.getRepairUnitId());
    return new RepairScheduleStatus(repairSchedule, repairUnit);
  }

  /**
   * @param clusterName The cluster name to list the schedules for. If not given, will list all
   *     schedules for all clusters.
   * @param keyspaceName The keyspace name to list schedules for. Limits the returned list and works
   *     whether the cluster name is given or not.
   * @return All schedules in the system.
   */
  @GET
  @RolesAllowed({"user", "operator"})
  public Response listSchedules(
      @QueryParam("clusterName") Optional<String> clusterName,
      @QueryParam("keyspace") Optional<String> keyspaceName) {

    List<RepairScheduleStatus> scheduleStatuses = Lists.newArrayList();
    getScheduleList(clusterName, keyspaceName)
        .forEach(
            (schedule) -> {
              RepairUnit unit =
                  context.storage.getRepairUnitDao().getRepairUnit(schedule.getRepairUnitId());
              scheduleStatuses.add(new RepairScheduleStatus(schedule, unit));
            });
    return Response.ok().entity(scheduleStatuses).build();
  }

  private Collection<RepairSchedule> getScheduleList(
      Optional<String> clusterName, Optional<String> keyspaceName) {
    Collection<RepairSchedule> schedules;
    if (clusterName.isPresent() && keyspaceName.isPresent()) {
      schedules =
          context
              .storage
              .getRepairScheduleDao()
              .getRepairSchedulesForClusterAndKeyspace(clusterName.get(), keyspaceName.get());
    } else if (clusterName.isPresent()) {
      schedules =
          context.storage.getRepairScheduleDao().getRepairSchedulesForCluster(clusterName.get());
    } else if (keyspaceName.isPresent()) {
      schedules =
          context.storage.getRepairScheduleDao().getRepairSchedulesForKeyspace(keyspaceName.get());
    } else {
      schedules = context.storage.getRepairScheduleDao().getAllRepairSchedules();
    }
    return schedules;
  }

  /**
   * Delete a RepairSchedule object with given id.
   *
   * <p>Repair schedule can only be deleted when it is not active, so you must stop it first.
   *
   * @param repairScheduleId The id for the RepairSchedule instance to delete.
   * @param owner The assigned owner of the deleted resource. Must match the stored one.
   * @return 202 response code if the delete has been accepted, 409 if schedule can't be stopped.
   */
  @DELETE
  @Path("/{id}")
  @RolesAllowed({"operator"})
  public Response deleteRepairSchedule(
      @PathParam("id") UUID repairScheduleId, @QueryParam("owner") Optional<String> owner) {

    LOG.info(
        "delete repair schedule called with repairScheduleId: {}, and owner: {}",
        repairScheduleId,
        owner);
    if (!owner.isPresent()) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity("required query parameter \"owner\" is missing")
          .build();
    }
    Optional<RepairSchedule> scheduleToDelete =
        context.storage.getRepairScheduleDao().getRepairSchedule(repairScheduleId);
    if (scheduleToDelete.isPresent()) {
      if (RepairSchedule.State.ACTIVE == scheduleToDelete.get().getState()) {
        String msg =
            String.format(
                "Repair schedule %s currently running. Must be first stopped", repairScheduleId);
        return Response.status(Response.Status.CONFLICT).entity(msg).build();
      }
      if (!scheduleToDelete.get().getOwner().equalsIgnoreCase(owner.get())) {
        String msg =
            String.format("Repair schedule %s is not owned by %s", repairScheduleId, owner.get());
        return Response.status(Response.Status.CONFLICT).entity(msg).build();
      }
      repairScheduleService.deleteRepairSchedule(repairScheduleId);
      return Response.accepted().build();
    }
    return Response.status(Response.Status.NOT_FOUND)
        .entity("Repair schedule with id \"" + repairScheduleId + "\" not found")
        .build();
  }

  private DateTime getNextActivationTime(Optional<String> scheduleTriggerTime)
      throws IllegalArgumentException {
    DateTime nextActivation;
    if (scheduleTriggerTime.isPresent()) {
      nextActivation = DateTime.parse(scheduleTriggerTime.get());
      LOG.info(
          "first schedule activation will be: {}",
          RepairRunStatus.dateTimeToIso8601(nextActivation));
    } else {
      nextActivation = DateTime.now().plusDays(1).withTimeAtStartOfDay();
      LOG.info(
          "no schedule_trigger_time given, so setting first scheduling next night: {}",
          RepairRunStatus.dateTimeToIso8601(nextActivation));
    }
    return nextActivation;
  }

  @GET
  @Path("/{clusterName}/{id}/percent_repaired")
  @RolesAllowed({"user", "operator"})
  public List<PercentRepairedMetric> getPercentRepairedMetricsForSchedule(
      @PathParam("clusterName") String clusterName, @PathParam("id") UUID repairScheduleId)
      throws IllegalArgumentException {
    long since = DateTime.now().minusHours(1).getMillis();
    return context.storage.getPercentRepairedMetrics(clusterName, repairScheduleId, since);
  }
}
