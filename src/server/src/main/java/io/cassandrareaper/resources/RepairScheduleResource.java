/*
 * Copyright 2015-2017 Spotify AB
 * Copyright 2016-2018 The Last Pickle Ltd
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
import io.cassandrareaper.core.RepairSchedule;
import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.resources.view.RepairRunStatus;
import io.cassandrareaper.resources.view.RepairScheduleStatus;
import io.cassandrareaper.service.RepairRunService;
import io.cassandrareaper.service.RepairScheduleService;
import io.cassandrareaper.service.RepairUnitService;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
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

  public RepairScheduleResource(AppContext context) {
    this.context = context;
    this.repairUnitService = RepairUnitService.create(context);
    this.repairScheduleService = RepairScheduleService.create(context);
    this.repairRunService = RepairRunService.create(context);
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
      @QueryParam("scheduleDaysBetween") Optional<Integer> scheduleDaysBetween,
      @QueryParam("scheduleTriggerTime") Optional<String> scheduleTriggerTime,
      @QueryParam("nodes") Optional<String> nodesToRepairParam,
      @QueryParam("datacenters") Optional<String> datacentersToRepairParam,
      @QueryParam("blacklistedTables") Optional<String> blacklistedTableNamesParam,
      @QueryParam("repairThreadCount") Optional<Integer> repairThreadCountParam,
      @QueryParam("activeTime") Optional<String> activeTimeParam,
      @QueryParam("inactiveTime") Optional<String> inactiveTimeParam) {

    try {
      Response possibleFailResponse = RepairRunResource.checkRequestForAddRepair(
              context,
              clusterName,
              keyspace,
              owner,
              segmentCountPerNode,
              repairParallelism,
              intensityStr,
              incrementalRepairStr,
              nodesToRepairParam,
              datacentersToRepairParam,
              repairThreadCountParam,
              activeTimeParam,
              inactiveTimeParam);

      if (null != possibleFailResponse) {
        return possibleFailResponse;
      }

      DateTime nextActivation;
      try {
        nextActivation = getNextActivationTime(scheduleTriggerTime);
        if (nextActivation.isBefore(DateTime.now().minusMinutes(15))) {
          return Response.status(Response.Status.BAD_REQUEST)
              .entity("given schedule_trigger_time is too far in the past: "
                      + RepairRunStatus.dateTimeToIso8601(nextActivation))
              .build();
        }
      } catch (IllegalArgumentException ex) {
        LOG.info("cannot parse data string: {}", scheduleTriggerTime.get(), ex);
        return Response.status(Response.Status.BAD_REQUEST).entity("invalid schedule_trigger_time").build();
      }

      if (!scheduleDaysBetween.isPresent()) {
        return Response.status(Response.Status.BAD_REQUEST)
            .entity("missing required parameter: scheduleDaysBetween")
            .build();
      }

      Cluster cluster = context.storage.getCluster(Cluster.toSymbolicName(clusterName.get())).get();
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
      } catch (final IllegalArgumentException ex) {
        LOG.error(ex.getMessage(), ex);
        return Response.status(Response.Status.NOT_FOUND).entity(ex.getMessage()).build();
      }

      final Set<String> datacentersToRepair;
      try {
        datacentersToRepair = RepairRunService
            .getDatacentersToRepairBasedOnParam(cluster, datacentersToRepairParam);
      } catch (final IllegalArgumentException ex) {
        LOG.error(ex.getMessage(), ex);
        return Response.status(Response.Status.NOT_FOUND).entity(ex.getMessage()).build();
      }

      boolean incremental = isIncrementalRepair(incrementalRepairStr);
      RepairParallelism parallelism = context.config.getRepairParallelism();
      if (repairParallelism.isPresent()) {
        LOG.debug("using given repair parallelism {} over configured value {}", repairParallelism.get(), parallelism);
        parallelism = RepairParallelism.valueOf(repairParallelism.get().toUpperCase());
      }

      if (!parallelism.equals(RepairParallelism.PARALLEL) && incremental) {
        return Response.status(Response.Status.BAD_REQUEST)
            .entity("Can't mix sequential repair and incremental repairs")
            .build();
      }

      final String activeTime;
      final String inactiveTime;
      if (activeTimeParam.isPresent() && inactiveTimeParam.isPresent()) {
        activeTime = activeTimeParam.get();
        inactiveTime = inactiveTimeParam.get();
        LOG.debug("activeTime - inactiveTime: {} - {}", activeTime, inactiveTime);
      } else {
        activeTime = "";
        inactiveTime = "";
        LOG.debug("no activeTime / inactiveTime given, so using default value: {} - {}", activeTime, inactiveTime);
      }

      RepairUnit.Builder unitBuilder = RepairUnit.builder()
          .clusterName(cluster.getName())
          .keyspaceName(keyspace.get())
          .columnFamilies(tableNames)
          .incrementalRepair(incremental)
          .nodes(nodesToRepair)
          .datacenters(datacentersToRepair)
          .blacklistedTables(blacklistedTableNames)
          .repairThreadCount(repairThreadCountParam.orElse(context.config.getRepairThreadCount()))
          .activeTime(activeTime)
          .inactiveTime(inactiveTime);

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
          activeTime,
          inactiveTime
      );

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
      String activeTime,
      String inactiveTime) {

    Optional<RepairSchedule> conflictingRepairSchedule
        = repairScheduleService.conflictingRepairSchedule(cluster, unitBuilder);

    if (conflictingRepairSchedule.isPresent()) {
      RepairSchedule existingSchedule = conflictingRepairSchedule.get();

      if (existingSchedule.getDaysBetween() == days
          && existingSchedule.getOwner().equals(owner)
          && existingSchedule.getRepairParallelism() == parallel) {

        return Response.noContent().location(buildRepairScheduleUri(uriInfo, existingSchedule)).build();
      }

      String msg = String.format(
          "A repair schedule already exists for cluster \"%s\", keyspace \"%s\", and column families: %s",
          cluster.getName(),
          unitBuilder.keyspaceName,
          unitBuilder.columnFamilies);

      return Response
          .status(Response.Status.CONFLICT)
          .location(buildRepairScheduleUri(uriInfo, existingSchedule))
          .entity(msg)
          .build();
    } else {

      RepairUnit unit = repairUnitService.getOrCreateRepairUnit(cluster, unitBuilder);

      Preconditions
          .checkState(unit.getIncrementalRepair() == incremental, "%s!=%s", unit.getIncrementalRepair(), incremental);

      RepairSchedule newRepairSchedule = repairScheduleService
          .storeNewRepairSchedule(cluster, unit, days, next, owner, segments, parallel, intensity,
                  activeTime, inactiveTime);

      return Response.created(buildRepairScheduleUri(uriInfo, newRepairSchedule)).build();
    }
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
   * @return OK if all goes well, NO_CONTENT if new state is the same as the old one,
   *     and 400 (BAD_REQUEST) if transition is not supported.
   */
  @PUT
  @Path("/{id}")
  public Response modifyState(
      @Context UriInfo uriInfo,
      @PathParam("id") UUID repairScheduleId,
      @QueryParam("state") Optional<String> state) {

    LOG.info("modify repair schedule state called with: id = {}, state = {}", repairScheduleId, state);

    if (!state.isPresent()) {
      return Response.status(Response.Status.BAD_REQUEST.getStatusCode()).entity("\"state\" argument missing").build();
    }

    Optional<RepairSchedule> repairSchedule = context.storage.getRepairSchedule(repairScheduleId);
    if (!repairSchedule.isPresent()) {
      return Response.status(Response.Status.NOT_FOUND)
          .entity("repair schedule with id " + repairScheduleId + " not found")
          .build();
    }

    RepairUnit repairUnit = context.storage.getRepairUnit(repairSchedule.get().getRepairUnitId());
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
      return Response.noContent().location(buildRepairScheduleUri(uriInfo, repairSchedule.get())).build();
    }

    if (isPausing(oldState, newState)) {
      return pauseSchedule(repairSchedule.get(), uriInfo);
    } else if (isResuming(oldState, newState)) {
      return resumeSchedule(repairSchedule.get(), uriInfo);
    } else {
      String errMsg = String.format("Transition %s->%s not supported.", oldState.toString(), newState.toString());
      LOG.error(errMsg);
      return Response.status(Response.Status.BAD_REQUEST).entity(errMsg).build();
    }
  }

  private static boolean isPausing(RepairSchedule.State oldState, RepairSchedule.State newState) {
    return oldState == RepairSchedule.State.ACTIVE && newState == RepairSchedule.State.PAUSED;
  }

  private static boolean isResuming(RepairSchedule.State oldState, RepairSchedule.State newState) {
    return oldState == RepairSchedule.State.PAUSED && newState == RepairSchedule.State.ACTIVE;
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
  public Response getRepairSchedule(
      @PathParam("id") UUID repairScheduleId) {
    LOG.debug("get repair_schedule called with: id = {}", repairScheduleId);
    Optional<RepairSchedule> repairSchedule = context.storage.getRepairSchedule(repairScheduleId);
    if (repairSchedule.isPresent()) {
      return Response.ok().entity(getRepairScheduleStatus(repairSchedule.get())).build();
    } else {
      return Response.status(404).entity("repair schedule with id " + repairScheduleId + " doesn't exist").build();
    }
  }

  /**
   * Force start a repair from a schedule.
   *
   * @return detailed information about a repair schedule.
   */
  @POST
  @Path("/start/{id}")
  public Response startRepairSchedule(@PathParam("id") UUID repairScheduleId) {
    LOG.debug("start repair_schedule called with: id = {}", repairScheduleId);
    Optional<RepairSchedule> repairSchedule = context.storage.getRepairSchedule(repairScheduleId);
    if (repairSchedule.isPresent()) {
      RepairSchedule newSchedule = repairSchedule.get()
          .with()
          .nextActivation(DateTime.now())
          .build(repairScheduleId);

      context.storage.updateRepairSchedule(newSchedule);
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
  public Response getRepairSchedulesForCluster(
      @PathParam("cluster_name") String clusterName) {
    LOG.debug("get repair schedules for cluster called with: cluster_name = {}", clusterName);
    Collection<RepairSchedule> repairSchedules = context.storage.getRepairSchedulesForCluster(clusterName);
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
    RepairUnit repairUnit = context.storage.getRepairUnit(repairSchedule.getRepairUnitId());
    return new RepairScheduleStatus(repairSchedule, repairUnit);
  }

  /**
   * Crafts an URI used to identify given repair schedule.
   *
   * @return The created resource URI.
   */
  private static URI buildRepairScheduleUri(UriInfo uriInfo, RepairSchedule repairSchedule) {
    return uriInfo.getBaseUriBuilder().path("repair_schedule").path(repairSchedule.getId().toString()).build();
  }

  /**
   * @param clusterName The cluster name to list the schedules for. If not given, will list all schedules for all
   *        clusters.
   * @param keyspaceName The keyspace name to list schedules for. Limits the returned list and works whether the cluster
   *        name is given or not.
   * @return All schedules in the system.
   */
  @GET
  public Response listSchedules(
      @QueryParam("clusterName") Optional<String> clusterName,
      @QueryParam("keyspace") Optional<String> keyspaceName) {

    List<RepairScheduleStatus> scheduleStatuses = Lists.newArrayList();
    getScheduleList(clusterName, keyspaceName).forEach((schedule) -> {
      RepairUnit unit = context.storage.getRepairUnit(schedule.getRepairUnitId());
      scheduleStatuses.add(new RepairScheduleStatus(schedule, unit));
    });
    return Response.ok().entity(scheduleStatuses).build();
  }

  private Collection<RepairSchedule> getScheduleList(Optional<String> clusterName, Optional<String> keyspaceName) {
    Collection<RepairSchedule> schedules;
    if (clusterName.isPresent() && keyspaceName.isPresent()) {
      schedules = context.storage.getRepairSchedulesForClusterAndKeyspace(clusterName.get(), keyspaceName.get());
    } else if (clusterName.isPresent()) {
      schedules = context.storage.getRepairSchedulesForCluster(clusterName.get());
    } else if (keyspaceName.isPresent()) {
      schedules = context.storage.getRepairSchedulesForKeyspace(keyspaceName.get());
    } else {
      schedules = context.storage.getAllRepairSchedules();
    }
    return schedules;
  }

  /**
   * Delete a RepairSchedule object with given id.
   *
   * <p>
   * Repair schedule can only be deleted when it is not active, so you must stop it first.
   *
   * @param repairScheduleId The id for the RepairSchedule instance to delete.
   * @param owner The assigned owner of the deleted resource. Must match the stored one.
   * @return 202 response code if the delete has been accepted, 409 if schedule can't be stopped.
   */
  @DELETE
  @Path("/{id}")
  public Response deleteRepairSchedule(
      @PathParam("id") UUID repairScheduleId,
      @QueryParam("owner") Optional<String> owner) {

    LOG.info("delete repair schedule called with repairScheduleId: {}, and owner: {}", repairScheduleId, owner);
    if (!owner.isPresent()) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity("required query parameter \"owner\" is missing")
          .build();
    }
    Optional<RepairSchedule> scheduleToDelete = context.storage.getRepairSchedule(repairScheduleId);
    if (scheduleToDelete.isPresent()) {
      if (RepairSchedule.State.ACTIVE == scheduleToDelete.get().getState()) {
        String msg = String.format("Repair schedule %s currently running. Must be first stopped", repairScheduleId);
        return Response.status(Response.Status.CONFLICT).entity(msg).build();
      }
      if (!scheduleToDelete.get().getOwner().equalsIgnoreCase(owner.get())) {
        String msg = String.format("Repair schedule %s is not owned by %s", repairScheduleId, owner.get());
        return Response.status(Response.Status.CONFLICT).entity(msg).build();
      }
      context.storage.deleteRepairSchedule(repairScheduleId);
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
      LOG.info("first schedule activation will be: {}", RepairRunStatus.dateTimeToIso8601(nextActivation));
    } else {
      nextActivation = DateTime.now().plusDays(1).withTimeAtStartOfDay();
      LOG.info(
          "no schedule_trigger_time given, so setting first scheduling next night: {}",
          RepairRunStatus.dateTimeToIso8601(nextActivation));
    }
    return nextActivation;
  }
}
