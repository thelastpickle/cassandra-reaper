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

import com.google.common.base.CharMatcher;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;

import com.spotify.reaper.AppContext;
import com.spotify.reaper.ReaperApplication;
import com.spotify.reaper.ReaperException;
import com.spotify.reaper.cassandra.JmxProxy;
import com.spotify.reaper.core.Cluster;
import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.core.RepairSegment;
import com.spotify.reaper.core.RepairUnit;
import com.spotify.reaper.resources.view.RepairRunStatus;
import com.spotify.reaper.service.RepairRunFactory;
import com.spotify.reaper.service.RepairRunner;

import org.apache.cassandra.repair.RepairParallelism;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

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

import static com.google.common.base.Preconditions.checkNotNull;

@Path("/repair_run")
@Produces(MediaType.APPLICATION_JSON)
public class RepairRunResource {

  public static final Splitter COMMA_SEPARATED_LIST_SPLITTER =
      Splitter.on(',').trimResults(CharMatcher.anyOf(" ()[]\"'")).omitEmptyStrings();

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
      @QueryParam("intensity") Optional<String> intensityStr
  ) {
    LOG.info("add repair run called with: clusterName = {}, keyspace = {}, tables = {}, owner = {},"
             + " cause = {}, segmentCount = {}, repairParallelism = {}, intensity = {}",
             clusterName, keyspace, tableNamesParam, owner, cause, segmentCount, repairParallelism,
             intensityStr);
    try {
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
      if (repairParallelism.isPresent()) {
        try {
          ReaperApplication.checkRepairParallelismString(repairParallelism.get());
        } catch (ReaperException ex) {
          return Response.status(Response.Status.BAD_REQUEST).entity(ex.getMessage()).build();
        }
      }
      Double intensity = null;
      if (intensityStr.isPresent()) {
        try {
          intensity = Double.parseDouble(intensityStr.get());
          if (intensity <= 0.0 || intensity > 1.0) {
            return Response.status(Response.Status.BAD_REQUEST).entity(
                "query parameter \"intensity\" must be in half closed range (0.0, 1.0]: "
                + intensityStr.get()).build();
          }
        } catch (NumberFormatException ex) {
          return Response.status(Response.Status.BAD_REQUEST).entity(
              "invalid value for query parameter \"intensity\": " + intensityStr.get()).build();
        }
      }

      if (intensity == null) {
        intensity = context.config.getRepairIntensity();
        LOG.debug("no intensity given, so using default value: " + intensity);
      }

      Optional<Cluster> cluster =
          context.storage.getCluster(Cluster.toSymbolicName(clusterName.get()));
      if (!cluster.isPresent()) {
        return Response.status(Response.Status.NOT_FOUND).entity(
            "No cluster found with name \"" + clusterName.get()
            + "\", did you register your cluster first?").build();
      }

      Set<String> knownTables;
      try (JmxProxy jmxProxy = context.jmxConnectionFactory.connectAny(cluster.get())) {
        knownTables = jmxProxy.getTableNamesForKeyspace(keyspace.get());
        if (knownTables.isEmpty()) {
          LOG.debug("no known tables for keyspace {} in cluster {}", keyspace.get(),
                    cluster.get().getName());
          return Response.status(Response.Status.NOT_FOUND).entity(
              "no column families found for keyspace").build();
        }
      }

      Set<String> tableNames;
      if (tableNamesParam.isPresent() && !tableNamesParam.get().isEmpty()) {
        tableNames = Sets.newHashSet(COMMA_SEPARATED_LIST_SPLITTER.split(tableNamesParam.get()));
        for (String name : tableNames) {
          if (!knownTables.contains(name)) {
            return Response.status(Response.Status.NOT_FOUND).entity(
                "keyspace doesn't contain a table named \"" + name + "\"").build();
          }
        }
      } else {
        tableNames = Collections.emptySet();
      }

      Optional<RepairUnit> storedRepairUnit =
          context.storage.getRepairUnit(cluster.get().getName(), keyspace.get(), tableNames);
      RepairUnit theRepairUnit;
      if (storedRepairUnit.isPresent()) {
        LOG.info(
            "use existing repair unit for cluster '{}', keyspace '{}', and column families: {}",
            cluster.get().getName(), keyspace.get(), tableNames);
        theRepairUnit = storedRepairUnit.get();
      } else {
        LOG.info("create new repair unit for cluster '{}', keyspace '{}', and column families: {}",
                 cluster.get().getName(), keyspace.get(), tableNames);
        theRepairUnit = context.storage.addRepairUnit(
            new RepairUnit.Builder(cluster.get().getName(), keyspace.get(), tableNames));
      }

      int segments = context.config.getSegmentCount();
      if (segmentCount.isPresent()) {
        LOG.debug("using given segment count {} instead of configured value {}",
                  segmentCount.get(), context.config.getSegmentCount());
        segments = segmentCount.get();
      }
      String repairParallelismStr = context.config.getRepairParallelism();
      if (repairParallelism.isPresent()) {
        LOG.debug("using given repair parallelism {} instead of configured value {}",
                  repairParallelism.get(), context.config.getRepairParallelism());
        repairParallelismStr = repairParallelism.get();
      }
      RepairRun newRepairRun = RepairRunFactory.registerRepairRun(
          context, cluster.get(), theRepairUnit, cause, owner.get(), segments,
          RepairParallelism.valueOf(repairParallelismStr.toUpperCase()), intensity);

      return Response.created(buildRepairRunURI(uriInfo, newRepairRun))
          .entity(new RepairRunStatus(newRepairRun, theRepairUnit)).build();

    } catch (ReaperException e) {
      LOG.error(e.getMessage());
      e.printStackTrace();
      return Response.status(500).entity(e.getMessage()).build();
    }
  }

  /**
   * Modifies a state of the repair run. <p/> Currently supports NOT_STARTED|PAUSED -> RUNNING and
   * RUNNING -> PAUSED.
   *
   * @return OK if all goes well NOT_MODIFIED if new state is the same as the old one, and 501
   * (NOT_IMPLEMENTED) if transition is not supported.
   */
  @PUT
  @Path("/{id}")
  public Response modifyRunState(
      @Context UriInfo uriInfo,
      @PathParam("id") Long repairRunId,
      @QueryParam("state") Optional<String> state) {

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

    Optional<RepairUnit>
        repairUnit =
        context.storage.getRepairUnit(repairRun.get().getRepairUnitId());
    if (!repairUnit.isPresent()) {
      String errMsg = "repair unit with id " + repairRun.get().getRepairUnitId() + " not found";
      LOG.error(errMsg);
      return Response.status(Response.Status.NOT_FOUND).entity(errMsg).build();
    }

    RepairRun.RunState newState = RepairRun.RunState.valueOf(state.get());
    RepairRun.RunState oldState = repairRun.get().getRunState();

    if (oldState == newState) {
      return Response.ok("given \"state\" is same as the current run state").build();
    }

    if (isStarting(oldState, newState)) {
      return startRun(repairRun.get(), repairUnit.get());
    } else if (isPausing(oldState, newState)) {
      return pauseRun(repairRun.get(), repairUnit.get());
    } else if (isResuming(oldState, newState)) {
      return resumeRun(repairRun.get(), repairUnit.get());
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

  private Response startRun(RepairRun repairRun, RepairUnit repairUnit) {
    LOG.info("Starting run {}", repairRun.getId());
    RepairRunner.startRepairRun(context, repairRun);
    return Response.status(Response.Status.OK).entity(new RepairRunStatus(repairRun, repairUnit))
        .build();
  }

  private Response pauseRun(RepairRun repairRun, RepairUnit repairUnit) {
    LOG.info("Pausing run {}", repairRun.getId());
    RepairRunner.pauseRepairRun(context, repairRun);
    return Response.ok().entity(new RepairRunStatus(repairRun, repairUnit)).build();
  }

  private Response resumeRun(RepairRun repairRun, RepairUnit repairUnit) {
    LOG.info("Resuming run {}", repairRun.getId());
    RepairRunner.startRepairRun(context, repairRun);
    return Response.ok().entity(new RepairRunStatus(repairRun, repairUnit)).build();
  }

  /**
   * @return detailed information about a repair run.
   */
  @GET
  @Path("/{id}")
  public Response getRepairRun(@PathParam("id") Long repairRunId) {
    LOG.info("get repair_run called with: id = {}", repairRunId);
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
    LOG.info("get repair run for cluster called with: cluster_name = {}", clusterName);
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
    assert repairUnit.isPresent() : "no repair unit found with id: " + repairRun.getRepairUnitId();
    RepairRunStatus repairRunStatus = new RepairRunStatus(repairRun, repairUnit.get());
    if (repairRun.getRunState() != RepairRun.RunState.NOT_STARTED) {
      int segmentsRepaired =
          context.storage.getSegmentAmountForRepairRun(repairRun.getId(), RepairSegment.State.DONE);
      repairRunStatus.setSegmentsRepaired(segmentsRepaired);
    }
    return repairRunStatus;
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
      LOG.error(e.getMessage());
      e.printStackTrace();
    }
    checkNotNull(runUri, "failed to build repair run uri");
    return runUri;
  }

}
