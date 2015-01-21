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

import com.google.common.base.Optional;
import com.google.common.collect.Lists;

import com.spotify.reaper.ReaperApplicationConfiguration;
import com.spotify.reaper.ReaperException;
import com.spotify.reaper.cassandra.JmxProxy;
import com.spotify.reaper.core.Cluster;
import com.spotify.reaper.core.RepairUnit;
import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.core.RepairSegment;
import com.spotify.reaper.resources.view.RepairRunStatus;
import com.spotify.reaper.cassandra.JmxConnectionFactory;
import com.spotify.reaper.service.RepairRunner;
import com.spotify.reaper.service.RingRange;
import com.spotify.reaper.service.SegmentGenerator;
import com.spotify.reaper.storage.IStorage;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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

  private static final Logger LOG = LoggerFactory.getLogger(RepairRunResource.class);

  private final IStorage storage;
  private final ReaperApplicationConfiguration config;
  private final JmxConnectionFactory jmxFactory;

  public RepairRunResource(ReaperApplicationConfiguration config, IStorage storage) {
    this.config = config;
    this.storage = storage;
    this.jmxFactory = new JmxConnectionFactory();
  }

  public RepairRunResource(ReaperApplicationConfiguration config, IStorage storage,
      JmxConnectionFactory jmxFactory) {
    this.storage = storage;
    this.config = config;
    this.jmxFactory = jmxFactory;
  }

  /**
   * Endpoint used to create a repair run. Does not allow triggering the run. triggerRepairRun()
   * must be called to initiate the repair. Creating a repair run includes generating repair
   * segments.
   *
   * Notice that query parameter "tables" can be a single String, or a comma-separated list
   * of table names. If the "tables" parameter is omitted, and only the keyspace is defined,
   * then created repair run will target all the tables in the keyspace.
   *
   * @return repair run ID in case of everything going well,
   *         and a status code 500 in case of errors.
   */
  @POST
  public Response addRepairRun(
      @Context UriInfo uriInfo,
      @QueryParam("clusterName") Optional<String> clusterName,
      @QueryParam("keyspace") Optional<String> keyspace,
      @QueryParam("tables") Optional<String> tableNames,
      @QueryParam("owner") Optional<String> owner,
      @QueryParam("cause") Optional<String> cause) {

    LOG.info("add repair run called with: clusterName = {}, keyspace = {}, tables = {}, owner = {},"
             + " cause = {}", clusterName, keyspace, tableNames, owner, cause);

    try {
      if (!clusterName.isPresent()) {
        throw new ReaperException("\"clusterName\" argument missing");
      }
      if (!keyspace.isPresent()) {
        throw new ReaperException("\"keyspace\" argument missing");
      }
      if (!owner.isPresent()) {
        throw new ReaperException("\"owner\" argument missing");
      }

      Cluster cluster = getCluster(clusterName.get());
      JmxProxy jmxProxy = jmxFactory.create(cluster.getSeedHosts().iterator().next());
      List<String> knownTables = jmxProxy.getTableNamesForKeyspace(keyspace.get());
      if (knownTables.size() == 0) {
        LOG.debug("no known tables for keyspace {} in cluster {}", keyspace.get(),
            clusterName.get());
        return Response.status(Response.Status.NOT_FOUND).entity(
            "no column families found for keyspace").build();
      }

      jmxProxy.close();

      RepairUnit repairUnit = getRepairUnit(clusterName.get(), keyspace.get(), tableNames.get());
      RepairRun newRepairRun = registerRepairRun(cluster, repairUnit, cause, owner.get());
      return Response.created(buildRepairRunURI(uriInfo, newRepairRun))
          .entity(new RepairRunStatus(newRepairRun, repairUnit))
          .build();
    } catch (ReaperException e) {
      LOG.error(e.getMessage());
      e.printStackTrace();
      return Response.status(500).entity(e.getMessage()).build();
    }
  }

  /**
   * Modifies a state of the repair run.
   *
   * Currently supports NOT_STARTED|PAUSED -> RUNNING and RUNNING -> PAUSED.
   * @return OK if all goes well NOT_MODIFIED if new state is the same as the old one,
   * and 501 (NOT_IMPLEMENTED) if transition is not supported.
   */
  @PUT
  @Path("/{id}")
  public Response modifyRunState(
    @Context UriInfo uriInfo,
    @PathParam("id") Long repairRunId,
    @QueryParam("state") Optional<String> state) {

    LOG.info("pause repair run called with: id = {}, state = {}", repairRunId, state);

    if (!state.isPresent()) {
      return Response.status(Response.Status.BAD_REQUEST.getStatusCode())
        .entity("\"state\" argument missing")
        .build();
    }

    try {
      RepairRun repairRun = getRepairRun(repairRunId);
      RepairUnit repairUnit = storage.getRepairUnit(repairRun.getRepairUnitId());
      RepairRun.RunState newState = RepairRun.RunState.valueOf(state.get());
      RepairRun.RunState oldState = repairRun.getRunState();

      if (oldState == newState) {
        return Response.ok("given \"state\" is same as the current run state").build();
      }

      if (isStarting(oldState, newState)) {
        return startRun(repairRun, repairUnit);
      }
      if (isPausing(oldState, newState)) {
        return pauseRun(repairRun, repairUnit);
      }
      if (isResuming(oldState, newState)) {
        return resumeRun(repairRun, repairUnit);
      }
      String errMsg = String.format("Transition %s->%s not supported.", newState.toString(),
        oldState.toString());
      LOG.error(errMsg);
      return Response.status(501).entity(errMsg).build();
    } catch (ReaperException e) {
      LOG.error(e.getMessage());
      e.printStackTrace();
      return Response.status(Response.Status.NOT_FOUND).entity(e.getMessage()).build();
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
    RepairRun updatedRun = repairRun.with()
      .runState(RepairRun.RunState.RUNNING)
      .startTime(DateTime.now())
      .build(repairRun.getId());
    storage.updateRepairRun(updatedRun);
    RepairRunner.startNewRepairRun(storage, repairRun.getId(), jmxFactory);
    return Response.status(Response.Status.OK).entity(new RepairRunStatus(repairRun, repairUnit))
      .build();
  }

  private Response pauseRun(RepairRun repairRun, RepairUnit repairUnit) {
    LOG.info("Pausing run {}", repairRun.getId());
    RepairRun updatedRun = repairRun.with()
      .runState(RepairRun.RunState.PAUSED)
      .build(repairRun.getId());
    storage.updateRepairRun(updatedRun);
    return Response.ok().entity(new RepairRunStatus(repairRun, repairUnit)).build();
  }

  private Response resumeRun(RepairRun repairRun, RepairUnit repairUnit) {
    LOG.info("Resuming run {}", repairRun.getId());
    RepairRun updatedRun = repairRun.with()
      .runState(RepairRun.RunState.RUNNING)
      .build(repairRun.getId());
    storage.updateRepairRun(updatedRun);
    return Response.ok().entity(new RepairRunStatus(repairRun, repairUnit)).build();
  }

  /**
   * @return detailed information about a repair run.
   */
  @GET
  @Path("/{id}")
  public Response getRepairRun(@PathParam("id") Long repairRunId) {
    LOG.info("get repair_run called with: id = {}", repairRunId);
    RepairRun repairRun = storage.getRepairRun(repairRunId);
    if (null != repairRun) {
      return Response.ok().entity(getRepairRunStatus(repairRun)).build();
    }
    else {
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
    Collection<RepairRun> repairRuns = storage.getRepairRunsForCluster(clusterName);
    Collection<RepairRunStatus> repairRunViews = new ArrayList<>();
    for (RepairRun repairRun : repairRuns) {
      repairRunViews.add(getRepairRunStatus(repairRun));
    }
    return Response.ok().entity(repairRunViews).build();
  }

  /**
   * @return cluster information for the given cluster name
   * @throws ReaperException if cluster with given name is not found
   */
  private Cluster getCluster(String clusterName) throws ReaperException {
    Cluster cluster = storage.getCluster(clusterName);
    if (cluster == null) {
      throw new ReaperException(String.format("Cluster \"%s\" not found", clusterName));
    }
    return cluster;
  }

  /**
   * @return repair unit information for given cluster, keyspace and table name
   * @throws ReaperException if such table is not found in Reaper's storage
   */
  private RepairUnit getRepairUnit(String clusterName, String keyspace,
    Collection<String> tableNames) throws ReaperException {
    RepairUnit repairUnit = storage.getRepairUnit(clusterName, keyspace, tableName);
    if (repairUnit == null) {
      throw new ReaperException(String.format("Column family \"%s/%s/%s\" not found", clusterName,
          keyspace, tableName));
    }
    return repairUnit;
  }

  /**
   * @return table information for given table id
   * @throws ReaperException if such table is not found in Reaper's storage
   */
  private RepairUnit getRepairUnit(long repairUnitId) throws ReaperException {
    RepairUnit repairUnit =  storage.getRepairUnit(repairUnitId);
    if (repairUnit == null) {
      throw new ReaperException(String.format("Column family with id \"%d\" not found",
          repairUnitId));
    }
    return repairUnit;
  }

  /**
   * Creates a repair run but does not trigger it.
   *
   * Creating a repair run involves:
   *   1) split token range into segments
   *   2) create a RepairRun instance
   *   3) create RepairSegment instances linked to RepairRun. these are directly stored in storage
   * @throws ReaperException if repair run fails to be stored in Reaper's storage
   */
  private RepairRun registerRepairRun(Cluster cluster, RepairUnit repairUnit,
      Optional<String> cause, String owner) throws ReaperException {

    // preparing a repair run involves several steps

    // the first step is to generate token segments
    List<RingRange> tokenSegments = generateSegments(cluster, repairUnit);
    checkNotNull(tokenSegments, "failed generating repair segments");

    // the next step is to prepare a repair run object
    RepairRun repairRun = storeNewRepairRun(cluster, repairUnit, cause, owner);
    checkNotNull(repairRun, "failed preparing repair run");

    // Notice that our RepairRun core object doesn't contain pointer to
    // the set of RepairSegments in the run, as they are accessed separately.
    // However, RepairSegment has a pointer to the RepairRun it lives in

    // the last preparation step is to generate actual repair segments
    storeNewRepairSegments(tokenSegments, repairRun, repairUnit);

    // now we're done and can return
    return repairRun;
  }

  /**
   * Splits a token range for given table into segments
   * @return
   * @throws ReaperException when fails to discover seeds for the cluster or fails to connect to
   *   any of the nodes in the Cluster.
   */
  private List<RingRange> generateSegments(Cluster targetCluster, RepairUnit existingTable)
      throws ReaperException {
    List<RingRange> segments = null;
    SegmentGenerator sg = new SegmentGenerator(targetCluster.getPartitioner());
    Set<String> seedHosts = targetCluster.getSeedHosts();
    if (seedHosts.isEmpty()) {
      String errMsg = String.format("didn't get any seed hosts for cluster \"%s\"",
          existingTable.getClusterName());
      LOG.error(errMsg);
      throw new ReaperException(errMsg);
    }
    for (String host : seedHosts) {
      try {
        JmxProxy jmxProxy = jmxFactory.create(host);
        List<BigInteger> tokens = jmxProxy.getTokens();
        segments = sg.generateSegments(existingTable.getSegmentCount(), tokens);
        jmxProxy.close();
        break;
      } catch (ReaperException e) {
        LOG.warn("couldn't connect to host: {}, will try next one", host);
      }
    }
    if (segments == null) {
      String errMsg = String.format("failed to generate repair segments for cluster \"%s\"",
          existingTable.getClusterName());
      LOG.error(errMsg);
      throw new ReaperException(errMsg);
    }
    return segments;
  }

  /**
   * Instantiates a RepairRun and stores it in the storage backend.
   * @return
   * @throws ReaperException when fails to store the RepairRun.
   */
  private RepairRun storeNewRepairRun(Cluster cluster, RepairUnit table, Optional<String> cause,
      String owner) throws ReaperException {
    RepairRun.Builder runBuilder = new RepairRun.Builder(cluster.getName(), table.getId(),
        DateTime.now(), config.getRepairIntensity());
    runBuilder.cause(cause.isPresent() ? cause.get() : "no cause specified");
    runBuilder.owner(owner);
    RepairRun newRepairRun = storage.addRepairRun(runBuilder);
    if (newRepairRun == null) {
      String errMsg = String.format("failed storing repair run for cluster \"%s/%s/%s",
          cluster.getName(), table.getKeyspaceName(), table.getName());
      LOG.error(errMsg);
      throw new ReaperException(errMsg);
    }
    return newRepairRun;
  }

  /**
   * Creates the repair runs linked to given RepairRun and stores them directly in the
   * storage backend.
   */
  private void storeNewRepairSegments(List<RingRange> tokenSegments, RepairRun repairRun,
      RepairUnit table) {
    List <RepairSegment.Builder> repairSegmentBuilders = Lists.newArrayList();
    for (RingRange range : tokenSegments) {
      RepairSegment.Builder repairSegment = new RepairSegment.Builder(repairRun.getId(), range,
          table.getId());
      repairSegmentBuilders.add(repairSegment);
    }
    // TODO(zvo): I don't like we can't figure out if this suceeds or not
    storage.addRepairSegments(repairSegmentBuilders, repairRun.getId());
  }

  /**
   * @return only a status of a repair run, not the entire repair run info.
   */
  private RepairRunStatus getRepairRunStatus(RepairRun repairRun) {
    RepairUnit repairUnit = storage.getColumnFamily(repairRun.getRepairUnitId());
    RepairRunStatus repairRunStatus = new RepairRunStatus(repairRun, repairUnit);
    if (repairRun.getRunState() != RepairRun.RunState.NOT_STARTED) {
      int segmentsRepaired =
          storage.getSegmentAmountForRepairRun(repairRun.getId(), RepairSegment.State.DONE);
      repairRunStatus.setSegmentsRepaired(segmentsRepaired);
    }
    return repairRunStatus;
  }

  /**
   * Crafts an URI used to identify given repair run.
   * @return
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
