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

import com.spotify.reaper.AppContext;
import com.spotify.reaper.ReaperException;
import com.spotify.reaper.cassandra.JmxProxy;
import com.spotify.reaper.core.Cluster;
import com.spotify.reaper.resources.view.ClusterStatus;
import com.spotify.reaper.resources.view.NodesStatus;
import com.spotify.reaper.resources.view.RepairRunStatus;
import com.spotify.reaper.resources.view.RepairScheduleStatus;

import com.spotify.reaper.service.ClusterRepairScheduler;

import jersey.repackaged.com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

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

@Path("/cluster")
@Produces(MediaType.APPLICATION_JSON)
public class ClusterResource {

  private static final int JMX_NODE_STATUS_CONCURRENCY = 3;

  private static final ExecutorService CLUSTER_STATUS_EXECUTOR
          = Executors.newFixedThreadPool(JMX_NODE_STATUS_CONCURRENCY *2);

  private static final Logger LOG = LoggerFactory.getLogger(ClusterResource.class);

  private final AppContext context;
  private final ClusterRepairScheduler clusterRepairScheduler;

  public ClusterResource(AppContext context) {
    this.context = context;
    this.clusterRepairScheduler = new ClusterRepairScheduler(context);
  }

  @GET
  public Response getClusterList(@QueryParam("seedHost") Optional<String> seedHost) {
    LOG.debug("get cluster list called");
    Collection<Cluster> clusters = context.storage.getClusters();
    List<String> clusterNames = new ArrayList<>();
    for (Cluster cluster : clusters) {
      if (seedHost.isPresent()) {
        if (cluster.getSeedHosts().contains(seedHost.get())) {
          clusterNames.add(cluster.getName());
        }
      } else {
        clusterNames.add(cluster.getName());
      }
    }
    return Response.ok().entity(clusterNames).build();
  }

  @GET
  @Path("/{cluster_name}")
  public Response getCluster(
      @PathParam("cluster_name") String clusterName,
      @QueryParam("limit") Optional<Integer> limit) throws ReaperException {
    LOG.debug("get cluster called with cluster_name: {}", clusterName);
    return viewCluster(clusterName, limit, Optional.<URI>absent());
  }

  private Response viewCluster(String clusterName, Optional<Integer> limit,
      Optional<URI> createdURI) throws ReaperException {
    Optional<Cluster> cluster = context.storage.getCluster(clusterName);

    if (!cluster.isPresent()) {
      return Response.status(Response.Status.NOT_FOUND)
          .entity("cluster with name \"" + clusterName + "\" not found").build();
    } else {
      ClusterStatus view =
          new ClusterStatus(cluster.get(),
              context.storage.getClusterRunStatuses(clusterName, limit.or(Integer.MAX_VALUE)),
              context.storage.getClusterScheduleStatuses(clusterName), getNodesStatus(cluster).orNull());
      if (createdURI.isPresent()) {
        return Response.created(createdURI.get())
            .entity(view).build();
      } else {
        return Response.ok()
            .entity(view).build();
      }
    }
  }

  @POST
  public Response addCluster(
      @Context UriInfo uriInfo,
      @QueryParam("seedHost") Optional<String> seedHost) throws ReaperException {
    if (!seedHost.isPresent()) {
      LOG.error("POST on cluster resource called without seedHost");
      return Response.status(400).entity("query parameter \"seedHost\" required").build();
    }
    LOG.debug("add cluster called with seedHost: {}", seedHost.get());

    Cluster newCluster;
    try {
      newCluster = createClusterWithSeedHost(seedHost.get());
    } catch (java.lang.SecurityException e) {
      LOG.error(e.getMessage(), e);
      return Response.status(400)
          .entity("seed host \"" + seedHost.get() + "\" JMX threw security exception: "
                  + e.getMessage()).build();
    } catch (ReaperException e) {
      LOG.error(e.getMessage(), e);
      return Response.status(400)
          .entity("failed to create cluster with seed host: " + seedHost.get()).build();
    }
    Optional<Cluster> existingCluster = context.storage.getCluster(newCluster.getName());
    if (existingCluster.isPresent()) {
      LOG.info("cluster already stored with this name: {}", existingCluster);
      return Response.status(403)
          .entity(String.format("cluster \"%s\" already exists", existingCluster.get().getName()))
          .build();
    } else {
      LOG.info("creating new cluster based on given seed host: {}", newCluster.getName());
      context.storage.addCluster(newCluster);

      if (context.config.hasAutoSchedulingEnabled()) {
        try {
          clusterRepairScheduler.scheduleRepairs(newCluster);
        } catch (ReaperException e) {
          LOG.error("failed to automatically schedule repairs", e);
          return Response.status(400)
            .entity("failed to automatically schedule repairs for cluster with seed host \"" + seedHost.get()
                    + "\". Exception was: " + e.getMessage()).build();
        }
      }
    }

    URI createdURI;
    try {
      createdURI = new URL(uriInfo.getAbsolutePath().toURL(), newCluster.getName()).toURI();
    } catch (Exception e) {
      String errMsg = "failed creating target URI for cluster: " + newCluster.getName();
      LOG.error(errMsg, e);
      return Response.status(400).entity(errMsg).build();
    }

    return viewCluster(newCluster.getName(), Optional.<Integer>absent(), Optional.of(createdURI));
  }

  public Cluster createClusterWithSeedHost(String seedHostInput) throws ReaperException {
    Optional<String> clusterName = Optional.absent();
    Optional<String> partitioner = Optional.absent();
    Optional<List<String>> liveNodes = Optional.absent();
    Set<String> seedHosts = CommonTools.parseSeedHosts(seedHostInput);

    try (JmxProxy jmxProxy = context.jmxConnectionFactory
            .connectAny(Optional.absent(), seedHosts, context.config.getJmxConnectionTimeoutInSeconds())) {

      clusterName = Optional.of(jmxProxy.getClusterName());
      partitioner = Optional.of(jmxProxy.getPartitioner());
      liveNodes = Optional.of(jmxProxy.getLiveNodes());
    } catch (ReaperException e) {
      LOG.error("failed to create cluster with seed hosts: {}", seedHosts, e);
    }

    if(!clusterName.isPresent()) {
      throw new ReaperException("Could not connect any seed host");
    }

    Set<String> seedHostsFinal = seedHosts;
    if (context.config.getEnableDynamicSeedList() && liveNodes.isPresent()) {
      seedHostsFinal = !liveNodes.get().isEmpty()
                          ? liveNodes.get().stream().collect(Collectors.toSet())
                          : seedHosts;
    }

    LOG.debug("Seeds {}", seedHostsFinal);

    return new Cluster(clusterName.get(), partitioner.get(), seedHostsFinal);
  }

  @PUT
  @Path("/{cluster_name}")
  public Response modifyClusterSeed(
      @Context UriInfo uriInfo,
      @PathParam("cluster_name") String clusterName,
      @QueryParam("seedHost") Optional<String> seedHost) throws ReaperException {

    if (!seedHost.isPresent()) {
      LOG.error("PUT on cluster resource called without seedHost");
      return Response.status(400).entity("query parameter \"seedHost\" required").build();
    }
    LOG.info("modify cluster called with: cluster_name = {}, seedHost = {}", clusterName, seedHost.get());

    Optional<Cluster> cluster = context.storage.getCluster(clusterName);
    if (!cluster.isPresent()) {
      return Response
              .status(Response.Status.NOT_FOUND)
              .entity("cluster with name " + clusterName + " not found")
              .build();
    }

    Set<String> newSeeds = CommonTools.parseSeedHosts(seedHost.get());

    if(context.config.getEnableDynamicSeedList()) {
      try (JmxProxy jmxProxy = context.jmxConnectionFactory
              .connectAny(Optional.absent(), newSeeds, context.config.getJmxConnectionTimeoutInSeconds())) {

        Optional<List<String>> liveNodes = Optional.of(jmxProxy.getLiveNodes());
        newSeeds = liveNodes.get().stream().collect(Collectors.toSet());
      } catch (ReaperException e) {
        LOG.error("failed to create cluster with seed hosts: {}", newSeeds, e);
      }
    }

    if (newSeeds.equals(cluster.get().getSeedHosts()) || newSeeds.isEmpty()) {
      return Response.notModified().build();
    }

    Cluster newCluster = new Cluster(cluster.get().getName(), cluster.get().getPartitioner(), newSeeds);
    context.storage.updateCluster(newCluster);

    return viewCluster(newCluster.getName(), Optional.<Integer>absent(), Optional.<URI>absent());
  }

  /**
   * Delete a Cluster object with given name.
   *
   * Cluster can be only deleted when it hasn't any RepairRun or RepairSchedule instances under it,
   * i.e. you must delete all repair runs and schedules first.
   *
   * @param clusterName The name of the Cluster instance you are about to delete.
   * @return The deleted RepairRun instance, with state overwritten to string "DELETED".
   * @throws ReaperException
   */
  @DELETE
  @Path("/{cluster_name}")
  public Response deleteCluster(@PathParam("cluster_name") String clusterName) throws ReaperException {

    LOG.info("delete cluster called with clusterName: {}", clusterName);
    Optional<Cluster> clusterToDelete = context.storage.getCluster(clusterName);
    if (!clusterToDelete.isPresent()) {
      return Response.status(Response.Status.NOT_FOUND).entity(
          "cluster with name \"" + clusterName + "\" not found").build();
    }
    if (!context.storage.getRepairSchedulesForCluster(clusterName).isEmpty()) {
      return Response.status(Response.Status.FORBIDDEN).entity(
          "cluster with name \"" + clusterName + "\" cannot be deleted, as it "
          + "has repair schedules").build();
    }
    if (!context.storage.getRepairRunsForCluster(clusterName).isEmpty()) {
      return Response.status(Response.Status.FORBIDDEN).entity(
          "cluster with name \"" + clusterName + "\" cannot be deleted, as it "
          + "has repair runs").build();
    }
    Optional<Cluster> deletedCluster = context.storage.deleteCluster(clusterName);
    if (deletedCluster.isPresent()) {
      return Response.ok(
              new ClusterStatus(
                      deletedCluster.get(),
                      Collections.<RepairRunStatus>emptyList(),
                      Collections.<RepairScheduleStatus>emptyList(),
                      getNodesStatus(deletedCluster).orNull()))
          .build();
    }
    return Response.serverError().entity("delete failed for schedule with name \"" + clusterName + "\"").build();
  }


  /**
   * Callable to get and parse endpoint states through JMX
   *
   *
   * @param seedHost The host address to connect to via JMX
   * @return An optional NodesStatus object with the status of each node in the cluster as seen from the seedHost node
   */
  private Callable<Optional<NodesStatus>> getEndpointState(List<String> seeds) {
    return () -> {
      try (JmxProxy jmxProxy = context.jmxConnectionFactory
              .connectAny(Optional.absent(), seeds, context.config.getJmxConnectionTimeoutInSeconds())) {

        Optional<String> allEndpointsState = Optional.fromNullable(jmxProxy.getAllEndpointsState());
        Optional<Map<String, String>> simpleStates = Optional.fromNullable(jmxProxy.getSimpleStates());

        return Optional
                .of(new NodesStatus(jmxProxy.getHost(), allEndpointsState.or(""), simpleStates.or(new HashMap<>())));

      } catch (RuntimeException e) {
        LOG.debug("failed to create cluster with seed hosts: {}", seeds, e);
        Thread.sleep(TimeUnit.MILLISECONDS.convert(JmxProxy.JMX_CONNECTION_TIMEOUT, JmxProxy.JMX_CONNECTION_TIMEOUT_UNIT));
        return Optional.absent();
      }
    };
  }


  /**
   * Get all nodes state by querying the AllEndpointsState attribute through JMX.
   *
   * To speed up execution, the method calls JMX on 3 nodes asynchronously and processes the first response
   *
   * @param cluster
   * @return An optional NodesStatus object with all nodes statuses
   */
  public Optional<NodesStatus> getNodesStatus(Optional<Cluster> cluster){
    Optional<NodesStatus> nodesStatus = Optional.absent();
    if (cluster.isPresent() && null != cluster.get().getSeedHosts()) {

        List<String> seedHosts = Lists.newArrayList(cluster.get().getSeedHosts());

        List<Callable<Optional<NodesStatus>>> endpointStateTasks
                = Lists.<Callable<Optional<NodesStatus>>>newArrayList(
                        getEndpointState(seedHosts),
                        getEndpointState(seedHosts),
                        getEndpointState(seedHosts));

        try {
          nodesStatus = CLUSTER_STATUS_EXECUTOR.invokeAny(
                  endpointStateTasks,
                  JmxProxy.JMX_CONNECTION_TIMEOUT,
                  JmxProxy.JMX_CONNECTION_TIMEOUT_UNIT);

        } catch (InterruptedException | ExecutionException | TimeoutException e) {
          LOG.debug("failed grabbing nodes status", e);
        }

        if (nodesStatus.isPresent()) {
          return nodesStatus;
        }
      }

    return nodesStatus;
  }

}
