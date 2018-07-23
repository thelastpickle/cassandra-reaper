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
import io.cassandrareaper.core.Node;
import io.cassandrareaper.jmx.JmxProxy;
import io.cassandrareaper.resources.view.ClusterStatus;
import io.cassandrareaper.resources.view.NodesStatus;
import io.cassandrareaper.service.ClusterRepairScheduler;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

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

import com.codahale.metrics.InstrumentedExecutorService;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/cluster")
@Produces(MediaType.APPLICATION_JSON)
public final class ClusterResource {

  private static final Logger LOG = LoggerFactory.getLogger(ClusterResource.class);

  private final AppContext context;
  private final ExecutorService executor;
  private final ClusterRepairScheduler clusterRepairScheduler;

  public ClusterResource(AppContext context, ExecutorService executor) {
    this.context = context;
    this.executor = new InstrumentedExecutorService(executor, context.metricRegistry);
    this.clusterRepairScheduler = new ClusterRepairScheduler(context);
  }

  @GET
  public Response getClusterList(
      @QueryParam("seedHost") Optional<String> seedHost) {

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
      @QueryParam("limit") Optional<Integer> limit) {

    LOG.debug("get cluster called with cluster_name: {}", clusterName);
    Optional<Cluster> cluster = context.storage.getCluster(clusterName);

    if (!cluster.isPresent()) {
      return Response.status(Response.Status.NOT_FOUND)
          .entity("cluster with name \"" + clusterName + "\" not found")
          .build();

    } else {
      ClusterStatus clusterStatus = new ClusterStatus(
            cluster.get(),
            context.storage.getClusterRunStatuses(cluster.get().getName(), limit.or(Integer.MAX_VALUE)),
            context.storage.getClusterScheduleStatuses(cluster.get().getName()),
            getNodesStatus(cluster).orNull());

      return Response.ok().entity(clusterStatus).build();
    }
  }

  @GET
  @Path("/{cluster_name}/tables")
  public Response getClusterTables(@PathParam("cluster_name") String clusterName)
      throws ReaperException {
    Map<String, List<String>> tablesByKeyspace = Maps.newHashMap();

    Optional<Cluster> cluster = context.storage.getCluster(clusterName);
    if (cluster.isPresent()) {
      try {
        JmxProxy jmxProxy =
            context.jmxConnectionFactory.connectAny(
                cluster.get(), context.config.getJmxConnectionTimeoutInSeconds());
        tablesByKeyspace = jmxProxy.listTablesByKeyspace();
      } catch (RuntimeException e) {
        LOG.error("Couldn't retrieve the list of tables for cluster {}", clusterName, e);
        return Response.status(400).entity(e).build();
      }
    }

    return Response.ok().entity(tablesByKeyspace).build();
  }

  @POST
  public Response addOrUpdateCluster(
      @Context UriInfo uriInfo,
      @QueryParam("seedHost") Optional<String> seedHost) {

    LOG.info("POST addOrUpdateCluster called with seedHost: {}", seedHost.orNull());
    return addOrUpdateCluster(uriInfo, Optional.absent(), seedHost);
  }

  @PUT
  @Path("/{cluster_name}")
  public Response addOrUpdateCluster(
      @Context UriInfo uriInfo,
      @PathParam("cluster_name") String clusterName,
      @QueryParam("seedHost") Optional<String> seedHost) {

    LOG.info("PUT addOrUpdateCluster called with: cluster_name = {}, seedHost = {}", clusterName, seedHost.orNull());
    return addOrUpdateCluster(uriInfo, Optional.of(clusterName), seedHost);
  }

  private Response addOrUpdateCluster(
      UriInfo uriInfo,
      Optional<String> clusterName,
      Optional<String> seedHost) {

    if (!seedHost.isPresent()) {
      LOG.error("POST/PUT on cluster resource {} called without seedHost", clusterName.orNull());
      return Response.status(Response.Status.BAD_REQUEST).entity("query parameter \"seedHost\" required").build();
    }

    try {
      Cluster cluster = findClusterWithSeedHost(seedHost.get());
      if (null == cluster) {
        String msg = String.format("failed to find cluster %s with seed host %s", clusterName.or(""), seedHost.get());
        return Response.status(Response.Status.BAD_REQUEST).entity(msg).build();
      }
      if (clusterName.isPresent() && !cluster.getName().equals(clusterName.get())) {

        String msg = String.format(
            "POST/PUT on cluster resource %s called with seedHost %s belonging to different cluster %s",
            clusterName.get(),
            seedHost.get(),
            cluster.getName());

        LOG.info(msg);
        return Response.status(Response.Status.BAD_REQUEST).entity(msg).build();
      }
      Optional<Cluster> existingCluster = context.storage.getCluster(cluster.getName());
      URI location = uriInfo.getBaseUriBuilder().path("cluster").path(cluster.getName()).build();
      if (existingCluster.isPresent()) {
        LOG.debug("Attempting updating nodelist for cluster {}", existingCluster.get().getName());

        // the cluster is already managed by reaper. if nothing is changed return 204. then if updated return 200.
        cluster = updateClusterSeeds(existingCluster.get(), seedHost.get());
        if (cluster.getSeedHosts().equals(existingCluster.get().getSeedHosts())) {
          LOG.debug("Nodelist of cluster {} is already up to date.", existingCluster.get().getName());
          return Response.noContent().location(location).build();
        } else {
          LOG.info("Nodelist of cluster {} updated", existingCluster.get().getName());
          return Response.ok().location(location).build();
        }

      } else {
        LOG.info("creating new cluster based on given seed host: {}", cluster.getName());
        context.storage.addCluster(cluster);

        if (context.config.hasAutoSchedulingEnabled()) {
          try {
            clusterRepairScheduler.scheduleRepairs(cluster);
          } catch (ReaperException e) {
            String msg = String.format(
                "failed to automatically schedule repairs for cluster %s with seed host %s",
                clusterName.or(""),
                seedHost.get());

            LOG.error(msg, e);
            return Response.serverError().entity(msg).build();
          }
        }
      }
      return Response.created(location).build();

    } catch (ReaperException e) {
      String msg = String.format("update cluster failed, %s with seed host %s", clusterName.or(""), seedHost.get());
      LOG.error(msg, e);
      return Response.serverError().entity(msg).build();
    }
  }

  @Nullable // if cluster can't be found
  private Cluster findClusterWithSeedHost(String seedHost) {
    Optional<String> clusterName = Optional.absent();
    Optional<String> partitioner = Optional.absent();
    Optional<List<String>> liveNodes = Optional.absent();

    Set<String> seedHosts = parseSeedHosts(seedHost);
    String cluster = parseClusterNameFromSeedHost(seedHost).or("");

    try {
      JmxProxy jmxProxy = connectAny(seedHosts, cluster);
      clusterName = Optional.of(jmxProxy.getClusterName());
      partitioner = Optional.of(jmxProxy.getPartitioner());
      liveNodes = Optional.of(jmxProxy.getLiveNodes());
    } catch (ReaperException e) {
      LOG.error("failed to find cluster with seed hosts: {}", seedHosts, e);
    }

    if (clusterName.isPresent()) {
      if (context.config.getEnableDynamicSeedList() && liveNodes.isPresent()) {
        seedHosts = !liveNodes.get().isEmpty() ? liveNodes.get().stream().collect(Collectors.toSet()) : seedHosts;
      }
      LOG.debug("Seeds {}", seedHosts);
    }
    return clusterName.isPresent() ? new Cluster(clusterName.get(), partitioner.get(), seedHosts) : null;
  }

  /**
   * Updates the list of nodes of a cluster based on the current topology.
   *
   * @param cluster the Cluster object we intend to update
   * @param seedHosts a list of hosts to connect to in the cluster
   * @return the updated cluster object with a refreshed seed list
   * @throws ReaperException failure to jmx connect/call to cluster
   */
  private Cluster updateClusterSeeds(Cluster cluster, String seedHosts) throws ReaperException {
    Set<String> newSeeds = parseSeedHosts(seedHosts);
    try {
      JmxProxy jmxProxy = connectAny(newSeeds,cluster.getName());
      Optional<List<String>> liveNodes = Optional.of(jmxProxy.getLiveNodes());
      newSeeds = liveNodes.get().stream().collect(Collectors.toSet());
      if (!cluster.getSeedHosts().equals(newSeeds)) {
        cluster = new Cluster(cluster.getName(), cluster.getPartitioner(), newSeeds);
        context.storage.updateCluster(cluster);
      }
      return cluster;
    } catch (ReaperException e) {
      throw new ReaperException(
          String.format("failed to update cluster %s with new seed hosts %s", cluster.getName(), seedHosts),
          e);
    }
  }

  /**
   * Delete a Cluster object with given name.
   *
   * <p>
   * Cluster can be only deleted when it hasn't any RepairRun or RepairSchedule instances under it, i.e. you must
   * delete all repair runs and schedules first.
   *
   * @param clusterName The name of the Cluster instance you are about to delete.
   */
  @DELETE
  @Path("/{cluster_name}")
  public Response deleteCluster(
      @PathParam("cluster_name") String clusterName) {

    LOG.info("delete cluster {}", clusterName);
    Optional<Cluster> clusterToDelete = context.storage.getCluster(clusterName);
    if (!clusterToDelete.isPresent()) {
      return Response.status(Response.Status.NOT_FOUND)
          .entity("cluster \"" + clusterName + "\" not found")
          .build();
    }
    if (!context.storage.getRepairSchedulesForCluster(clusterName).isEmpty()) {
      return Response.status(Response.Status.CONFLICT)
          .entity("cluster \"" + clusterName + "\" cannot be deleted, as it has repair schedules")
          .build();
    }
    if (!context.storage.getRepairRunsForCluster(clusterName, Optional.absent()).isEmpty()) {
      return Response.status(Response.Status.CONFLICT)
          .entity("cluster \"" + clusterName + "\" cannot be deleted, as it has repair runs")
          .build();
    }
    context.storage.deleteCluster(clusterName);
    return Response.accepted().build();
  }

  /**
   * Callable to get and parse endpoint states through JMX
   *
   * @param seedHost The host address to connect to via JMX
   * @return An optional NodesStatus object with the status of each node in the cluster as seen from
   *     the seedHost node
   */
  private Callable<Optional<NodesStatus>> getEndpointState(List<String> seeds, String clusterName) {
    return () -> {
      try {
        JmxProxy jmxProxy = connectAny(seeds, clusterName);
        Optional<String> allEndpointsState = Optional.fromNullable(jmxProxy.getAllEndpointsState());
        Optional<Map<String, String>> simpleStates = Optional.fromNullable(jmxProxy.getSimpleStates());

        return Optional.of(
            new NodesStatus(jmxProxy.getHost(), allEndpointsState.or(""), simpleStates.or(new HashMap<>())));

      } catch (RuntimeException e) {
        LOG.debug("failed to get endpoints for cluster {} with seeds {}", clusterName, seeds, e);
        Thread.sleep((int) JmxProxy.DEFAULT_JMX_CONNECTION_TIMEOUT.getSeconds() * 1000);
        return Optional.absent();
      }
    };
  }

  /**
   * Get all nodes state by querying the AllEndpointsState attribute through JMX.
   *
   * <p>
   * To speed up execution, the method calls JMX on 3 nodes asynchronously and processes the first response
   *
   * @return An optional NodesStatus object with all nodes statuses
   */
  public Optional<NodesStatus> getNodesStatus(Optional<Cluster> cluster) {
    if (cluster.isPresent() && null != cluster.get().getSeedHosts()) {
      List<String> seedHosts = Lists.newArrayList(cluster.get().getSeedHosts());
      Callable<Optional<NodesStatus>> callable = getEndpointState(seedHosts, cluster.get().getName());
      List<Callable<Optional<NodesStatus>>> endpointStateTasks = Lists.newArrayList(callable, callable, callable);

      try {
        return executor.invokeAny(
            endpointStateTasks,
            (int) JmxProxy.DEFAULT_JMX_CONNECTION_TIMEOUT.getSeconds(),
            TimeUnit.SECONDS);

      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        LOG.debug("failed grabbing nodes status", e);
      }
    }
    return Optional.absent();
  }

  /*
   * Creates a Set of seed hosts based on the comma delimited string passed
   * as argument when adding a cluster.
   */
  static Set<String> parseSeedHosts(String seedHost) {
    return Arrays.stream(seedHost.split(","))
        .map(String::trim)
        .map(host -> parseSeedHost(host))
        .collect(Collectors.toSet());
  }

  /*
   * Due to constraints with JMX credentials, we can get seed hosts
   * with the cluster name attached, after a @ character.
   */
  static String parseSeedHost(String seedHost) {
    return seedHost.split("@")[0];
  }

  /*
   * To support different credentials for different clusters,
   * we must allow to indicate the name of the cluster in the seed host address
   * so that we can get credentials from the config yaml for that cluster.
   * Seed host can take the following form : 127.0.0.1@my-cluster
   */
  static Optional<String> parseClusterNameFromSeedHost(String seedHost) {
    if (seedHost.contains("@")) {
      List<String> hosts = Arrays.stream(seedHost.split(",")).map(String::trim).collect(Collectors.toList());
      if (!hosts.isEmpty()) {
        return Optional.of(hosts.get(0).split("@")[1]);
      }
    }

    return Optional.absent();
  }

  private JmxProxy connectAny(Collection<String> seedHosts, String cluster) throws ReaperException {
    return context.jmxConnectionFactory.connectAny(
        seedHosts
            .stream()
            .map(host -> Node.builder().withClusterName(cluster).withHostname(parseSeedHost(host)).build())
            .collect(Collectors.toList()),
        context.config.getJmxConnectionTimeoutInSeconds());
  }
}
