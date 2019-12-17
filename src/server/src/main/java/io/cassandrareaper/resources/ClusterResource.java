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
import io.cassandrareaper.jmx.ClusterFacade;
import io.cassandrareaper.jmx.JmxProxy;
import io.cassandrareaper.resources.view.ClusterStatus;
import io.cassandrareaper.resources.view.NodesStatus;
import io.cassandrareaper.service.ClusterRepairScheduler;

import java.net.URI;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
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

import com.codahale.metrics.InstrumentedExecutorService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/cluster")
@Produces(MediaType.APPLICATION_JSON)
public final class ClusterResource {

  private static final Logger LOG = LoggerFactory.getLogger(ClusterResource.class);

  private final AppContext context;
  private final ExecutorService executor;
  private final ClusterRepairScheduler clusterRepairScheduler;
  private final ClusterFacade clusterFacade;

  private ClusterResource(AppContext context, ExecutorService executor, Supplier<ClusterFacade> clusterFacadeSupplier)
      throws ReaperException {
    this.context = context;
    this.executor = new InstrumentedExecutorService(executor, context.metricRegistry);
    this.clusterRepairScheduler = new ClusterRepairScheduler(context);
    this.clusterFacade = clusterFacadeSupplier.get();
  }

  @VisibleForTesting
  static ClusterResource create(AppContext context, ExecutorService executor, Supplier<ClusterFacade> supplier)
      throws ReaperException {
    return new ClusterResource(context, executor, supplier);
  }

  public static ClusterResource create(AppContext context, ExecutorService executor) throws ReaperException {
    return new ClusterResource(context, executor, () -> ClusterFacade.create(context));
  }

  @GET
  public Response getClusterList(@QueryParam("seedHost") Optional<String> seedHost) {
    LOG.debug("get cluster list called");

    Collection<String> clusters = context.storage.getClusters()
        .stream()
        .filter(c -> !seedHost.isPresent() || c.getSeedHosts().contains(seedHost.get()))
        .sorted()
        .map(c -> c.getName())
        .collect(Collectors.toList());

    return Response.ok().entity(clusters).build();
  }

  @GET
  @Path("/{cluster_name}")
  public Response getCluster(
      @PathParam("cluster_name") String clusterName,
      @QueryParam("limit") Optional<Integer> limit) {

    LOG.debug("get cluster called with cluster_name: {}", clusterName);
    try {
      String jmxUsername = "";
      boolean jmxPasswordIsSet = false;

      if (context.jmxConnectionFactory.getJmxCredentialsForCluster(clusterName).isPresent()) {
        jmxUsername = context.jmxConnectionFactory.getJmxCredentialsForCluster(clusterName).get().getUsername();

        jmxPasswordIsSet = !StringUtils.isEmpty(
            context.jmxConnectionFactory.getJmxCredentialsForCluster(clusterName).get().getPassword());
      }
      Cluster cluster = context.storage.getCluster(clusterName);

      ClusterStatus clusterStatus = new ClusterStatus(
            cluster,
            jmxUsername,
            jmxPasswordIsSet,
            context.storage.getClusterRunStatuses(cluster.getName(), limit.orElse(Integer.MAX_VALUE)),
            context.storage.getClusterScheduleStatuses(cluster.getName()),
            getNodesStatus(cluster));

      return Response.ok().entity(clusterStatus).build();
    } catch (IllegalArgumentException ignore) { }
    return Response.status(404).entity("cluster with name \"" + clusterName + "\" not found").build();
  }

  @GET
  @Path("/{cluster_name}/tables")
  public Response getClusterTables(@PathParam("cluster_name") String clusterName) throws ReaperException {
    try {
      return Response.ok()
          .entity(ClusterFacade.create(context).listTablesByKeyspace(context.storage.getCluster(clusterName)))
          .build();
    } catch (IllegalArgumentException ex) {
      return Response.status(404).entity(ex).build();
    }
  }

  @POST
  public Response addOrUpdateCluster(
      @Context UriInfo uriInfo,
      @QueryParam("seedHost") Optional<String> seedHost,
      @QueryParam("jmxPort") Optional<Integer> jmxPort) {

    LOG.info("POST addOrUpdateCluster called with seedHost: {}", seedHost.orElse(null));
    return addOrUpdateCluster(uriInfo, Optional.empty(), seedHost, jmxPort);
  }

  @PUT
  @Path("/{cluster_name}")
  public Response addOrUpdateCluster(
      @Context UriInfo uriInfo,
      @PathParam("cluster_name") String clusterName,
      @QueryParam("seedHost") Optional<String> seedHost,
      @QueryParam("jmxPort") Optional<Integer> jmxPort) {

    LOG.info(
        "PUT addOrUpdateCluster called with: cluster_name = {}, seedHost = {}",
        clusterName, seedHost.orElse(null));

    return addOrUpdateCluster(uriInfo, Optional.of(clusterName), seedHost, jmxPort);
  }

  private Response addOrUpdateCluster(
      UriInfo uriInfo,
      Optional<String> clusterName,
      Optional<String> seedHost,
      Optional<Integer> jmxPort) {

    if (!seedHost.isPresent()) {
      LOG.error("POST/PUT on cluster resource {} called without seedHost", clusterName.orElse(null));
      return Response.status(Response.Status.BAD_REQUEST).entity("query parameter \"seedHost\" required").build();
    }

    final Optional<Cluster> cluster = findClusterWithSeedHost(seedHost.get(), jmxPort);
    if (!cluster.isPresent()) {
      return Response
          .status(Response.Status.BAD_REQUEST)
          .entity(String.format("no cluster %s with seed host %s", clusterName.orElse(""), seedHost.get()))
          .build();
    }
    if (clusterName.isPresent() && !cluster.get().getName().equals(clusterName.get())) {
      String msg = String.format(
          "POST/PUT on cluster resource %s called with seedHost %s belonging to different cluster %s",
          clusterName.get(),
          seedHost.get(),
          cluster.get().getName());

      LOG.info(msg);
      return Response.status(Response.Status.BAD_REQUEST).entity(msg).build();
    }

    Optional<Cluster> existingCluster = context.storage.getClusters().stream()
        .filter(c -> c.getName().equalsIgnoreCase(cluster.get().getName()))
        .findAny();

    URI location = uriInfo.getBaseUriBuilder().path("cluster").path(cluster.get().getName()).build();
    if (existingCluster.isPresent()) {
      LOG.debug("Attempting updating nodelist for cluster {}", existingCluster.get().getName());
      try {
        // the cluster is already managed by reaper. if nothing is changed return 204. then if updated return 200.
        Cluster updatedCluster = updateClusterSeeds(existingCluster.get(), seedHost.get());
        if (updatedCluster.getSeedHosts().equals(existingCluster.get().getSeedHosts())) {
          LOG.debug("Nodelist of cluster {} is already up to date.", existingCluster.get().getName());
          return Response.noContent().location(location).build();
        } else {
          LOG.info("Nodelist of cluster {} updated", existingCluster.get().getName());
          return Response.ok().location(location).build();
        }
      } catch (ReaperException ex) {
        LOG.error("fail:", ex);
        return Response.serverError().entity(ex.getMessage()).build();
      }
    } else {
      LOG.info("creating new cluster based on given seed host: {}", cluster.get().getName());
      context.storage.addCluster(cluster.get());

      if (context.config.hasAutoSchedulingEnabled()) {
        try {
          clusterRepairScheduler.scheduleRepairs(cluster.get());
        } catch (ReaperException e) {
          String msg = String.format(
              "failed to automatically schedule repairs for cluster %s with seed host %s",
              clusterName.orElse(""),
              seedHost.get());

          LOG.error(msg, e);
          return Response.serverError().entity(msg).build();
        }
      }
    }
    return Response.created(location).build();
  }

  public Optional<Cluster> findClusterWithSeedHost(String seedHost, Optional<Integer> jmxPort) {
    Set<String> seedHosts = parseSeedHosts(seedHost);
    try {
      Cluster cluster = Cluster.builder()
          .withName(parseClusterNameFromSeedHost(seedHost).orElse(""))
          .withSeedHosts(ImmutableSet.of(seedHost))
          .withJmxPort(jmxPort.orElse(Cluster.DEFAULT_JMX_PORT))
          .build();

      String clusterName = clusterFacade.getClusterName(cluster, seedHosts);
      String partitioner = clusterFacade.getPartitioner(cluster, seedHosts);
      List<String> liveNodes = clusterFacade.getLiveNodes(cluster, seedHosts);

      if (context.config.getEnableDynamicSeedList() && !liveNodes.isEmpty()) {
        seedHosts = ImmutableSet.copyOf(liveNodes);
      }
      LOG.debug("Cluster {}", seedHosts);

      return Optional.of(Cluster.builder()
              .withName(clusterName)
              .withPartitioner(partitioner)
              .withSeedHosts(seedHosts)
              .withJmxPort(jmxPort.orElse(Cluster.DEFAULT_JMX_PORT))
              .withState(Cluster.State.ACTIVE)
              .withLastContact(LocalDate.now())
              .build());
    } catch (ReaperException e) {
      LOG.error("failed to find cluster with seed hosts: {}", seedHosts, e);
    }
    return Optional.empty();
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
      Set<String> previousNodes = Collections.emptySet();
      try {
        previousNodes = ImmutableSet.copyOf(clusterFacade.getLiveNodes(cluster));
      } catch (ReaperException expected) {
        // The previous node could all be gone, and that's fine.
      }
      Set<String> liveNodes = ImmutableSet.copyOf(clusterFacade.getLiveNodes(cluster, newSeeds));

      Preconditions.checkArgument(
          !Collections.disjoint(previousNodes, liveNodes) || previousNodes.isEmpty(),
          "Trying to update a different cluster using the same name: %s. No nodes overlap between %s and %s",
          cluster.getName(), StringUtils.join(previousNodes, ','), StringUtils.join(liveNodes, ','));

      if (!cluster.getSeedHosts().equals(liveNodes)) {
        cluster = cluster.with()
              .withSeedHosts(liveNodes)
              .withState(Cluster.State.ACTIVE)
              .withLastContact(LocalDate.now())
              .build();

        context.storage.updateCluster(cluster);
      }
      return cluster;
    } catch (ReaperException e) {
      String err = String.format("failed to update cluster %s from new seed hosts %s", cluster.getName(), seedHosts);
      throw new ReaperException(err, e);
    }
  }

  /**
   * Delete a Cluster object with given name.
   *
   * <p>Cluster can only be forced deleted when it has any RepairRuns or RepairSchedule instances associated to it.
   */
  @DELETE
  @Path("/{cluster_name}")
  public Response deleteCluster(
      @PathParam("cluster_name") String clusterName,
      @QueryParam("force") Optional<Boolean> force) {

    LOG.info("delete cluster {}", clusterName);
    try {
      if (!force.orElse(Boolean.FALSE)) {
        if (!context.storage.getRepairSchedulesForCluster(clusterName).isEmpty()) {
          return Response.status(Response.Status.CONFLICT)
              .entity("cluster \"" + clusterName + "\" cannot be deleted, as it has repair schedules")
              .build();
        }
        if (!context.storage.getRepairRunsForCluster(clusterName, Optional.empty()).isEmpty()) {
          return Response.status(Response.Status.CONFLICT)
              .entity("cluster \"" + clusterName + "\" cannot be deleted, as it has repair runs")
              .build();
        }
        if (!context.storage.getEventSubscriptions(clusterName).isEmpty()) {
          return Response.status(Response.Status.CONFLICT)
              .entity("cluster \"" + clusterName + "\" cannot be deleted, as it has diagnostic events subscriptions")
              .build();
        }
      }
      if (context.storage.getRepairRunsWithState(RepairRun.RunState.RUNNING)
          .stream()
          .anyMatch(run -> "clusterName".equals(run.getClusterName()))) {

        return Response.status(Response.Status.CONFLICT)
            .entity("cluster \"" + clusterName + "\" cannot be deleted, as it has running repairs. Stop them first.")
            .build();
      }

      context.storage.deleteCluster(clusterName);
      return Response.accepted().build();
    } catch (IllegalArgumentException ex) {
      return Response.status(Response.Status.NOT_FOUND)
          .entity("cluster \"" + clusterName + "\" not found")
          .build();
    }
  }

  /**
   * Callable to get and parse endpoint states through JMX
   *
   * @param jmxPort Optional jmx port to connect to
   * @param seedHost The host address to connect to via JMX
   * @return An optional NodesStatus object with the status of each node in the cluster as seen from
   *     the seedHost node
   */
  private Callable<NodesStatus> getEndpointState(Set<String> seeds, String clusterName, int jmxPort) {
    Cluster cluster = Cluster.builder().withName(clusterName).withSeedHosts(seeds).withJmxPort(jmxPort).build();

    return () -> {
      try {
        return clusterFacade.getNodesStatus(cluster, seeds);
      } catch (RuntimeException e) {
        LOG.debug("failed to get endpoints for cluster {} with seeds {}", clusterName, seeds, e);
        Thread.sleep((int) JmxProxy.DEFAULT_JMX_CONNECTION_TIMEOUT.getSeconds() * 1000);
        return new NodesStatus(Collections.EMPTY_LIST);
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
  private NodesStatus getNodesStatus(Cluster cluster) {
    List<Callable<NodesStatus>> endpointStateTasks = Lists.newArrayList();
    List<String> seedHosts = new ArrayList<>(cluster.getSeedHosts());
    Collections.shuffle(seedHosts);
    int index = 0;
    for (String host : seedHosts) {
      if (index >= 3) {
        break;
      }
      endpointStateTasks.add(getEndpointState(Collections.singleton(host), cluster.getName(), cluster.getJmxPort()));
      index++;
    }

    try {
      return executor.invokeAny(
          endpointStateTasks,
          (int) JmxProxy.DEFAULT_JMX_CONNECTION_TIMEOUT.getSeconds(),
          TimeUnit.SECONDS);

    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      LOG.debug("failed grabbing nodes status", e);
    }
    return new NodesStatus(Collections.EMPTY_LIST);
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
    return Iterables.get(Splitter.on('@').split(seedHost), 0);
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
        return Optional.of(Iterables.get(Splitter.on('@').split(hosts.get(0)), 1));
      }
    }
    return Optional.empty();
  }
}
