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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.spotify.reaper.AppContext;
import com.spotify.reaper.ReaperException;
import com.spotify.reaper.cassandra.JmxProxy;
import com.spotify.reaper.core.Cluster;
import com.spotify.reaper.core.RepairRun;
import com.spotify.reaper.resources.view.ClusterStatus;
import com.spotify.reaper.resources.view.KeyspaceStatus;
import com.spotify.reaper.resources.view.hierarchy.HCluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
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

  private static final Logger LOG = LoggerFactory.getLogger(ClusterResource.class);

  private final AppContext context;

  public ClusterResource(AppContext context) {
    this.context = context;
  }

  @GET
  public Response getClusterList() {
    LOG.info("get cluster list called");
    Collection<Cluster> clusters = context.storage.getClusters();
    List<String> clusterNames = new ArrayList<>();
    for (Cluster cluster : clusters) {
      clusterNames.add(cluster.getName());
    }
    return Response.ok().entity(clusterNames).build();
  }

  @GET
  @Path("/{cluster_name}")
  public Response getCluster(@PathParam("cluster_name") String clusterName) {
    LOG.info("get cluster called with cluster_name: {}", clusterName);
    Optional<Cluster> cluster = context.storage.getCluster(clusterName);
    if (cluster.isPresent()) {
      return viewCluster(cluster.get(), Optional.<URI>absent());
    } else {
      return Response.status(Response.Status.NOT_FOUND)
          .entity("cluster with name \"" + clusterName + "\" not found").build();
    }
  }

  @GET
  @Path("/{cluster_name}/{keyspace_name}")
  public Response getCluster(@PathParam("cluster_name") String clusterName,
                             @PathParam("keyspace_name") String keyspaceName) {
    LOG.info("get cluster/keyspace called with cluster_name: {}, and keyspace_name: {}",
             clusterName, keyspaceName);
    Optional<Cluster> cluster = context.storage.getCluster(clusterName);
    if (cluster.isPresent()) {
      return viewKeyspace(cluster.get(), keyspaceName);
    } else {
      return Response.status(Response.Status.NOT_FOUND)
          .entity("cluster with name \"" + clusterName + "\" not found").build();
    }
  }

  @GET
  @Path("/{cluster_name}/hierarchy")
  public Response getClusterHierarchy(@PathParam("cluster_name") String clusterName) {
    LOG.info("get cluster overview called with cluster_name: {}", clusterName);
    Optional<Cluster> cluster = context.storage.getCluster(clusterName);
    if (cluster.isPresent()) {
      return viewClusterHierarchy(cluster.get());
    } else {
      return Response.status(Response.Status.NOT_FOUND)
          .entity("cluster with name \"" + clusterName + "\" not found").build();
    }
  }

  @POST
  public Response addCluster(
      @Context UriInfo uriInfo,
      @QueryParam("seedHost") Optional<String> seedHost) {
    if (!seedHost.isPresent()) {
      LOG.error("POST on cluster resource called without seedHost");
      return Response.status(400).entity("query parameter \"seedHost\" required").build();
    }
    LOG.info("add cluster called with seedHost: {}", seedHost.get());

    Cluster newCluster;
    try {
      newCluster = createClusterWithSeedHost(seedHost.get());
    } catch (ReaperException e) {
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
      LOG.info("creating new cluster based on given seed host: {}", newCluster);
      context.storage.addCluster(newCluster);
    }

    URI createdURI;
    try {
      createdURI = (new URL(uriInfo.getAbsolutePath().toURL(), newCluster.getName())).toURI();
    } catch (Exception e) {
      String errMsg = "failed creating target URI for cluster: " + newCluster.getName();
      LOG.error(errMsg);
      e.printStackTrace();
      return Response.status(400).entity(errMsg).build();
    }

    return viewCluster(newCluster, Optional.of(createdURI));
  }

  public Cluster createClusterWithSeedHost(String seedHost)
      throws ReaperException {
    String clusterName;
    String partitioner;
    try (JmxProxy jmxProxy = context.jmxConnectionFactory.connect(seedHost)) {
      clusterName = jmxProxy.getClusterName();
      partitioner = jmxProxy.getPartitioner();
    } catch (ReaperException e) {
      LOG.error("failed to create cluster with seed host: " + seedHost);
      e.printStackTrace();
      throw e;
    }
    return new Cluster(clusterName, partitioner, Collections.singleton(seedHost));
  }

  private Response viewCluster(Cluster cluster, Optional<URI> createdURI) {
    ClusterStatus view = new ClusterStatus(cluster);
    Collection<Collection<Object>> runIdTuples = Lists.newArrayList();
    for (Long repairRunId : context.storage.getRepairRunIdsForCluster(cluster.getName())) {
      Optional<RepairRun> repairRun = context.storage.getRepairRun(repairRunId);
      if (repairRun.isPresent()) {
        runIdTuples
            .add(Lists.newArrayList(new Object[]{repairRunId, repairRun.get().getRunState()}));
      }
    }
    view.setRepairRunIds(runIdTuples);
    try (JmxProxy jmx = context.jmxConnectionFactory.connectAny(cluster)) {
      view.setKeyspaces(jmx.getKeyspaces());
    } catch (ReaperException e) {
      e.printStackTrace();
      LOG.error("failed connecting JMX", e);
      return Response.status(500).entity("failed connecting given clusters JMX endpoint").build();
    }
    if (createdURI.isPresent()) {
      return Response.created(createdURI.get()).entity(view).build();
    } else {
      return Response.ok().entity(view).build();
    }
  }

  private Response viewKeyspace(Cluster cluster, String keyspaceName) {
    KeyspaceStatus view = new KeyspaceStatus(cluster);
    try (JmxProxy jmx = context.jmxConnectionFactory.connectAny(cluster)) {
      if (jmx.getKeyspaces().contains(keyspaceName)) {
        view.setTables(jmx.getTableNamesForKeyspace(keyspaceName));
      } else {
        return Response.status(Response.Status.NOT_FOUND)
            .entity("cluster with name \"" + cluster.getName() + "\" does not contain keyspace \""
                    + keyspaceName + "\"").build();
      }
    } catch (ReaperException e) {
      e.printStackTrace();
      LOG.error("failed connecting JMX", e);
      return Response.status(500).entity("failed connecting given clusters JMX endpoint").build();
    }
    return Response.ok().entity(view).build();
  }

  private Response viewClusterHierarchy(Cluster cluster) {
    HCluster view = new HCluster(context.storage, cluster.getName());
    try {
      ObjectMapper objectMapper = new ObjectMapper()
          .setPropertyNamingStrategy(
              PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);
      return Response.ok().entity(objectMapper.writeValueAsString(view)).build();
    } catch (JsonProcessingException e) {
      return Response.serverError().entity("JSON processing failed").build();
    }
  }

  /**
   * Delete a Cluster object with given name.
   *
   * Cluster can be only deleted when it hasn't any RepairRun or RepairSchedule instances under it,
   * i.e. you must delete all repair runs and schedules first.
   *
   * @param clusterName  The name of the Cluster instance you are about to delete.
   * @return The deleted RepairRun instance, with state overwritten to string "DELETED".
   */
  @DELETE
  @Path("/{cluster_name}")
  public Response deleteCluster(@PathParam("cluster_name") String clusterName) {
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
      return Response.ok().entity(new ClusterStatus(deletedCluster.get())).build();
    }
    return Response.serverError().entity("delete failed for schedule with name \""
                                         + clusterName + "\"").build();
  }

}
