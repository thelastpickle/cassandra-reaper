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
import com.spotify.reaper.ReaperException;
import com.spotify.reaper.cassandra.JmxConnectionFactory;
import com.spotify.reaper.cassandra.JmxProxy;
import com.spotify.reaper.core.Cluster;
import com.spotify.reaper.resources.view.ClusterStatus;
import com.spotify.reaper.storage.IStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@Path("/cluster")
@Produces(MediaType.APPLICATION_JSON)
public class ClusterResource {

  private static final Logger LOG = LoggerFactory.getLogger(ClusterResource.class);

  private final JmxConnectionFactory jmxFactory;
  private final IStorage storage;

  public ClusterResource(IStorage storage, JmxConnectionFactory jmxFactory) {
    this.storage = storage;
    this.jmxFactory = jmxFactory;
  }

  @GET
  public Response getClusterList() {
    LOG.info("get cluster list called");
    Collection<Cluster> clusters = storage.getClusters();
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
    Optional<Cluster> cluster = storage.getCluster(clusterName);
    if (cluster.isPresent()) {
      return viewCluster(cluster.get(), Optional.<URI>absent());
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
      newCluster = ResourceUtils.createClusterWithSeedHost(seedHost.get(), jmxFactory);
    } catch (ReaperException e) {
      return Response.status(400)
          .entity("failed to create cluster with seed host: " + seedHost.get()).build();
    }
    Optional<Cluster> existingCluster = storage.getCluster(newCluster.getName());
    if (existingCluster.isPresent()) {
      LOG.info("cluster already stored with this name: {}", existingCluster);
      return Response.status(403)
          .entity(String.format("cluster \"%s\" already exists", existingCluster.get().getName()))
          .build();
    } else {
      LOG.info("creating new cluster based on given seed host: {}", newCluster);
      storage.addCluster(newCluster);
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

  private Response viewCluster(Cluster cluster, Optional<URI> createdURI) {
    ClusterStatus view = new ClusterStatus(cluster);
    view.setRepairRunIds(storage.getRepairRunIdsForCluster(cluster.getName()));
    try {
      JmxProxy jmx = this.jmxFactory.connectAny(cluster);
      view.setKeyspaces(jmx.getKeyspaces());
      jmx.close();
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

}
