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
import com.spotify.reaper.cassandra.JmxProxy;
import com.spotify.reaper.core.Cluster;
import com.spotify.reaper.resources.view.ClusterStatus;
import com.spotify.reaper.storage.IStorage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

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

  private final IStorage storage;

  public ClusterResource(IStorage storage) {
    this.storage = storage;
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
    Cluster cluster = storage.getCluster(clusterName);
    ClusterStatus view = new ClusterStatus(cluster);
    view.setRepairRunIds(storage.getRepairRunIdsForCluster(cluster.getName()));
    return Response.ok().entity(view).build();
  }

  @POST
  public Response addCluster(@Context UriInfo uriInfo,
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
    Cluster existingCluster = storage.getCluster(newCluster.getName());
    if (existingCluster == null) {
      LOG.info("creating new cluster based on given seed host: {}", newCluster);
      storage.addCluster(newCluster);
    } else {
      LOG.info("cluster already stored with this name: {}", existingCluster);
      return Response.status(403)
          .entity(String.format("cluster \"%s\" already exists", existingCluster.getName()))
          .build();
    }

    URI createdURI = null;
    try {
      createdURI = (new URL(uriInfo.getAbsolutePath().toURL(), newCluster.getName())).toURI();
    } catch (Exception e) {
      String errMsg = "failed creating target URI for cluster: " + newCluster.getName();
      LOG.error(errMsg);
      e.printStackTrace();
      return Response.status(400).entity(errMsg).build();
    }

    return Response.created(createdURI).entity(new ClusterStatus(newCluster)).build();
  }

  public static Cluster createClusterWithSeedHost(String seedHost)
      throws ReaperException {
    String clusterName;
    String partitioner;
    try {
      JmxProxy jmxProxy = JmxProxy.connect(seedHost);
      clusterName = jmxProxy.getClusterName();
      partitioner = jmxProxy.getPartitioner();
      jmxProxy.close();
    } catch (ReaperException e) {
      LOG.error("failed to create cluster with seed host: " + seedHost);
      e.printStackTrace();
      throw e;
    }
    Cluster newCluster =
        new Cluster(clusterName, partitioner, Collections.singleton(seedHost));
    return newCluster;
  }

}
