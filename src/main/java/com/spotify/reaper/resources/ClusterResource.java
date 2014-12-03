package com.spotify.reaper.resources;

import com.google.common.base.Optional;

import com.spotify.reaper.ReaperException;
import com.spotify.reaper.cassandra.JmxProxy;
import com.spotify.reaper.core.Cluster;
import com.spotify.reaper.storage.IStorage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URL;
import java.util.Collections;

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
  @Path("/{name}")
  public Response getCluster(@PathParam("name") String name) {
    LOG.info("get cluster called with name: {}", name);
    Cluster cluster = storage.getCluster(name);
    return Response.ok().entity(cluster).build();
  }

  @POST
  public Response addCluster(@Context UriInfo uriInfo, @QueryParam("host") Optional<String> host) {
    if (!host.isPresent()) {
      LOG.error("POST on cluster resource called without host");
      return Response.status(400).entity("query parameter \"host\" required").build();
    }
    LOG.info("add cluster called with host: {}", host);

    String clusterName;
    String partitioner;
    try {
      JmxProxy jmxProxy = JmxProxy.connect(host.get());
      clusterName = jmxProxy.getClusterName();
      partitioner = jmxProxy.getPartitioner();
      jmxProxy.close();
    } catch (ReaperException e) {
      String errMsg = "failed to get cluster info from seed host: " + host.get();
      LOG.error(errMsg);
      e.printStackTrace();
      return Response.status(400).entity(errMsg).build();
    }

    URI createdURI = null;
    try {
      createdURI = (new URL(uriInfo.getAbsolutePath().toURL(), clusterName)).toURI();
    } catch (Exception e) {
      String errMsg = "failed creating target URI for cluster: " + clusterName;
      LOG.error(errMsg);
      e.printStackTrace();
      return Response.status(400).entity(errMsg).build();
    }

    storage.addCluster(new Cluster.Builder()
                           .name(clusterName)
                           .seedHosts(Collections.singleton(host.get()))
                           .partitioner(partitioner)
                           .build());

    String replyMsg = "cluster with name \"" + clusterName + "\" created";
    return Response.created(createdURI).entity(replyMsg).build();
  }

}
