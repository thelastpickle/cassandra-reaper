package com.spotify.reaper.resources;

import com.google.common.base.Optional;

import com.spotify.reaper.core.Cluster;
import com.spotify.reaper.storage.IStorage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/table")
@Produces(MediaType.APPLICATION_JSON)
public class TableResource {

  private static final Logger LOG = LoggerFactory.getLogger(TableResource.class);

  private final IStorage storage;

  public TableResource(IStorage storage) {
    this.storage = storage;
  }

  @GET
  @Path("/{clusterName}/{keyspace}/{table}")
  public Response getCluster(@PathParam("clusterName") String clusterName,
                             @PathParam("keyspace") String keyspace,
                             @PathParam("table") String table) {
    LOG.info("get table called with: clusterName = {}, keyspace = {}, table = {}",
             clusterName, keyspace, table);
    return Response.ok().entity("not implemented yet").build();
  }

  @POST
  public Response addTable(@QueryParam("clusterName") Optional<String> clusterName,
                         @QueryParam("seedHost") Optional<String> seedHost,
                         @QueryParam("keyspace") Optional<String> keyspace,
                         @QueryParam("table") Optional<String> table) {
    LOG.info("add table called with: clusterName = {}, seedHost = {}, keyspace = {}, table = {}",
             clusterName, seedHost, keyspace, table);
    return Response.ok().entity("not implemented yet").build();
  }

}
