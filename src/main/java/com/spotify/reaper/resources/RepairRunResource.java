package com.spotify.reaper.resources;

import com.google.common.base.Optional;

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

@Path("/repair_run")
@Produces(MediaType.APPLICATION_JSON)
public class RepairRunResource {

  private static final Logger LOG = LoggerFactory.getLogger(RepairRunResource.class);

  private final IStorage storage;

  public RepairRunResource(IStorage storage) {
    this.storage = storage;
  }

  @GET
  @Path("/{id}")
  public Response getCluster(@PathParam("id") Long repairRunId) {
    LOG.info("get repair_run called with: id = {}", repairRunId);
    return Response.ok().entity("not implemented yet").build();
  }

  // We probably don't want to create repair runs with this resource,
  // but actually only by posting the cluster resource.
  // Get here is used only for providing visibility to what is going on with the run.
}
