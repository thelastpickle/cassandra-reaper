package com.spotify.reaper.resources;

import com.google.common.base.Optional;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

@Path("/add_cluster")
@Produces(MediaType.TEXT_PLAIN)
public class AddClusterResource {

  @POST
  public String addCluster(@QueryParam("host") Optional<String> host) {
    return String.format("Not implemented yet");
  }

}
