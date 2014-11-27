package com.spotify.reaper.resources;

import com.google.common.base.Optional;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

@Path("/add_table")
@Produces(MediaType.TEXT_PLAIN)
public class AddTableResource {

  @POST
  public String addTable(@QueryParam("host") Optional<String> host,
                         @QueryParam("keyspace") Optional<String> keyspace,
                         @QueryParam("table") Optional<String> table) {
    return String.format("Not implemented yet");
  }

}
