package com.spotify.reaper.resources;

import com.google.common.base.Optional;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

@Path("/ping")
@Produces(MediaType.TEXT_PLAIN)
public class PingResource {

  @GET
  public String answerPing(@QueryParam("name") Optional<String> name) {
    return String.format("Ping %s", name.or("stranger"));
  }

}
