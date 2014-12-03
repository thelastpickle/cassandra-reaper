package com.spotify.reaper.resources;

import com.google.common.base.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

@Path("/ping")
@Produces(MediaType.TEXT_PLAIN)
public class PingResource {

  private static final Logger LOG = LoggerFactory.getLogger(ClusterResource.class);

  @GET
  public String answerPing(@QueryParam("name") Optional<String> name) {
    LOG.info("ping called with name: {}", name);
    return String.format("Ping %s", name.or("stranger"));
  }

}
