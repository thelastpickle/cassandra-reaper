package com.spotify.reaper.resources;

import com.google.common.base.Optional;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

@Path("/repair_table")
@Produces(MediaType.TEXT_PLAIN)
public class RepairTableResource {

  @POST
  public String addTable(@QueryParam("repair_run_id") Optional<String> repairRunID) {

    return String.format("Not implemented yet");
  }

}
