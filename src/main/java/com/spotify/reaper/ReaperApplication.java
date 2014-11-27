package com.spotify.reaper;

import com.spotify.reaper.resources.AddClusterResource;
import com.spotify.reaper.resources.AddTableResource;
import com.spotify.reaper.resources.PingResource;
import com.spotify.reaper.resources.RepairTableResource;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

public class ReaperApplication extends Application<ReaperApplicationConfiguration> {

  public static void main(String[] args) throws Exception {
    new ReaperApplication().run(args);
  }

  @Override
  public String getName() {
    return "cassandra-reaper";
  }

  @Override
  public void initialize(Bootstrap<ReaperApplicationConfiguration> bootstrap) {
    // nothing to do yet
  }

  @Override
  public void run(ReaperApplicationConfiguration configuration,
                  Environment environment) {
    final PingResource pingResource = new PingResource();
    final AddClusterResource addClusterResource = new AddClusterResource();
    final AddTableResource addTableResource = new AddTableResource();
    final RepairTableResource repairTableResource = new RepairTableResource();
    environment.jersey().register(pingResource);
    environment.jersey().register(addClusterResource);
    environment.jersey().register(addTableResource);
    environment.jersey().register(repairTableResource);
  }

}
