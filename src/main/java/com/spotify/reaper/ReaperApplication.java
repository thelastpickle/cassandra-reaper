package com.spotify.reaper;

import com.spotify.reaper.resources.PingResource;

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
    final PingResource resource = new PingResource();
    environment.jersey().register(resource);
  }

}
