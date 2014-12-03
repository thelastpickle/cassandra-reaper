package com.spotify.reaper;

import com.spotify.reaper.resources.ClusterResource;
import com.spotify.reaper.resources.PingResource;
import com.spotify.reaper.resources.TableResource;
import com.spotify.reaper.storage.IStorage;
import com.spotify.reaper.storage.MemoryStorage;
import com.spotify.reaper.storage.PostgresStorage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

public class ReaperApplication extends Application<ReaperApplicationConfiguration> {

  private static final Logger LOG = LoggerFactory.getLogger(ReaperApplication.class);

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
  public void run(ReaperApplicationConfiguration config,
                  Environment environment) throws ReaperException {

    IStorage storage = initializeStorage(config, environment);

    final PingResource pingResource = new PingResource();
    final ClusterResource addClusterResource = new ClusterResource(storage);
    final TableResource addTableResource = new TableResource(storage);

    environment.jersey().register(pingResource);
    environment.jersey().register(addClusterResource);
    environment.jersey().register(addTableResource);
  }

  private IStorage initializeStorage(ReaperApplicationConfiguration config,
                                     Environment environment) throws ReaperException {
    if (config.getStorageType().equalsIgnoreCase("memory")) {
      return new MemoryStorage();
    }
    else if (config.getStorageType().equalsIgnoreCase("database")) {
      return new PostgresStorage(config, environment);
    }
    else {
      LOG.error("invalid storageType: {}", config.getStorageType());
      throw new ReaperException("invalid storage type: " + config.getStorageType());
    }
  }

}
