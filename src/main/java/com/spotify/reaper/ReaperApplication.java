/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.spotify.reaper;

import com.spotify.reaper.resources.ClusterResource;
import com.spotify.reaper.resources.PingResource;
import com.spotify.reaper.resources.TableResource;
import com.spotify.reaper.service.RepairRunner;
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

    LOG.info("initializing runner thread pool with {} threads", config.getRepairRunThreadCount());
    RepairRunner.initializeThreadPool(config.getRepairRunThreadCount());

    LOG.info("initializing storage of type: {}", config.getStorageType());
    IStorage storage = initializeStorage(config, environment);

    LOG.info("creating resources and registering endpoints");
    final PingResource pingResource = new PingResource();
    final ClusterResource addClusterResource = new ClusterResource(storage);
    final TableResource addTableResource = new TableResource(config, storage);

    environment.jersey().register(pingResource);
    environment.jersey().register(addClusterResource);
    environment.jersey().register(addTableResource);

    LOG.info("Reaper is ready to accept connections");
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
