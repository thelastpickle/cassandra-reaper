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
import com.spotify.reaper.resources.ReaperHealthCheck;
import com.spotify.reaper.resources.RepairRunResource;
import com.spotify.reaper.resources.TableResource;
import com.spotify.reaper.cassandra.JmxConnectionFactory;
import com.spotify.reaper.service.RepairRunner;
import com.spotify.reaper.storage.IStorage;
import com.spotify.reaper.storage.MemoryStorage;
import com.spotify.reaper.storage.PostgresStorage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.util.concurrent.TimeUnit;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

public class ReaperApplication extends Application<ReaperApplicationConfiguration> {

  static final Logger LOG = LoggerFactory.getLogger(ReaperApplication.class);

  public static void main(String[] args) throws Exception {
    new ReaperApplication().run(args);
  }

  @Override
  public String getName() {
    return "cassandra-reaper";
  }

  /**
   * Before a Dropwizard application can provide the command-line interface, parse a configuration
   * file, or run as a server, it must first go through a bootstrapping phase. You can add Bundles,
   * Commands, or register Jackson modules to allow you to include custom types as part of your
   * configuration class.
   */
  @Override
  public void initialize(Bootstrap<ReaperApplicationConfiguration> bootstrap) {
    // nothing to initialize
  }

  @Override
  public void run(ReaperApplicationConfiguration config,
                  Environment environment) throws ReaperException {
    checkConfiguration(config);

    addSignalHandlers(); // SIGHUP, etc.

    LOG.info("initializing runner thread pool with {} threads", config.getRepairRunThreadCount());
    RepairRunner.initializeThreadPool(config.getRepairRunThreadCount(),
        config.getHangingRepairTimeoutMins(), TimeUnit.MINUTES, 30, TimeUnit.SECONDS);
    JmxConnectionFactory jmxConnectionFactory = new JmxConnectionFactory();

    LOG.info("initializing storage of type: {}", config.getStorageType());
    IStorage storage = initializeStorage(config, environment);

    LOG.info("creating and registering health checks");
    // Notice that health checks are registered under the admin application on /healthcheck
    final ReaperHealthCheck healthCheck = new ReaperHealthCheck(storage);
    environment.healthChecks().register("reaper", healthCheck);
    environment.jersey().register(healthCheck);

    LOG.info("creating resources and registering endpoints");
    final PingResource pingResource = new PingResource();
    environment.jersey().register(pingResource);

    final ClusterResource addClusterResource = new ClusterResource(storage, jmxConnectionFactory);
    environment.jersey().register(addClusterResource);

    final TableResource addTableResource = new TableResource(config, storage);
    environment.jersey().register(addTableResource);

    final RepairRunResource addRepairRunResource = new RepairRunResource(config, storage);
    environment.jersey().register(addRepairRunResource);

    LOG.info("Reaper is ready to accept connections");

    LOG.info("resuming pending repair runs");
    RepairRunner.resumeRunningRepairRuns(storage, jmxConnectionFactory);
  }

  private IStorage initializeStorage(ReaperApplicationConfiguration config,
                                     Environment environment) throws ReaperException {
    IStorage storage;
    if (config.getStorageType().equalsIgnoreCase("memory")) {
      storage = new MemoryStorage();
    } else if (config.getStorageType().equalsIgnoreCase("database")) {
      storage = new PostgresStorage(config, environment);
    } else {
      LOG.error("invalid storageType: {}", config.getStorageType());
      throw new ReaperException("invalid storage type: " + config.getStorageType());
    }
    assert storage.isStorageConnected() : "Failed to connect storage";
    return storage;
  }

  private void checkConfiguration(ReaperApplicationConfiguration config) throws ReaperException {
    LOG.debug("repairIntensity: " + config.getRepairIntensity());
    LOG.debug("repairRunThreadCount: " + config.getRepairRunThreadCount());
    LOG.debug("segmentCount: " + config.getSegmentCount());
    LOG.debug("snapshotRepair: " + config.getSnapshotRepair());
    LOG.debug("hangingRepairTimeoutMins: " + config.getHangingRepairTimeoutMins());
  }

  void reloadConfiguration() {
    // TODO: reload configuration, but how?
    LOG.warn("SIGHUP signal dropped, missing implementation");
  }

  private void addSignalHandlers() {
    LOG.debug("adding signal handler for SIGHUP");
    Signal.handle(new Signal("HUP"), new SignalHandler() {
      @Override
      public void handle(Signal signal) {
        LOG.info("received SIGHUP signal: {}", signal);
        reloadConfiguration();
      }
    });
  }
}
