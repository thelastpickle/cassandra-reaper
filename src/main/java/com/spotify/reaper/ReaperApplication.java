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

import com.google.common.annotations.VisibleForTesting;

import com.spotify.reaper.cassandra.JmxConnectionFactory;
import com.spotify.reaper.resources.ClusterResource;
import com.spotify.reaper.resources.PingResource;
import com.spotify.reaper.resources.ReaperHealthCheck;
import com.spotify.reaper.resources.RepairRunResource;
import com.spotify.reaper.resources.RepairScheduleResource;
import com.spotify.reaper.service.RepairRunner;
import com.spotify.reaper.service.SchedulingManager;
import com.spotify.reaper.storage.IStorage;
import com.spotify.reaper.storage.MemoryStorage;
import com.spotify.reaper.storage.PostgresStorage;

import org.apache.cassandra.repair.RepairParallelism;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

public class ReaperApplication extends Application<ReaperApplicationConfiguration> {

  static final Logger LOG = LoggerFactory.getLogger(ReaperApplication.class);

  private static AppContext context;

  public ReaperApplication() {
    super();
    LOG.info("default ReaperApplication constructor called");
    ReaperApplication.context = new AppContext();
  }

  @VisibleForTesting
  public ReaperApplication(AppContext context) {
    super();
    LOG.info("ReaperApplication constructor called with custom AppContext");
    ReaperApplication.context = context;
  }

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
                  Environment environment) throws Exception {
    // Using UTC times everywhere as default. Affects only Yoda time.
    DateTimeZone.setDefault(DateTimeZone.UTC);

    checkConfiguration(config);
    context.config = config;

    addSignalHandlers(); // SIGHUP, etc.

    LOG.info("initializing runner thread pool with {} threads", config.getRepairRunThreadCount());
    RepairRunner.initializeThreadPool(
        config.getRepairRunThreadCount(),
        config.getHangingRepairTimeoutMins(), TimeUnit.MINUTES,
        30, TimeUnit.SECONDS);

    if (context.storage == null) {
      LOG.info("initializing storage of type: {}", config.getStorageType());
      context.storage = initializeStorage(config, environment);
    } else {
      LOG.info("storage already given in context, not initializing a new one");
    }

    if (context.jmxConnectionFactory == null) {
      LOG.info("no JMX connection factory given in context, creating default");
      context.jmxConnectionFactory = new JmxConnectionFactory();
    }

    // read jmx host/port mapping from config and provide to jmx con.factory
    Map<String, Integer> jmxPorts = config.getJmxPorts();
    if (jmxPorts != null) {
      LOG.debug("using JMX ports mapping: " + jmxPorts);
      context.jmxConnectionFactory.setJmxPorts(jmxPorts);
    }

    LOG.info("creating and registering health checks");
    // Notice that health checks are registered under the admin application on /healthcheck
    final ReaperHealthCheck healthCheck = new ReaperHealthCheck(context);
    environment.healthChecks().register("reaper", healthCheck);
    environment.jersey().register(healthCheck);

    LOG.info("creating resources and registering endpoints");
    final PingResource pingResource = new PingResource();
    environment.jersey().register(pingResource);

    final ClusterResource addClusterResource = new ClusterResource(context);
    environment.jersey().register(addClusterResource);

    final RepairRunResource addRepairRunResource = new RepairRunResource(context);
    environment.jersey().register(addRepairRunResource);

    final RepairScheduleResource addRepairScheduleResource = new RepairScheduleResource(context);
    environment.jersey().register(addRepairScheduleResource);
    Thread.sleep(1000);

    SchedulingManager.start(context);

    LOG.info("resuming pending repair runs");
    RepairRunner.resumeRunningRepairRuns(context);
  }

  private IStorage initializeStorage(ReaperApplicationConfiguration config,
                                     Environment environment) throws ReaperException {
    IStorage storage;
    if ("memory".equalsIgnoreCase(config.getStorageType())) {
      storage = new MemoryStorage();
    } else if ("database".equalsIgnoreCase(config.getStorageType())) {
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
    assert config.getRepairIntensity() > 0.0 && config.getRepairIntensity() <= 1.0 :
        "repairIntensity must be a value between 0.0 and 1.0, but not 0.";
    LOG.debug("repairRunThreadCount: " + config.getRepairRunThreadCount());
    LOG.debug("segmentCount: " + config.getSegmentCount());
    LOG.debug("repairParallelism: " + config.getRepairParallelism());
    LOG.debug("hangingRepairTimeoutMins: " + config.getHangingRepairTimeoutMins());
    LOG.debug("jmxPorts: " + config.getJmxPorts());
    checkRepairParallelismString(config.getRepairParallelism());
  }

  public static void checkRepairParallelismString(String givenRepairParallelism)
      throws ReaperException {
    try {
      RepairParallelism.valueOf(givenRepairParallelism.toUpperCase());
    } catch (java.lang.IllegalArgumentException ex) {
      throw new ReaperException(
          "invalid repair parallelism given \"" + givenRepairParallelism
          + "\", must be one of: " + Arrays.toString(RepairParallelism.values()));
    }
  }

  void reloadConfiguration() {
    // TODO: reload configuration, but how?
    LOG.warn("SIGHUP signal dropped, missing implementation for configuration reload");
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
