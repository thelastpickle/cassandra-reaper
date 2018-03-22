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

package io.cassandrareaper;

import io.cassandrareaper.ReaperApplicationConfiguration.JmxCredentials;
import io.cassandrareaper.jmx.JmxConnectionFactory;
import io.cassandrareaper.jmx.JmxConnectionsInitializer;
import io.cassandrareaper.resources.ClusterResource;
import io.cassandrareaper.resources.PingResource;
import io.cassandrareaper.resources.ReaperHealthCheck;
import io.cassandrareaper.resources.RepairRunResource;
import io.cassandrareaper.resources.RepairScheduleResource;
import io.cassandrareaper.resources.SnapshotResource;
import io.cassandrareaper.resources.auth.LoginResource;
import io.cassandrareaper.resources.auth.ShiroExceptionMapper;
import io.cassandrareaper.service.AutoSchedulingManager;
import io.cassandrareaper.service.RepairManager;
import io.cassandrareaper.service.SchedulingManager;
import io.cassandrareaper.service.SnapshotManager;
import io.cassandrareaper.storage.CassandraStorage;
import io.cassandrareaper.storage.IDistributedStorage;
import io.cassandrareaper.storage.IStorage;
import io.cassandrareaper.storage.MemoryStorage;
import io.cassandrareaper.storage.PostgresStorage;

import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.servlet.DispatcherType;
import javax.servlet.FilterRegistration;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.datastax.driver.core.policies.EC2MultiRegionAddressTranslator;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.db.DataSourceFactory;
import io.dropwizard.jdbi.DBIFactory;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.exporter.MetricsServlet;
import org.eclipse.jetty.server.session.SessionHandler;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.flywaydb.core.Flyway;
import org.joda.time.DateTimeZone;
import org.secnod.dropwizard.shiro.ShiroBundle;
import org.secnod.dropwizard.shiro.ShiroConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;

public final class ReaperApplication extends Application<ReaperApplicationConfiguration> {

  private static final Logger LOG = LoggerFactory.getLogger(ReaperApplication.class);
  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
  private final AppContext context;

  public ReaperApplication() {
    super();
    LOG.info("default ReaperApplication constructor called");
    this.context = new AppContext();
  }

  @VisibleForTesting
  public ReaperApplication(AppContext context) {
    super();
    LOG.info("ReaperApplication constructor called with custom AppContext");
    this.context = context;
  }

  public static void main(String[] args) throws Exception {
    new ReaperApplication().run(args);
  }

  @Override
  public String getName() {
    return "cassandra-reaper";
  }

  /**
   * Before a Dropwizard application can provide the command-line interface, parse a configuration file, or run as a
   * server, it must first go through a bootstrapping phase. You can add Bundles, Commands, or register Jackson modules
   * to allow you to include custom types as part of your configuration class.
   */
  @Override
  public void initialize(Bootstrap<ReaperApplicationConfiguration> bootstrap) {
    bootstrap.addBundle(new AssetsBundle("/assets/", "/webui", "index.html"));
    bootstrap.getObjectMapper().registerModule(new JavaTimeModule());

    // enable using environment variables in yml files
    final SubstitutingSourceProvider envSourceProvider = new SubstitutingSourceProvider(
        bootstrap.getConfigurationSourceProvider(), new EnvironmentVariableSubstitutor(false));
    bootstrap.setConfigurationSourceProvider(envSourceProvider);

    bootstrap.addBundle(
        new ShiroBundle<ReaperApplicationConfiguration>() {
          @Override
          public void run(ReaperApplicationConfiguration configuration, Environment environment) {
            if (configuration.isAccessControlEnabled()) {
              super.run(configuration, environment);
            }
          }

          @Override
          protected ShiroConfiguration narrow(ReaperApplicationConfiguration configuration) {
            return configuration.getAccessControl().getShiroConfiguration();
          }
        });

    bootstrap.setConfigurationSourceProvider(
        new SubstitutingSourceProvider(
            bootstrap.getConfigurationSourceProvider(), new EnvironmentVariableSubstitutor()));
    bootstrap.getObjectMapper().registerModule(new JavaTimeModule());
  }

  @Override
  public void run(ReaperApplicationConfiguration config, Environment environment) throws Exception {
    // Using UTC times everywhere as default. Affects only Yoda time.
    DateTimeZone.setDefault(DateTimeZone.UTC);

    checkConfiguration(config);
    context.config = config;

    addSignalHandlers(); // SIGHUP, etc.

    context.metricRegistry = environment.metrics();
    CollectorRegistry.defaultRegistry.register(new DropwizardExports(environment.metrics()));

    environment
        .admin()
        .addServlet("prometheusMetrics", new MetricsServlet(CollectorRegistry.defaultRegistry))
        .addMapping("/prometheusMetrics");

    context.snapshotManager = SnapshotManager.create(context);

    LOG.info("initializing runner thread pool with {} threads", config.getRepairRunThreadCount());
    context.repairManager = RepairManager.create(context);
    context.repairManager.initializeThreadPool(
        config.getRepairRunThreadCount(),
        config.getHangingRepairTimeoutMins(),
        TimeUnit.MINUTES,
        config.getRepairManagerSchedulingIntervalSeconds(),
        TimeUnit.SECONDS);

    if (context.storage == null) {
      LOG.info("initializing storage of type: {}", config.getStorageType());
      context.storage = initializeStorage(config, environment);
    } else {
      LOG.info("storage already given in context, not initializing a new one");
    }

    if (context.jmxConnectionFactory == null) {
      LOG.info("no JMX connection factory given in context, creating default");
      context.jmxConnectionFactory = new JmxConnectionFactory(context.metricRegistry);

      // read jmx host/port mapping from config and provide to jmx con.factory
      Map<String, Integer> jmxPorts = config.getJmxPorts();
      if (jmxPorts != null) {
        LOG.debug("using JMX ports mapping: {}", jmxPorts);
        context.jmxConnectionFactory.setJmxPorts(jmxPorts);
      }

      if (config.useAddressTranslator()) {
        context.jmxConnectionFactory.setAddressTranslator(new EC2MultiRegionAddressTranslator());
      }
    }

    JmxCredentials jmxAuth = config.getJmxAuth();
    if (jmxAuth != null) {
      LOG.debug("using specified JMX credentials for authentication");
      context.jmxConnectionFactory.setJmxAuth(jmxAuth);
    }

    Map<String, JmxCredentials> jmxCredentials = config.getJmxCredentials();
    if (jmxCredentials != null) {
      LOG.debug("using specified JMX credentials per cluster for authentication");
      context.jmxConnectionFactory.setJmxCredentials(jmxCredentials);
    }

    // Enable cross-origin requests for using external GUI applications.
    if (config.isEnableCrossOrigin() || System.getProperty("enableCrossOrigin") != null) {
      final FilterRegistration.Dynamic cors
          = environment.servlets().addFilter("crossOriginRequests", CrossOriginFilter.class);
      cors.setInitParameter("allowedOrigins", "*");
      cors.setInitParameter("allowedHeaders", "X-Requested-With,Content-Type,Accept,Origin");
      cors.setInitParameter("allowedMethods", "OPTIONS,GET,PUT,POST,DELETE,HEAD");
      cors.addMappingForUrlPatterns(EnumSet.allOf(DispatcherType.class), true, "/*");
    }

    LOG.info("creating and registering health checks");
    // Notice that health checks are registered under the admin application on /healthcheck
    final ReaperHealthCheck healthCheck = new ReaperHealthCheck(context);
    environment.healthChecks().register("reaper", healthCheck);

    LOG.info("creating resources and registering endpoints");
    final PingResource pingResource = new PingResource(healthCheck);
    environment.jersey().register(pingResource);

    final ClusterResource addClusterResource = new ClusterResource(context);
    environment.jersey().register(addClusterResource);

    final RepairRunResource addRepairRunResource = new RepairRunResource(context);
    environment.jersey().register(addRepairRunResource);

    final RepairScheduleResource addRepairScheduleResource = new RepairScheduleResource(context);
    environment.jersey().register(addRepairScheduleResource);

    final SnapshotResource snapshotResource = new SnapshotResource(context);
    environment.jersey().register(snapshotResource);

    if (config.isAccessControlEnabled()) {
      SessionHandler sessionHandler = new SessionHandler();
      sessionHandler
          .getSessionManager()
          .setMaxInactiveInterval((int) config.getAccessControl().getSessionTimeout().getSeconds());
      environment.getApplicationContext().setSessionHandler(sessionHandler);
      environment.servlets().setSessionHandler(sessionHandler);
      environment.jersey().register(new ShiroExceptionMapper());
      environment.jersey().register(new LoginResource(context));
    }

    Thread.sleep(1000);

    SchedulingManager.start(context);

    if (config.hasAutoSchedulingEnabled()) {
      LOG.debug("using specified configuration for auto scheduling: {}", config.getAutoScheduling());
      AutoSchedulingManager.start(context);
    }

    initializeJmxSeedsForAllClusters();
    LOG.info("resuming pending repair runs");

    if (context.storage instanceof IDistributedStorage) {
      // Allowing multiple Reaper instances to work concurrently requires
      // us to poll the database for running repairs regularly
      // only with Cassandra storage
      scheduler.scheduleWithFixedDelay(
          () -> {
            try {
              context.repairManager.resumeRunningRepairRuns();
            } catch (ReaperException e) {
              LOG.error("Couldn't resume running repair runs", e);
            }
          },
          0,
          10,
          TimeUnit.SECONDS);
    } else {
      // Storage is different than Cassandra, assuming we have a single instance
      context.repairManager.resumeRunningRepairRuns();
    }
    LOG.info("Initialization complete! Reaper is ready to get things done!");
  }

  private IStorage initializeStorage(ReaperApplicationConfiguration config, Environment environment)
      throws ReaperException {
    IStorage storage;

    if ("memory".equalsIgnoreCase(config.getStorageType())) {
      storage = new MemoryStorage();
    } else if ("cassandra".equalsIgnoreCase(config.getStorageType())) {
      storage = new CassandraStorage(config, environment);
    } else if ("postgres".equalsIgnoreCase(config.getStorageType())
        || "h2".equalsIgnoreCase(config.getStorageType())
        || "database".equalsIgnoreCase(config.getStorageType())) {
      // create DBI instance
      final DBIFactory factory = new DBIFactory();

      // instanciate store
      storage = new PostgresStorage(factory.build(environment, config.getDataSourceFactory(), "postgresql"));
      initDatabase(config);
    } else {
      LOG.error("invalid storageType: {}", config.getStorageType());
      throw new ReaperException("invalid storage type: " + config.getStorageType());
    }
    Preconditions.checkState(storage.isStorageConnected(), "Failed to connect storage");
    return storage;
  }

  private void checkConfiguration(ReaperApplicationConfiguration config) {
    LOG.debug("repairIntensity: {}", config.getRepairIntensity());
    LOG.debug("incrementalRepair: {}", config.getIncrementalRepair());
    LOG.debug("repairRunThreadCount: {}", config.getRepairRunThreadCount());
    LOG.debug("segmentCount: {}", config.getSegmentCount());
    LOG.debug("repairParallelism: {}", config.getRepairParallelism());
    LOG.debug("hangingRepairTimeoutMins: {}", config.getHangingRepairTimeoutMins());
    LOG.debug("jmxPorts: {}", config.getJmxPorts());
  }

  void reloadConfiguration() {
    // TODO: reload configuration, but how?
    LOG.warn("SIGHUP signal dropped, missing implementation for configuration reload");
  }

  private void addSignalHandlers() {
    if (!System.getProperty("os.name").toLowerCase().contains("win")) {
      LOG.debug("adding signal handler for SIGHUP");
      Signal.handle(new Signal("HUP"), signal -> {
        LOG.info("received SIGHUP signal: {}", signal);
        reloadConfiguration();
      });
    }
  }

  private void initDatabase(ReaperApplicationConfiguration config) throws ReaperException {
    Flyway flyway = new Flyway();
    DataSourceFactory dsfactory = config.getDataSourceFactory();
    flyway.setDataSource(
        dsfactory.getUrl(),
        dsfactory.getUser(),
        dsfactory.getPassword());

    if ("database".equals(config.getStorageType())) {
      LOG.warn("!!!!!!!!!!    USAGE 'database' AS STORAGE TYPE IS NOW DEPRECATED   !!!!!!!!!!!!!!");
      LOG.warn("!!!!!!!!!!    PLEASE USE EITHER 'postgres' OR 'h2' FROM NOW ON     !!!!!!!!!!!!!!");
      if (config.getDataSourceFactory().getUrl().contains("h2")) {
        flyway.setLocations("/db/h2");
      } else {
        flyway.setLocations("/db/postgres");
      }
    } else {
      flyway.setLocations("/db/".concat(config.getStorageType().toLowerCase()));
    }
    flyway.setBaselineOnMigrate(true);
    flyway.repair();
    flyway.migrate();
  }

  private void initializeJmxSeedsForAllClusters() {
    LOG.info("Initializing JMX seed list for all clusters...");
    try (JmxConnectionsInitializer jmxConnectionsIntializer = JmxConnectionsInitializer.create(context);
        Timer.Context cxt = context
                .metricRegistry
                .timer(MetricRegistry.name(JmxConnectionFactory.class, "jmxConnectionsIntializer"))
                .time()) {

      context
          .storage
          .getClusters()
          .parallelStream()
          .forEach(cluster -> jmxConnectionsIntializer.on(cluster));

      LOG.info("Initialized JMX seed list for all clusters.");
    } catch (RuntimeException e) {
      LOG.error("Failed initializing JMX seed list", e);
    }
  }
}
