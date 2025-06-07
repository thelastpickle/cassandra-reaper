/*
 * Copyright 2014-2017 Spotify AB
 * Copyright 2016-2019 The Last Pickle Ltd
 * Copyright 2020-2020 DataStax, Inc.
 *
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

import io.cassandrareaper.ReaperApplicationConfiguration.DatacenterAvailability;
import io.cassandrareaper.auth.AuthLoginResource;
import io.cassandrareaper.auth.BasicAuthenticator;
import io.cassandrareaper.auth.JwtAuthenticator;
import io.cassandrareaper.auth.RoleAuthorizer;
import io.cassandrareaper.auth.User;
import io.cassandrareaper.auth.UserStore;
import io.cassandrareaper.auth.WebuiAuthenticationFilter;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.Node;
import io.cassandrareaper.crypto.Cryptograph;
import io.cassandrareaper.crypto.NoopCrypotograph;
import io.cassandrareaper.management.ClusterFacade;
import io.cassandrareaper.management.http.HttpManagementConnectionFactory;
import io.cassandrareaper.management.jmx.JmxManagementConnectionFactory;
import io.cassandrareaper.metrics.PrometheusMetricsFilter;
import io.cassandrareaper.resources.ClusterResource;
import io.cassandrareaper.resources.CryptoResource;
import io.cassandrareaper.resources.DiagEventSseResource;
import io.cassandrareaper.resources.DiagEventSubscriptionResource;
import io.cassandrareaper.resources.NodeStatsResource;
import io.cassandrareaper.resources.PingResource;
import io.cassandrareaper.resources.PrometheusMetricsResource;
import io.cassandrareaper.resources.ReaperHealthCheck;
import io.cassandrareaper.resources.ReaperResource;
import io.cassandrareaper.resources.RepairRunResource;
import io.cassandrareaper.resources.RepairScheduleResource;
import io.cassandrareaper.resources.RequestUtils;
import io.cassandrareaper.resources.SnapshotResource;
import io.cassandrareaper.service.AutoSchedulingManager;
import io.cassandrareaper.service.Heart;
import io.cassandrareaper.service.PurgeService;
import io.cassandrareaper.service.RepairManager;
import io.cassandrareaper.service.SchedulingManager;
import io.cassandrareaper.storage.IDistributedStorage;
import io.cassandrareaper.storage.InitializeStorage;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static io.cassandrareaper.metrics.PrometheusMetricsConfiguration.getCustomSampleMethodBuilder;

import com.codahale.metrics.InstrumentedScheduledExecutorService;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.auth.AuthDynamicFeature;
import io.dropwizard.auth.AuthValueFactoryProvider;
import io.dropwizard.auth.basic.BasicCredentialAuthFilter;
import io.dropwizard.auth.oauth.OAuthCredentialAuthFilter;
import io.dropwizard.client.HttpClientBuilder;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.core.Application;
import io.dropwizard.core.setup.Bootstrap;
import io.dropwizard.core.setup.Environment;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import jakarta.servlet.DispatcherType;
import jakarta.servlet.FilterRegistration;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.server.session.SessionHandler;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ReaperApplication extends Application<ReaperApplicationConfiguration> {

  private static final Logger LOG = LoggerFactory.getLogger(ReaperApplication.class);
  private final AppContext context;

  public ReaperApplication() {
    super();
    LOG.info("default ReaperApplication constructor called");
    this.context = new AppContext();
  }

  public static void main(String[] args) throws Exception {
    new ReaperApplication().run(args);
  }

  private static void setupSse(Environment environment) {
    // Enabling gzip buffering will prevent flushing of server-side-events, so we disable
    // compression for SSE
    environment
        .lifecycle()
        .addServerLifecycleListener(
            server -> {
              for (Handler handler : server.getChildHandlersByClass(GzipHandler.class)) {
                ((GzipHandler) handler).addExcludedMimeTypes("text/event-stream");
              }
            });
  }

  @Override
  public String getName() {
    return "cassandra-reaper";
  }

  @VisibleForTesting
  public AppContext getContext() {
    return context;
  }

  /**
   * Before a Dropwizard application can provide the command-line interface, parse a configuration
   * file, or run as a server, it must first go through a bootstrapping phase. You can add Bundles,
   * Commands, or register Jackson modules to allow you to include custom types as part of your
   * configuration class.
   */
  @Override
  public void initialize(Bootstrap<ReaperApplicationConfiguration> bootstrap) {
    bootstrap.addCommand(
        new ReaperDbMigrationCommand("schema-migration", "Performs database schema migrations"));
    bootstrap.addBundle(new AssetsBundle("/assets/", "/webui", "index.html"));
    bootstrap
        .getObjectMapper()
        .registerModule(new JavaTimeModule())
        .registerModule(new JodaModule());

    // enable using environment variables in yml files
    final SubstitutingSourceProvider envSourceProvider =
        new SubstitutingSourceProvider(
            bootstrap.getConfigurationSourceProvider(), new EnvironmentVariableSubstitutor(false));
    bootstrap.setConfigurationSourceProvider(envSourceProvider);

    // Dropwizard auth configuration will be done in run() method

    bootstrap.setConfigurationSourceProvider(
        new SubstitutingSourceProvider(
            bootstrap.getConfigurationSourceProvider(), new EnvironmentVariableSubstitutor()));
  }

  @Override
  public void run(ReaperApplicationConfiguration config, Environment environment) throws Exception {
    // Using UTC times everywhere as default. Affects only Yoda time.
    DateTimeZone.setDefault(DateTimeZone.UTC);

    LOG.info(
        "CONFIGURATION DEBUG: AccessControl config is: {}",
        config.getAccessControl() != null ? "PRESENT" : "NULL");
    if (config.getAccessControl() != null) {
      LOG.info(
          "CONFIGURATION DEBUG: JWT config is: {}",
          config.getAccessControl().getJwt() != null ? "PRESENT" : "NULL");
      LOG.info(
          "CONFIGURATION DEBUG: Users config has {} users",
          config.getAccessControl().getUsers() != null
              ? config.getAccessControl().getUsers().size()
              : 0);
    }

    checkConfiguration(config);
    context.config = config;
    context.metricRegistry = environment.metrics();
    CollectorRegistry.defaultRegistry.register(
        new DropwizardExports(
            environment.metrics(), new PrometheusMetricsFilter(), getCustomSampleMethodBuilder()));

    environment.jersey().register(new PrometheusMetricsResource());

    int repairThreads = config.getRepairRunThreadCount();
    int maxParallelRepairs = config.getMaxParallelRepairs();
    LOG.info(
        "initializing runner thread pool with {} threads and {} repair runners",
        repairThreads,
        maxParallelRepairs);

    tryInitializeStorage(config, environment);

    Cryptograph cryptograph =
        context.config == null || context.config.getCryptograph() == null
            ? new NoopCrypotograph()
            : context.config.getCryptograph().create();

    initializeManagement(context, environment, cryptograph);

    context.repairManager =
        RepairManager.create(
            context,
            environment
                .lifecycle()
                .scheduledExecutorService("RepairRunner")
                .threads(repairThreads)
                .build(),
            config.getRepairManagerSchedulingIntervalSeconds(),
            TimeUnit.SECONDS,
            maxParallelRepairs,
            context.storage.getRepairRunDao());

    RequestUtils.setCorsEnabled(config.isEnableCrossOrigin());
    // Enable cross-origin requests for using external GUI applications.
    if (config.isEnableCrossOrigin() || System.getProperty("enableCrossOrigin") != null) {
      FilterRegistration.Dynamic co =
          environment.servlets().addFilter("crossOriginRequests", CrossOriginFilter.class);
      co.setInitParameter("allowedOrigins", "*");
      co.setInitParameter(
          "allowedHeaders", "X-Requested-With,Content-Type,Accept,Origin,Authorization");
      co.setInitParameter("allowedMethods", "OPTIONS,GET,PUT,POST,DELETE,HEAD,PATCH");
      co.addMappingForUrlPatterns(EnumSet.allOf(DispatcherType.class), true, "/*");
    }

    setupSse(environment);
    LOG.info("creating and registering health checks");
    // Notice that health checks are registered under the admin application on /healthcheck
    final ReaperHealthCheck healthCheck = new ReaperHealthCheck(context);
    environment.healthChecks().register("reaper", healthCheck);

    LOG.info("creating resources and registering endpoints");
    final PingResource pingResource = new PingResource(healthCheck);
    environment.jersey().register(pingResource);

    final ClusterResource addClusterResource =
        ClusterResource.create(
            context,
            cryptograph,
            context.storage.getEventsDao(),
            context.storage.getRepairRunDao());
    environment.jersey().register(addClusterResource);
    final RepairRunResource addRepairRunResource =
        new RepairRunResource(context, context.storage.getRepairRunDao());
    environment.jersey().register(addRepairRunResource);
    final RepairScheduleResource addRepairScheduleResource =
        new RepairScheduleResource(context, context.storage.getRepairRunDao());
    environment.jersey().register(addRepairScheduleResource);
    final SnapshotResource snapshotResource =
        new SnapshotResource(context, environment, context.storage.getSnapshotDao());
    environment.jersey().register(snapshotResource);
    final ReaperResource reaperResource = new ReaperResource(context);
    environment.jersey().register(reaperResource);

    final NodeStatsResource nodeStatsResource = new NodeStatsResource(context);
    environment.jersey().register(nodeStatsResource);

    final CryptoResource addCryptoResource = new CryptoResource(cryptograph);
    environment.jersey().register(addCryptoResource);

    CloseableHttpClient httpClient = createHttpClient(config, environment);

    if (config.getHttpManagement() == null || !config.getHttpManagement().isEnabled()) {
      ScheduledExecutorService ses =
          environment.lifecycle().scheduledExecutorService("Diagnostics").threads(6).build();
      final DiagEventSubscriptionResource eventsResource =
          new DiagEventSubscriptionResource(
              context, httpClient, ses, context.storage.getEventsDao());
      environment.jersey().register(eventsResource);
      final DiagEventSseResource diagEvents =
          new DiagEventSseResource(context, httpClient, ses, context.storage.getEventsDao());
      environment.jersey().register(diagEvents);
    }

    if (config.getAccessControl() != null) {
      LOG.info("ACCESS CONTROL: Setting up authentication - accessControl config found");
      SessionHandler sessionHandler = new SessionHandler();
      environment.getApplicationContext().setSessionHandler(sessionHandler);
      environment.servlets().setSessionHandler(sessionHandler);

      // Setup Dropwizard authentication using configuration
      UserStore userStore = new UserStore();

      // Add users from configuration
      if (config.getAccessControl().getUsers() != null) {
        LOG.info(
            "ACCESS CONTROL: Adding {} users from configuration",
            config.getAccessControl().getUsers().size());
        for (ReaperApplicationConfiguration.UserConfiguration userConfig :
            config.getAccessControl().getUsers()) {
          userStore.addUser(
              userConfig.getUsername(),
              userConfig.getPassword(),
              new HashSet<>(userConfig.getRoles()));
          LOG.info(
              "ACCESS CONTROL: Added user {} with roles {}",
              userConfig.getUsername(),
              userConfig.getRoles());
        }
      } else {
        LOG.info("ACCESS CONTROL: No users found in configuration");
      }

      String jwtSecret =
          config.getAccessControl().getJwt() != null
              ? config.getAccessControl().getJwt().getSecret()
              : "MySecretKeyForJWTWhichMustBeLongEnoughForHS256Algorithm";

      LOG.info(
          "ACCESS CONTROL: Using JWT secret: {}",
          jwtSecret != null ? "[REDACTED - length: " + jwtSecret.length() + "]" : "null");

      // JWT authentication for REST API
      JwtAuthenticator jwtAuthenticator = new JwtAuthenticator(jwtSecret, userStore);
      BasicAuthenticator basicAuthenticator = new BasicAuthenticator(userStore);
      RoleAuthorizer authorizer = new RoleAuthorizer();

      // Register JWT/OAuth filter for REST endpoints
      environment
          .jersey()
          .register(
              new AuthDynamicFeature(
                  new OAuthCredentialAuthFilter.Builder<User>()
                      .setAuthenticator(jwtAuthenticator)
                      .setAuthorizer(authorizer)
                      .setPrefix("Bearer")
                      .buildAuthFilter()));

      // Register Basic Auth filter as backup
      environment
          .jersey()
          .register(
              new AuthDynamicFeature(
                  new BasicCredentialAuthFilter.Builder<User>()
                      .setAuthenticator(basicAuthenticator)
                      .setAuthorizer(authorizer)
                      .buildAuthFilter()));

      // Register @Auth parameter injection
      environment.jersey().register(new AuthValueFactoryProvider.Binder<>(User.class));

      // Register login resource
      environment.jersey().register(new AuthLoginResource(userStore, jwtSecret));

      // Add WebUI authentication filter to protect /webui/* paths
      FilterRegistration.Dynamic webuiFilter =
          environment
              .servlets()
              .addFilter("webuiAuth", new WebuiAuthenticationFilter(jwtSecret, userStore));
      webuiFilter.addMappingForUrlPatterns(EnumSet.allOf(DispatcherType.class), true, "/webui/*");
      environment.jersey().register(RolesAllowedDynamicFeature.class);
    } else {
      LOG.warn("ACCESS CONTROL: No accessControl configuration found - authentication disabled!");
    }

    Thread.sleep(1000);
    context.schedulingManager =
        SchedulingManager.create(context, context.storage.getRepairRunDao());
    context.schedulingManager.start();

    if (config.hasAutoSchedulingEnabled()) {
      LOG.debug(
          "using specified configuration for auto scheduling: {}", config.getAutoScheduling());
      AutoSchedulingManager.start(context, context.storage.getRepairRunDao());
    }

    maybeInitializeSidecarMode(addClusterResource);
    LOG.info("resuming pending repair runs");

    Preconditions.checkState(
        context.storage instanceof IDistributedStorage
            || DatacenterAvailability.SIDECAR != context.config.getDatacenterAvailability(),
        "Cassandra backend storage is the only one allowing SIDECAR datacenter availability modes.");

    Preconditions.checkState(
        context.storage instanceof IDistributedStorage
            || DatacenterAvailability.EACH != context.config.getDatacenterAvailability(),
        "Cassandra backend storage is the only one allowing EACH datacenter availability modes.");

    ScheduledExecutorService scheduler =
        new InstrumentedScheduledExecutorService(
            environment
                .lifecycle()
                .scheduledExecutorService("ReaperApplication-scheduler")
                .threads(3)
                .build(),
            context.metricRegistry);

    // SIDECAR mode must be distributed. ALL|EACH|LOCAL are lazy: we wait until we see multiple
    // reaper instances
    context.isDistributed.compareAndSet(
        false, DatacenterAvailability.SIDECAR == context.config.getDatacenterAvailability());

    // Allowing multiple Reaper instances require concurrent database polls for repair and metrics
    // statuses
    scheduleRepairManager(scheduler);
    scheduleHeartbeat(scheduler);

    context.repairManager.resumeRunningRepairRuns();
    schedulePurge(scheduler);

    LOG.info("Initialization complete!");
    LOG.warn("Reaper is ready to get things done!");
  }

  private void initializeManagement(
      AppContext context, Environment environment, Cryptograph cryptograph) {
    if (context.managementConnectionFactory == null) {
      LOG.info("no management connection factory given in context, creating default");
      if (context.config.getHttpManagement() == null
          || !context.config.getHttpManagement().isEnabled()) {
        LOG.info(
            "HTTP management connection config not set, or set disabled. Creating JMX connection factory instead");
        context.managementConnectionFactory =
            new JmxManagementConnectionFactory(context, cryptograph);
      } else {
        ScheduledExecutorService jobStatusPollerExecutor =
            environment.lifecycle().scheduledExecutorService("JobStatusPoller").threads(2).build();
        context.managementConnectionFactory =
            new HttpManagementConnectionFactory(context, jobStatusPollerExecutor);
      }
    }
  }

  /**
   * If Reaper is in sidecar mode, grab the local host id and the associated broadcast address
   *
   * @param addClusterResource a cluster resource instance
   * @throws ReaperException any caught runtime exception
   */
  private void maybeInitializeSidecarMode(ClusterResource addClusterResource)
      throws ReaperException {
    if (context.config.isInSidecarMode()) {
      ClusterFacade clusterFacade = ClusterFacade.create(context);
      Node host =
          Node.builder()
              .withHostname(context.config.getEnforcedLocalNode().orElse("127.0.0.1"))
              .build();
      try {
        context.localNodeAddress =
            context.config.getEnforcedLocalNode().orElse(clusterFacade.getLocalEndpoint(host));

        LOG.info("Sidecar mode. Local node is : {}", context.localNodeAddress);
        selfRegisterClusterForSidecar(
            addClusterResource, context.config.getEnforcedLocalNode().orElse("127.0.0.1"));
      } catch (RuntimeException | InterruptedException | ReaperException e) {
        LOG.error("Failed connecting to the local node in sidecar mode {}", host, e);
        throw new ReaperException(e);
      }
    }
  }

  private boolean selfRegisterClusterForSidecar(ClusterResource addClusterResource, String seedHost)
      throws ReaperException {
    final Optional<Cluster> cluster =
        addClusterResource.findClusterWithSeedHost(seedHost, Optional.empty(), Optional.empty());
    if (!cluster.isPresent()) {
      return false;
    }

    if (context.storage.getClusterDao().getClusters().stream()
        .noneMatch(c -> c.getName().equals(cluster.get().getName()))) {
      LOG.info("registering new cluster : {}", cluster.get().getName());

      // it is ok for this be called in parallel by different sidecars on the same cluster,
      // so long we're not split-brain
      context.storage.getClusterDao().addCluster(cluster.get());
    }
    return true;
  }

  private CloseableHttpClient createHttpClient(
      ReaperApplicationConfiguration config, Environment environment) {
    return new HttpClientBuilder(environment)
        .using(config.getHttpClientConfiguration())
        .build(getName());
  }

  private void scheduleRepairManager(ScheduledExecutorService scheduler) {
    scheduler.scheduleWithFixedDelay(
        () -> {
          if (context.isDistributed.get()) {
            try {
              context.repairManager.resumeRunningRepairRuns();
            } catch (ReaperException | RuntimeException e) {
              // test-pollution: grim_reaper trashes this log error
              // if (!Boolean.getBoolean("grim.reaper.running")) {
              LOG.error("Couldn't resume running repair runs", e);
              // }
            }
          }
        },
        10,
        10,
        TimeUnit.SECONDS);
  }

  private void scheduleHeartbeat(ScheduledExecutorService scheduler) throws ReaperException {
    final Heart heart = Heart.create(context);

    scheduler.scheduleAtFixedRate(
        () -> {
          try {
            heart.beat();
          } catch (RuntimeException e) {
            // test-pollution: grim_reaper trashes this log error
            // if (!Boolean.getBoolean("grim.reaper.running")) {
            LOG.error("Couldn't heartbeat", e);
            // }
          }
        },
        0,
        10,
        TimeUnit.SECONDS);
  }

  private void schedulePurge(ScheduledExecutorService scheduler) {
    final PurgeService purgeManager =
        PurgeService.create(context, context.storage.getRepairRunDao());

    scheduler.scheduleWithFixedDelay(
        () -> {
          try {
            int purgedRuns = purgeManager.purgeDatabase();
            LOG.info("Purged {} repair runs from history", purgedRuns);
          } catch (RuntimeException | ReaperException e) {
            LOG.error("Failed purging repair runs from history", e);
          }
        },
        0,
        1,
        TimeUnit.HOURS);
  }

  private void checkConfiguration(ReaperApplicationConfiguration config) {
    LOG.debug("repairIntensity: {}", config.getRepairIntensity());
    LOG.debug("incrementalRepair: {}", config.getIncrementalRepair());
    LOG.debug("subrangeIncrementalRepair: {}", config.getSubrangeIncrementalRepair());
    LOG.debug("repairRunThreadCount: {}", config.getRepairRunThreadCount());
    LOG.debug("segmentCount: {}", config.getSegmentCount());
    LOG.debug("repairParallelism: {}", config.getRepairParallelism());
    LOG.debug("hangingRepairTimeoutMins: {}", config.getHangingRepairTimeoutMins());
    LOG.debug("jmxPorts: {}", config.getJmxPorts());

    if (config.getHttpManagement() != null) {
      if (config.getHttpManagement().isEnabled()) {
        if (config.getHttpManagement().getTruststoresDir() != null) {
          if (!Files.exists(Paths.get(config.getHttpManagement().getTruststoresDir()))) {
            throw new RuntimeException(
                String.format(
                    "HttpManagement truststores directory is configured as %s but it does not exist",
                    config.getHttpManagement().getTruststoresDir()));
          }
        }
      }
    }
  }

  private void tryInitializeStorage(ReaperApplicationConfiguration config, Environment environment)
      throws ReaperException, InterruptedException {
    if (context.storage == null) {
      LOG.info("initializing storage of type: {}", config.getStorageType());
      int storageFailures = 0;
      while (true) {
        try {
          context.storage =
              InitializeStorage.initializeStorage(config, environment, context.reaperInstanceId)
                  .initializeStorageBackend();

          // Allows to execute cleanup queries as shutdown hooks
          environment.lifecycle().manage(context.storage);
          break;
        } catch (RuntimeException e) {
          LOG.error("Storage is not ready yet, trying again to connect shortly...", e);
          storageFailures++;
          if (storageFailures > 60) {
            throw new ReaperException(
                "Too many failures when trying to connect storage. Exiting :'(");
          }
          Thread.sleep(10000);
        }
      }
    } else {
      LOG.info("storage already given in context, not initializing a new one");
    }
  }
}
