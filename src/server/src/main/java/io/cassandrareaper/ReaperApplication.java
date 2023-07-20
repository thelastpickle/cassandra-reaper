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
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.JmxCredentials;
import io.cassandrareaper.core.Node;
import io.cassandrareaper.crypto.Cryptograph;
import io.cassandrareaper.crypto.NoopCrypotograph;
import io.cassandrareaper.management.jmx.ClusterFacade;
import io.cassandrareaper.management.jmx.JmxManagementConnectionFactory;
import io.cassandrareaper.management.jmx.JmxConnectionsInitializer;
import io.cassandrareaper.metrics.PrometheusMetricsFilter;
import io.cassandrareaper.resources.ClusterResource;
import io.cassandrareaper.resources.CryptoResource;
import io.cassandrareaper.resources.DiagEventSseResource;
import io.cassandrareaper.resources.DiagEventSubscriptionResource;
import io.cassandrareaper.resources.NodeStatsResource;
import io.cassandrareaper.resources.PingResource;
import io.cassandrareaper.resources.ReaperHealthCheck;
import io.cassandrareaper.resources.ReaperResource;
import io.cassandrareaper.resources.RepairRunResource;
import io.cassandrareaper.resources.RepairScheduleResource;
import io.cassandrareaper.resources.RequestUtils;
import io.cassandrareaper.resources.SnapshotResource;
import io.cassandrareaper.resources.auth.LoginResource;
import io.cassandrareaper.resources.auth.ShiroExceptionMapper;
import io.cassandrareaper.resources.auth.ShiroJwtProvider;
import io.cassandrareaper.service.AutoSchedulingManager;
import io.cassandrareaper.service.Heart;
import io.cassandrareaper.service.PurgeService;
import io.cassandrareaper.service.RepairManager;
import io.cassandrareaper.service.SchedulingManager;
import io.cassandrareaper.storage.IDistributedStorage;
import io.cassandrareaper.storage.InitializeStorage;

import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.servlet.DispatcherType;
import javax.servlet.FilterRegistration;

import com.codahale.metrics.InstrumentedScheduledExecutorService;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.datastax.driver.core.policies.AddressTranslator;
import com.datastax.driver.core.policies.EC2MultiRegionAddressTranslator;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.client.HttpClientBuilder;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.exporter.MetricsServlet;
import org.apache.http.client.HttpClient;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.server.session.SessionHandler;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.joda.time.DateTimeZone;
import org.secnod.dropwizard.shiro.ShiroBundle;
import org.secnod.dropwizard.shiro.ShiroConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.cassandrareaper.metrics.PrometheusMetricsConfiguration.getCustomSampleMethodBuilder;

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
    // Enabling gzip buffering will prevent flushing of server-side-events, so we disable compression for SSE
    environment.lifecycle().addServerLifecycleListener(server -> {
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
   * Before a Dropwizard application can provide the command-line interface, parse a configuration file, or run as a
   * server, it must first go through a bootstrapping phase. You can add Bundles, Commands, or register Jackson modules
   * to allow you to include custom types as part of your configuration class.
   */
  @Override
  public void initialize(Bootstrap<ReaperApplicationConfiguration> bootstrap) {
    bootstrap.addCommand(new ReaperDbMigrationCommand("schema-migration", "Performs database schema migrations"));
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
  }

  @Override
  public void run(ReaperApplicationConfiguration config, Environment environment) throws Exception {
    // Using UTC times everywhere as default. Affects only Yoda time.
    DateTimeZone.setDefault(DateTimeZone.UTC);
    checkConfiguration(config);
    context.config = config;
    context.metricRegistry = environment.metrics();
    CollectorRegistry.defaultRegistry.register(new DropwizardExports(environment.metrics(),
        new PrometheusMetricsFilter(), getCustomSampleMethodBuilder()));

    environment
        .admin()
        .addServlet("prometheusMetrics", new MetricsServlet(CollectorRegistry.defaultRegistry))
        .addMapping("/prometheusMetrics");

    int repairThreads = config.getRepairRunThreadCount();
    int maxParallelRepairs = config.getMaxParallelRepairs();
    LOG.info("initializing runner thread pool with {} threads and {} repair runners",
        repairThreads, maxParallelRepairs);

    tryInitializeStorage(config, environment);

    Cryptograph cryptograph = context.config == null || context.config.getCryptograph() == null
        ? new NoopCrypotograph() : context.config.getCryptograph().create();

    initializeJmx(config, cryptograph);

    context.repairManager = RepairManager.create(
        context,
        environment.lifecycle().scheduledExecutorService("RepairRunner").threads(repairThreads).build(),
        config.getRepairManagerSchedulingIntervalSeconds(),
        TimeUnit.SECONDS,
        maxParallelRepairs,
        context.storage.getRepairRunDao());

    RequestUtils.setCorsEnabled(config.isEnableCrossOrigin());
    // Enable cross-origin requests for using external GUI applications.
    if (config.isEnableCrossOrigin() || System.getProperty("enableCrossOrigin") != null) {
      FilterRegistration.Dynamic co = environment.servlets().addFilter("crossOriginRequests", CrossOriginFilter.class);
      co.setInitParameter("allowedOrigins", "*");
      co.setInitParameter("allowedHeaders", "X-Requested-With,Content-Type,Accept,Origin,Authorization");
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

    final ClusterResource addClusterResource = ClusterResource.create(context, cryptograph,
        context.storage.getEventsDao(),
        context.storage.getRepairRunDao());
    environment.jersey().register(addClusterResource);
    final RepairRunResource addRepairRunResource = new RepairRunResource(context,
        context.storage.getRepairRunDao());
    environment.jersey().register(addRepairRunResource);
    final RepairScheduleResource addRepairScheduleResource = new RepairScheduleResource(context,
        context.storage.getRepairRunDao());
    environment.jersey().register(addRepairScheduleResource);
    final SnapshotResource snapshotResource = new SnapshotResource(context, environment,
        context.storage.getSnapshotDao());
    environment.jersey().register(snapshotResource);
    final ReaperResource reaperResource = new ReaperResource(context);
    environment.jersey().register(reaperResource);

    final NodeStatsResource nodeStatsResource = new NodeStatsResource(context);
    environment.jersey().register(nodeStatsResource);

    final CryptoResource addCryptoResource = new CryptoResource(cryptograph);
    environment.jersey().register(addCryptoResource);

    HttpClient httpClient = createHttpClient(config, environment);
    ScheduledExecutorService ses = environment.lifecycle().scheduledExecutorService("Diagnostics").threads(6).build();
    final DiagEventSubscriptionResource eventsResource = new DiagEventSubscriptionResource(context, httpClient, ses,
        context.storage.getEventsDao());
    environment.jersey().register(eventsResource);
    final DiagEventSseResource diagEvents = new DiagEventSseResource(context, httpClient, ses,
        context.storage.getEventsDao());
    environment.jersey().register(diagEvents);

    if (config.isAccessControlEnabled()) {
      SessionHandler sessionHandler = new SessionHandler();
      sessionHandler.setMaxInactiveInterval((int) config.getAccessControl().getSessionTimeout().getSeconds());
      RequestUtils.setSessionTimeout(config.getAccessControl().getSessionTimeout());
      environment.getApplicationContext().setSessionHandler(sessionHandler);
      environment.servlets().setSessionHandler(sessionHandler);
      environment.jersey().register(new ShiroExceptionMapper());
      environment.jersey().register(new LoginResource());
      environment.jersey().register(new ShiroJwtProvider(context));
    }

    Thread.sleep(1000);
    context.schedulingManager = SchedulingManager.create(context,
        context.storage.getRepairRunDao());
    context.schedulingManager.start();

    if (config.hasAutoSchedulingEnabled()) {
      LOG.debug("using specified configuration for auto scheduling: {}", config.getAutoScheduling());
      AutoSchedulingManager.start(context, context.storage.getRepairRunDao());
    }

    initializeJmxSeedsForAllClusters();
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

    ScheduledExecutorService scheduler = new InstrumentedScheduledExecutorService(
        environment.lifecycle().scheduledExecutorService("ReaperApplication-scheduler").threads(3).build(),
        context.metricRegistry);

    // SIDECAR mode must be distributed. ALL|EACH|LOCAL are lazy: we wait until we see multiple reaper instances
    context.isDistributed
        .compareAndSet(false, DatacenterAvailability.SIDECAR == context.config.getDatacenterAvailability());

    // Allowing multiple Reaper instances require concurrent database polls for repair and metrics statuses
    scheduleRepairManager(scheduler);
    scheduleHeartbeat(scheduler);


    context.repairManager.resumeRunningRepairRuns();
    schedulePurge(scheduler);

    LOG.info("Initialization complete!");
    LOG.warn("Reaper is ready to get things done!");
  }

  private void initializeJmx(ReaperApplicationConfiguration config, Cryptograph cryptograph) {
    if (context.jmxManagementConnectionFactory == null) {
      LOG.info("no JMX connection factory given in context, creating default");
      context.jmxManagementConnectionFactory = new JmxManagementConnectionFactory(context, cryptograph);

      // read jmx host/port mapping from config and provide to jmx con.factory
      Map<String, Integer> jmxPorts = config.getJmxPorts();
      if (jmxPorts != null) {
        LOG.debug("using JMX ports mapping: {}", jmxPorts);
        context.jmxManagementConnectionFactory.setJmxPorts(jmxPorts);
      }
      if (config.useAddressTranslator()) {
        context.jmxManagementConnectionFactory.setAddressTranslator(new EC2MultiRegionAddressTranslator());
      }
      if (config.getJmxAddressTranslator().isPresent()) {
        AddressTranslator addressTranslator = config.getJmxAddressTranslator().get().build();
        context.jmxManagementConnectionFactory.setAddressTranslator(addressTranslator);
      }
    }

    if (config.getJmxmp() != null) {
      if (config.getJmxmp().isEnabled()) {
        LOG.info("JMXMP enabled");
      }
      context.jmxManagementConnectionFactory.setJmxmp(config.getJmxmp());
    }

    JmxCredentials jmxAuth = config.getJmxAuth();
    if (jmxAuth != null) {
      LOG.debug("using specified JMX credentials for authentication");
      context.jmxManagementConnectionFactory.setJmxAuth(jmxAuth);
    }

    Map<String, JmxCredentials> jmxCredentials = config.getJmxCredentials();
    if (jmxCredentials != null) {
      LOG.debug("using specified JMX credentials per cluster for authentication");
      context.jmxManagementConnectionFactory.setJmxCredentials(jmxCredentials);
    }
  }

  /**
   * If Reaper is in sidecar mode, grab the local host id and the associated broadcast address
   *
   * @param addClusterResource a cluster resource instance
   * @throws ReaperException any caught runtime exception
   */
  private void maybeInitializeSidecarMode(ClusterResource addClusterResource) throws ReaperException {
    if (context.config.isInSidecarMode()) {
      ClusterFacade clusterFacade = ClusterFacade.create(context);
      Node host = Node.builder().withHostname(context.config.getEnforcedLocalNode().orElse("127.0.0.1")).build();
      try {
        context.localNodeAddress = context.config
            .getEnforcedLocalNode()
            .orElse(clusterFacade.getLocalEndpoint(host));

        LOG.info("Sidecar mode. Local node is : {}", context.localNodeAddress);
        selfRegisterClusterForSidecar(addClusterResource, context.config.getEnforcedLocalNode().orElse("127.0.0.1"));
      } catch (RuntimeException | InterruptedException | ReaperException e) {
        LOG.error("Failed connecting to the local node in sidecar mode {}", host, e);
        throw new ReaperException(e);
      }
    }
  }

  private boolean selfRegisterClusterForSidecar(ClusterResource addClusterResource, String seedHost)
      throws ReaperException {
    final Optional<Cluster> cluster = addClusterResource.findClusterWithSeedHost(seedHost, Optional.empty(),
        Optional.empty());
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

  private HttpClient createHttpClient(ReaperApplicationConfiguration config, Environment environment) {
    return new HttpClientBuilder(environment).using(config.getHttpClientConfiguration()).build(getName());
  }

  private void scheduleRepairManager(ScheduledExecutorService scheduler) {
    scheduler.scheduleWithFixedDelay(
        () -> {
          if (context.isDistributed.get()) {
            try {
              context.repairManager.resumeRunningRepairRuns();
            } catch (ReaperException | RuntimeException e) {
              // test-pollution: grim_reaper trashes this log error
              //if (!Boolean.getBoolean("grim.reaper.running")) {
              LOG.error("Couldn't resume running repair runs", e);
              //}
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
            //if (!Boolean.getBoolean("grim.reaper.running")) {
            LOG.error("Couldn't heartbeat", e);
            //}
          }
        },
        0,
        10,
        TimeUnit.SECONDS);
  }

  private void schedulePurge(ScheduledExecutorService scheduler) {
    final PurgeService purgeManager = PurgeService.create(context,
        context.storage.getRepairRunDao());

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
    LOG.debug("repairRunThreadCount: {}", config.getRepairRunThreadCount());
    LOG.debug("segmentCount: {}", config.getSegmentCount());
    LOG.debug("repairParallelism: {}", config.getRepairParallelism());
    LOG.debug("hangingRepairTimeoutMins: {}", config.getHangingRepairTimeoutMins());
    LOG.debug("jmxPorts: {}", config.getJmxPorts());
  }

  private void initializeJmxSeedsForAllClusters() {
    LOG.info("Initializing JMX seed list for all clusters...");
    try (JmxConnectionsInitializer jmxConnectionsIntializer = JmxConnectionsInitializer.create(context);
         Timer.Context cxt = context
             .metricRegistry
             .timer(MetricRegistry.name(JmxManagementConnectionFactory.class, "jmxConnectionsIntializer"))
             .time()) {

      context
          .storage
          .getClusterDao()
          .getClusters()
          .parallelStream()
          .sorted()
          .forEach(cluster -> jmxConnectionsIntializer.on(cluster));

      LOG.info("Initialized JMX seed list for all clusters.");
    }
  }

  private void tryInitializeStorage(ReaperApplicationConfiguration config, Environment environment)
      throws ReaperException, InterruptedException {
    if (context.storage == null) {
      LOG.info("initializing storage of type: {}", config.getStorageType());
      int storageFailures = 0;
      while (true) {
        try {
          context.storage = InitializeStorage.initializeStorage(
              config, environment, context.reaperInstanceId).initializeStorageBackend();

          // Allows to execute cleanup queries as shutdown hooks
          environment.lifecycle().manage(context.storage);
          break;
        } catch (RuntimeException e) {
          LOG.error("Storage is not ready yet, trying again to connect shortly...", e);
          storageFailures++;
          if (storageFailures > 60) {
            throw new ReaperException("Too many failures when trying to connect storage. Exiting :'(");
          }
          Thread.sleep(10000);
        }
      }
    } else {
      LOG.info("storage already given in context, not initializing a new one");
    }
  }
}