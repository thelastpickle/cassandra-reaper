package com.spotify.reaper.acceptance;

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;

import com.spotify.reaper.AppContext;
import com.spotify.reaper.ReaperApplication;
import com.spotify.reaper.ReaperApplicationConfiguration;
import com.spotify.reaper.SimpleReaperClient;
import com.sun.jersey.api.client.ClientResponse;

import net.sourceforge.argparse4j.inf.Namespace;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import io.dropwizard.cli.ServerCommand;
import io.dropwizard.lifecycle.ServerLifecycleListener;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

/**
 * Simple Reaper application runner for testing purposes.
 * Starts a Jetty server that wraps Reaper application,
 * and registers a shutdown hook for JVM exit event.
 */
public class ReaperTestJettyRunner {

  private static final Logger LOG = LoggerFactory.getLogger(ReaperTestJettyRunner.class);

  private static ReaperTestJettyRunner runnerInstance;
  private static SimpleReaperClient reaperClientInstance;

  public static ReaperTestJettyRunner setup(AppContext testContext, String yamlConfigFile) throws Exception {
    if (runnerInstance == null) {
      String testConfigPath = Resources.getResource(yamlConfigFile).getPath();
      LOG.info("initializing ReaperTestJettyRunner with config in path: " + testConfigPath);
      runnerInstance = new ReaperTestJettyRunner(testConfigPath, testContext);
      runnerInstance.start();
      // Stop the testing Reaper service instance after tests are finished.
      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          if (runnerInstance != null) {
            runnerInstance.stop();
          }
        }
      });
    }
    
    return runnerInstance;
  }

  public static ClientResponse callReaper(String httpMethod, String urlPath,
                                          Optional<Map<String, String>> params) {
    assert runnerInstance != null : "service not initialized, call setup() first";
    return SimpleReaperClient.doHttpCall(httpMethod, "localhost", runnerInstance.getLocalPort(),
                                         urlPath, params);
  }

  public static SimpleReaperClient getClient() {
    if (reaperClientInstance == null) {
      reaperClientInstance = new SimpleReaperClient("localhost", runnerInstance.getLocalPort());
    }
    return reaperClientInstance;
  }

  private final String configPath;

  private Server jettyServer;
  private AppContext context;

  public ReaperTestJettyRunner(String configPath, AppContext context) {
    this.configPath = configPath;
    this.context = context;
  }

  public void start() {
    if (jettyServer != null) {
      return;
    }
    try {
      ReaperApplication reaper;
      if (context != null) {
        reaper = new ReaperApplication(context);
      } else {
        reaper = new ReaperApplication();
      }

      final Bootstrap<ReaperApplicationConfiguration> bootstrap =
          new Bootstrap<ReaperApplicationConfiguration>(reaper) {
            @Override
            public void run(ReaperApplicationConfiguration configuration, Environment environment)
                throws Exception {
              environment.lifecycle().addServerLifecycleListener(new ServerLifecycleListener() {
                @Override
                public void serverStarted(Server server) {
                  jettyServer = server;
                }
              });
              super.run(configuration, environment);
            }
          };

      reaper.initialize(bootstrap);
      final ServerCommand<ReaperApplicationConfiguration> command = new ServerCommand<>(reaper);

      ImmutableMap.Builder<String, Object> file = ImmutableMap.builder();
      if (!Strings.isNullOrEmpty(configPath)) {
        file.put("file", configPath);
      }
      final Namespace namespace = new Namespace(file.build());

      command.run(bootstrap, namespace);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void stop() {
    if (null != jettyServer) {
      try {
        jettyServer.stop();
        jettyServer.join();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    jettyServer = null;
    runnerInstance = null;
  }

  public int getLocalPort() {
    assert jettyServer != null : "service not initialized, call setup() first";
    return ((ServerConnector) jettyServer.getConnectors()[0]).getLocalPort();
  }

}
