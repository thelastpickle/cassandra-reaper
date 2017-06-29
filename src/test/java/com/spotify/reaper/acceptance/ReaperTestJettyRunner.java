package com.spotify.reaper.acceptance;

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;

import com.spotify.reaper.AppContext;
import com.spotify.reaper.ReaperApplication;
import com.spotify.reaper.ReaperApplicationConfiguration;
import com.spotify.reaper.SimpleReaperClient;


import net.sourceforge.argparse4j.inf.Namespace;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.glassfish.jersey.client.ClientResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;

import javax.ws.rs.core.Response;

import io.dropwizard.Application;
import io.dropwizard.Configuration;
import io.dropwizard.cli.ServerCommand;
import io.dropwizard.lifecycle.ServerLifecycleListener;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.dropwizard.testing.ConfigOverride;
import io.dropwizard.testing.DropwizardTestSupport;

/**
 * Simple Reaper application runner for testing purposes.
 * Starts a Jetty server that wraps Reaper application,
 * and registers a shutdown hook for JVM exit event.
 */
public class ReaperTestJettyRunner {

  private static final Logger LOG = LoggerFactory.getLogger(ReaperTestJettyRunner.class);
  private Server jettyServer;
  private static ReaperJettyTestSupport runnerInstance;
  private static SimpleReaperClient reaperClientInstance;

  public static ReaperJettyTestSupport setup(AppContext testContext, String yamlConfigFile) throws Exception {
    if (runnerInstance == null) {
      runnerInstance = new ReaperJettyTestSupport(Resources.getResource(yamlConfigFile).getPath(), testContext);
      
      runnerInstance.before();
      
      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          if (runnerInstance != null) {
            runnerInstance.after();
          }
        }
      });
    }
    
    return runnerInstance;
  }

  public static Response callReaper(String httpMethod, String urlPath,
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



  public ReaperTestJettyRunner() {

  }



  public int getLocalPort() {
    assert jettyServer != null : "service not initialized, call setup() first";
    return ((ServerConnector) jettyServer.getConnectors()[0]).getLocalPort();
  }

  public final static class ReaperJettyTestSupport extends DropwizardTestSupport<ReaperApplicationConfiguration>
  {
      AppContext context;

      private ReaperJettyTestSupport(String configFile, AppContext context)
      {
          super(ReaperApplication.class,
                  new File(configFile).getAbsolutePath(),
                  ConfigOverride.config("server.adminConnectors[0].port", "8084"),
                  ConfigOverride.config("server.applicationConnectors[0].port", "8083")
          );
          
      this.context = context;    
      }

      @Override
      public Application<ReaperApplicationConfiguration> newApplication()
      {
          return new ReaperApplication(this.context);
      }

      @Override
      public void after() {
          context.isRunning.set(false);
          try {
              Thread.sleep(100);
          } catch (InterruptedException ex) {}
          super.after();
      }
  }
  
}
