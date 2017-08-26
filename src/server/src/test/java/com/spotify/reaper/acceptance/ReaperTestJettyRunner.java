package com.spotify.reaper.acceptance;

import com.google.common.base.Optional;
import com.google.common.io.Resources;
import com.spotify.reaper.AppContext;
import com.spotify.reaper.app.ReaperApplication;
import com.spotify.reaper.ReaperApplicationConfiguration;
import com.spotify.reaper.SimpleReaperClient;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Map;
import javax.ws.rs.core.Response;
import io.dropwizard.Application;
import io.dropwizard.testing.ConfigOverride;
import io.dropwizard.testing.DropwizardTestSupport;

/**
 * Simple Reaper application runner for testing purposes.
 * Starts a Jetty server that wraps Reaper application,
 * and registers a shutdown hook for JVM exit event.
 */
public final class ReaperTestJettyRunner {

  ReaperJettyTestSupport runnerInstance;
  private Server jettyServer;
  private SimpleReaperClient reaperClientInstance;

  public ReaperJettyTestSupport setup(AppContext testContext, String yamlConfigFile) throws Exception {
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

  public Response callReaper(String httpMethod, String urlPath, Optional<Map<String, String>> params) {
    assert runnerInstance != null : "service not initialized, call setup() first";
    return SimpleReaperClient.doHttpCall(httpMethod, "localhost", runnerInstance.getLocalPort(), urlPath, params);
  }

  public SimpleReaperClient getClient() {
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

  public final static class ReaperJettyTestSupport extends DropwizardTestSupport<ReaperApplicationConfiguration> {
      AppContext context;

      private ReaperJettyTestSupport(String configFile, AppContext context) {
          super(ReaperApplication.class,
                  new File(configFile).getAbsolutePath(),
                  ConfigOverride.config("server.adminConnectors[0].port", "" + getAnyAvailablePort()),
                  ConfigOverride.config("server.applicationConnectors[0].port", "" + getAnyAvailablePort())
          );
          this.context = context;
      }

      @Override
      public Application<ReaperApplicationConfiguration> newApplication() {
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

      private static int getAnyAvailablePort() {
          // this method doesn't actually reserve the ports
          // so subsequent calls may well return the same number
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        } catch (IOException ex) {
            throw new IllegalStateException("no available ports", ex);
        }
    }
  }

}
