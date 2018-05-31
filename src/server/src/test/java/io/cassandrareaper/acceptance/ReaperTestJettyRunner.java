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

package io.cassandrareaper.acceptance;

import io.cassandrareaper.AppContext;
import io.cassandrareaper.ReaperApplication;
import io.cassandrareaper.ReaperApplicationConfiguration;
import io.cassandrareaper.SimpleReaperClient;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Map;

import javax.ws.rs.core.Response;

import com.google.common.base.Optional;
import com.google.common.io.Resources;
import io.dropwizard.Application;
import io.dropwizard.testing.ConfigOverride;
import io.dropwizard.testing.DropwizardTestSupport;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;

/**
 * Simple Reaper application runner for testing purposes.
 * Starts a Jetty server that wraps Reaper application,
 * and registers a shutdown hook for JVM exit event.
 */
public final class ReaperTestJettyRunner {

  ReaperJettyTestSupport runnerInstance;
  private Server jettyServer;
  private SimpleReaperClient reaperClientInstance;

  public ReaperTestJettyRunner() {
  }

  public ReaperJettyTestSupport setup(AppContext testContext, String yamlConfigFile) {
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

  public int getLocalPort() {
    assert jettyServer != null : "service not initialized, call setup() first";
    return ((ServerConnector) jettyServer.getConnectors()[0]).getLocalPort();
  }

  public static final class ReaperJettyTestSupport extends DropwizardTestSupport<ReaperApplicationConfiguration> {

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
      context.repairManager.close();
      context.isRunning.set(false);
      try {
        Thread.sleep(100);
      } catch (InterruptedException expected) { }
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
