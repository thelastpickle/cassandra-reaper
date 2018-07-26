/*
 * Copyright 2015-2017 Spotify AB
 * Copyright 2016-2018 The Last Pickle Ltd
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

package io.cassandrareaper.acceptance;

import io.cassandrareaper.AppContext;
import io.cassandrareaper.ReaperApplication;
import io.cassandrareaper.ReaperApplicationConfiguration;
import io.cassandrareaper.SimpleReaperClient;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;

import javax.ws.rs.core.Response;

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

  final ReaperJettyTestSupport runnerInstance;
  private Server jettyServer;
  private SimpleReaperClient reaperClientInstance;

  public ReaperTestJettyRunner(String yamlConfigFile, Optional<String> version) {
    runnerInstance = new ReaperJettyTestSupport(Resources.getResource(yamlConfigFile).getPath(), version);
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

    private ReaperJettyTestSupport(String configFile, Optional<String> version) {
      super(getApplicationClass(version),
          new File(configFile).getAbsolutePath(),
          ConfigOverride.config("server.adminConnectors[0].port", "" + getAnyAvailablePort()),
          ConfigOverride.config("server.applicationConnectors[0].port", "" + getAnyAvailablePort())
      );
      this.context = new AppContext();
    }

    private static Class getApplicationClass(Optional<String> version) {
      if (version.isPresent()) {
        try {
          ClassLoader loader = new ParentLastURLClassLoader(Paths.get(
                  "target",
                  "test-jars",
                  String.format("cassandra-reaper-%s.jar", version.get())).toUri().toURL());

          return loader.loadClass(ReaperApplication.class.getName());
        } catch (MalformedURLException | ClassNotFoundException ex) {
          throw new AssertionError(ex);
        }
      }
      return ReaperApplication.class;
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
        super.after();
      } catch (InterruptedException expected) { }
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

  private static final class ParentLastURLClassLoader extends ClassLoader {

    private final ChildURLClassLoader childClassLoader;

    private ParentLastURLClassLoader(final URL jarfile) {
      super(Thread.currentThread().getContextClassLoader());
      childClassLoader = new ChildURLClassLoader(new URL[]{jarfile}, new FindClassClassLoader(this.getParent()));
    }

    @Override
    protected synchronized Class<?> loadClass(final String name, final boolean resolve) throws ClassNotFoundException {
      try {
        return childClassLoader.findClass(name);
      } catch (ClassNotFoundException e) {
        return super.loadClass(name, resolve);
      }
    }

    /**
     * Make accessible `classloader.findClass(name)`
     */
    private static final class FindClassClassLoader extends ClassLoader {

      private FindClassClassLoader(final ClassLoader parent) {
        super(parent);
      }

      @Override
      public Class<?> findClass(final String name) throws ClassNotFoundException {
        return super.findClass(name);
      }
    }

    /**
     * This class delegates (child then parent) for the findClass method for a URLClassLoader.
     */
    private static final class ChildURLClassLoader extends URLClassLoader {

      private final FindClassClassLoader realParent;

      private ChildURLClassLoader(final URL[] urls, final FindClassClassLoader realParent) {
        super(urls, null);
        this.realParent = realParent;
      }

      @Override
      public Class<?> findClass(final String name) throws ClassNotFoundException {
        try {
          return super.findClass(name);
        } catch (ClassNotFoundException e) {
          return realParent.loadClass(name);
        }
      }
    }
  }
}
