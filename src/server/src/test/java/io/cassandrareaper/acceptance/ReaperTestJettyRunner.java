/*
 * Copyright 2015-2017 Spotify AB
 * Copyright 2016-2019 The Last Pickle Ltd
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

import io.cassandrareaper.ReaperApplication;
import io.cassandrareaper.ReaperApplicationConfiguration;
import io.cassandrareaper.SimpleReaperClient;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Optional;

import javax.ws.rs.core.Response;

import io.dropwizard.Application;
import io.dropwizard.testing.DropwizardTestSupport;
import io.dropwizard.testing.ResourceHelpers;

/**
 * Simple Reaper application runner for testing purposes.
 * Starts a Jetty server that wraps Reaper application,
 * and registers a shutdown hook for JVM exit event.
 * <p/>
 * Note, nothing in this class can import or reference any class that is in the shaded Reaper jar file.
 * All classes found in the Reaper jar file must be loaded via reflection and the ParentLastURLClassLoader.
 */
public final class ReaperTestJettyRunner {

  static final String REAPER_APPLICATION_FQN = "io.cassandrareaper.ReaperApplication";

  final DropwizardTestSupport<ReaperApplicationConfiguration> runnerInstance;
  private SimpleReaperClient reaperClientInstance;

  public ReaperTestJettyRunner(String yamlConfigFile, Optional<String> version) {
    runnerInstance = new DropwizardTestSupport<ReaperApplicationConfiguration>(
      ReaperApplication.class, ResourceHelpers.resourceFilePath(yamlConfigFile));
    try {
      runnerInstance.before();
    } catch (Exception e) { // CHECKSTYLE IGNORE THIS LINE
      throw new RuntimeException(e);
    }

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

  public Object getContext() {
    Application<ReaperApplicationConfiguration> application = runnerInstance.getApplication();
    try {
      Field field = application.getClass().getDeclaredField("context");
      field.setAccessible(true);
      return field.get(application);
    } catch (ReflectiveOperationException ex) {
      throw new AssertionError("Failed creating the proxy to DropwizardTestSupport", ex);
    }
  }

  String getContextStorageClassname() {
    try {
      // reflection for `context.storage.getClass().getName()`
      Object context = getContext();
      return context.getClass().getField("storage").get(context).getClass().getName();
    } catch (ReflectiveOperationException ex) {
      throw new AssertionError(ex);
    }
  }
}
