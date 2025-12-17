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

import io.cassandrareaper.AppContext;
import io.cassandrareaper.ReaperApplication;
import io.cassandrareaper.ReaperApplicationConfiguration;
import io.cassandrareaper.SimpleReaperClient;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.Sets;
import io.dropwizard.testing.ConfigOverride;
import io.dropwizard.testing.DropwizardTestSupport;
import io.dropwizard.testing.ResourceHelpers;
import jakarta.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple Reaper application runner for testing purposes. Starts a Jetty server that wraps Reaper
 * application, and registers a shutdown hook for JVM exit event.
 *
 * <p>Note, nothing in this class can import or reference any class that is in the shaded Reaper jar
 * file. All classes found in the Reaper jar file must be loaded via reflection and the
 * ParentLastURLClassLoader.
 */
public final class ReaperTestJettyRunner {

  private static final Logger LOG = LoggerFactory.getLogger(ReaperTestJettyRunner.class);
  static final Set<Integer> USED_PORTS = Sets.newConcurrentHashSet();

  final DropwizardTestSupport<ReaperApplicationConfiguration> runnerInstance;
  private SimpleReaperClient reaperClientInstance;
  private SimpleReaperClient reaperAdminClientInstance;

  public ReaperTestJettyRunner(String yamlConfigFile) {
    // Clean up any existing SQLite database before starting
    cleanupSqliteDatabase();

    runnerInstance =
        new DropwizardTestSupport<ReaperApplicationConfiguration>(
            ReaperApplication.class,
            ResourceHelpers.resourceFilePath(yamlConfigFile),
            ConfigOverride.config(
                "server.applicationConnectors[0].port", String.valueOf(getAnyAvailablePort())),
            ConfigOverride.config(
                "server.adminConnectors[0].port", String.valueOf(getAnyAvailablePort())));
    try {
      runnerInstance.before();
    } catch (Exception e) { // CHECKSTYLE IGNORE THIS LINE
      throw new RuntimeException(e);
    }

    Runtime.getRuntime().addShutdownHook(new Thread(runnerInstance::after));
  }

  private static void cleanupSqliteDatabase() {
    // Delete the SQLite database file used in integration tests
    File dbFile = new File("/tmp/reaper/storage/reaper.db");
    if (dbFile.exists()) {
      if (dbFile.delete()) {
        LOG.info("Deleted existing SQLite database file: {}", dbFile.getAbsolutePath());
      } else {
        LOG.warn("Failed to delete SQLite database file: {}", dbFile.getAbsolutePath());
      }
    }

    // Also delete journal and WAL files if they exist
    File journalFile = new File("/tmp/reaper/storage/reaper.db-journal");
    if (journalFile.exists()) {
      journalFile.delete();
    }
    File walFile = new File("/tmp/reaper/storage/reaper.db-wal");
    if (walFile.exists()) {
      walFile.delete();
    }
    File shmFile = new File("/tmp/reaper/storage/reaper.db-shm");
    if (shmFile.exists()) {
      shmFile.delete();
    }
  }

  public Response callReaper(
      String httpMethod, String urlPath, Optional<Map<String, String>> params) {
    return SimpleReaperClient.doHttpCall(
        httpMethod, "localhost", runnerInstance.getLocalPort(), urlPath, params);
  }

  public Response callReaperAdmin(
      String httpMethod, String urlPath, Optional<Map<String, String>> params) {
    return SimpleReaperClient.doHttpCall(
        httpMethod, "localhost", runnerInstance.getAdminPort(), urlPath, params);
  }

  public SimpleReaperClient getClient() {
    if (reaperClientInstance == null) {
      reaperClientInstance = new SimpleReaperClient("localhost", runnerInstance.getLocalPort());
    }
    return reaperClientInstance;
  }

  public SimpleReaperClient getAdminClient() {
    if (reaperAdminClientInstance == null) {
      reaperAdminClientInstance =
          new SimpleReaperClient("localhost", runnerInstance.getAdminPort());
    }
    return reaperAdminClientInstance;
  }

  public AppContext getContext() {
    ReaperApplication application = runnerInstance.getApplication();
    return application.getContext();
  }

  String getContextStorageClassname() {
    AppContext context = getContext();
    return context.storage.getClass().getName();
  }

  private static int getAnyAvailablePort() {
    try (ServerSocket s = new ServerSocket(0)) {
      return USED_PORTS.add(s.getLocalPort()) ? s.getLocalPort() : getAnyAvailablePort();
    } catch (IOException ex) {
      throw new IllegalStateException("no available ports", ex);
    }
  }
}
