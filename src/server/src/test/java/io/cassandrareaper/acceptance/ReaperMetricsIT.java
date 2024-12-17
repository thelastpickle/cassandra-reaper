/*
 * Copyright 2014-2017 Spotify AB
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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.stream.Stream;

import cucumber.api.CucumberOptions;
import cucumber.api.junit.Cucumber;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@RunWith(Cucumber.class)
@CucumberOptions(
    features = {
      "classpath:io.cassandrareaper.acceptance/integration_reaper_metrics.feature"
    },
    plugin = {"pretty"}
    )
public class ReaperMetricsIT {

  private static final Logger LOG = LoggerFactory.getLogger(ReaperMetricsIT.class);
  private static ReaperTestJettyRunner runner;
  private static final String MEMORY_CONFIG_FILE = "cassandra-reaper-metrics-test.yaml";

  protected ReaperMetricsIT() {}

  @BeforeClass
  public static void setUp() throws Exception {
    LOG.info(
        "setting up testing Reaper runner with {} seed hosts defined and memory storage",
        TestContext.TEST_CLUSTER_SEED_HOSTS.size());

    // We now have persistence in the memory store, so we need to clean up the storage folder before starting the tests
    deleteFolderContents("/tmp/reaper/storage/");
    runner = new ReaperTestJettyRunner(MEMORY_CONFIG_FILE);
    BasicSteps.addReaperRunner(runner);
  }

  @AfterClass
  public static void tearDown() {
    LOG.info("Stopping reaper service...");
    runner.runnerInstance.after();
  }

  public static void deleteFolderContents(String folderPath) throws IOException {
    Path path = Paths.get(folderPath);
    try (Stream<Path> walk = Files.walk(path)) {
      walk.sorted(Comparator.reverseOrder())
          .forEach(p -> {
            try {
              Files.delete(p);
            } catch (IOException e) {
              throw new RuntimeException("Failed to delete " + p, e);
            }
          });
    }
  }

}
