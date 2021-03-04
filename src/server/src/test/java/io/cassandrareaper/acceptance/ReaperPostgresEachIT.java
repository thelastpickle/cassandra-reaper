/*
 * Copyright 2021-2021 DataStax, Inc.
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

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

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
      "classpath:io.cassandrareaper.acceptance/integration_reaper_functionality.feature",
      "classpath:io.cassandrareaper.acceptance/event_subscriptions.feature"
    },
    plugin = {"pretty"}
    )
public class ReaperPostgresEachIT {

  private static final Logger LOG = LoggerFactory.getLogger(ReaperPostgresSidecarIT.class);
  private static final List<ReaperTestJettyRunner> RUNNER_INSTANCES = new CopyOnWriteArrayList<>();
  private static final String[] POSTGRES_CONFIG_FILE = {
      "reaper-postgres-each1-at.yaml",
      "reaper-postgres-each2-at.yaml",
  };

  protected ReaperPostgresEachIT() {}

  @BeforeClass
  public static void setUp() throws Exception {
    LOG.info("setting up testing Reaper runner with {} seed hosts defined and Postgres storage",
        TestContext.TEST_CLUSTER_SEED_HOSTS.size());

    int reaperInstances = Integer.getInteger("grim.reaper.min", 2);
    for (int i = 0;i < reaperInstances;i++) {
      createReaperTestJettyRunner(Optional.empty());
    }
  }

  @AfterClass
  public static void tearDown() {
    LOG.info("Stopping reaper service...");
    RUNNER_INSTANCES.forEach(r -> r.runnerInstance.after());
  }

  private static void createReaperTestJettyRunner(Optional<String> version) throws InterruptedException {
    ReaperTestJettyRunner runner = new ReaperTestJettyRunner(POSTGRES_CONFIG_FILE[RUNNER_INSTANCES.size()], version);
    RUNNER_INSTANCES.add(runner);
    Thread.sleep(100);
    if (RUNNER_INSTANCES.size() == 1) {
      // REST calls must go to the first runner as the second one cannot access nodes in DC1,
      // which will be used to register the cluster.
      BasicSteps.addReaperRunner(runner);
    }
  }
}
