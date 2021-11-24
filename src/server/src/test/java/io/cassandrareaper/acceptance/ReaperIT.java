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

import java.util.Optional;

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
public class ReaperIT {

  private static final Logger LOG = LoggerFactory.getLogger(ReaperIT.class);
  private static ReaperTestJettyRunner runner;
  private static final String MEMORY_CONFIG_FILE = "cassandra-reaper-at.yaml";

  protected ReaperIT() {}

  @BeforeClass
  public static void setUp() throws Exception {
    LOG.info(
        "setting up testing Reaper runner with {} seed hosts defined and memory storage",
        TestContext.TEST_CLUSTER_SEED_HOSTS.size());

    runner = new ReaperTestJettyRunner(MEMORY_CONFIG_FILE, Optional.empty());
    BasicSteps.addReaperRunner(runner);
  }

  @AfterClass
  public static void tearDown() {
    LOG.info("Stopping reaper service...");
    runner.runnerInstance.after();
  }

}
