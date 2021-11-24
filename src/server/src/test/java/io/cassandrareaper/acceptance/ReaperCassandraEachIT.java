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

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.google.common.base.Preconditions;
import cucumber.api.CucumberOptions;
import cucumber.api.junit.Cucumber;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

@RunWith(Cucumber.class)
@CucumberOptions(
    features = "classpath:io.cassandrareaper.acceptance/integration_reaper_functionality.feature",
    plugin = {"pretty"}
    )
public class ReaperCassandraEachIT implements Upgradable {

  private static final Logger LOG = LoggerFactory.getLogger(ReaperCassandraSidecarIT.class);
  private static final List<ReaperTestJettyRunner> RUNNER_INSTANCES = new CopyOnWriteArrayList<>();
  private static final String[] CASS_CONFIG_FILE
    = {
      "reaper-cassandra-each1-at.yaml",
      "reaper-cassandra-each2-at.yaml"
    };

  protected ReaperCassandraEachIT() {}

  @BeforeClass
  public static void setUp() throws Exception {
    LOG.info(
        "setting up testing Reaper runner with {} seed hosts defined and cassandra storage",
        TestContext.TEST_CLUSTER_SEED_HOSTS.size());

    BasicSteps.setup(new ReaperCassandraSidecarIT());
    int reaperInstances = Integer.getInteger("grim.reaper.min", 2);

    initSchema();
    for (int i = 0;i < reaperInstances;i++) {
      createReaperTestJettyRunner(Optional.empty());
    }
  }

  @Override
  public void upgradeReaperRunner(Optional<String> version) throws InterruptedException {
    synchronized (ReaperCassandraSidecarIT.class) {
      Preconditions.checkState(1 >= RUNNER_INSTANCES.size(), "Upgrading with multiple Reaper instances not supported");
    }
  }

  private static void createReaperTestJettyRunner(Optional<String> version) throws InterruptedException {
    ReaperTestJettyRunner runner
        = new ReaperTestJettyRunner(CASS_CONFIG_FILE[RUNNER_INSTANCES.size()], Optional.empty());
    RUNNER_INSTANCES.add(runner);
    Thread.sleep(100);
    if (RUNNER_INSTANCES.size() == 1) {
      // REST calls must go to the first runner as the second one cannot access nodes in DC1,
      // which will be used to register the cluster.
      BasicSteps.addReaperRunner(runner);
    }
  }

  public static void initSchema() throws IOException {
    try (Cluster cluster = buildCluster(); Session tmpSession = cluster.connect()) {
      await().with().pollInterval(3, SECONDS).atMost(2, MINUTES).until(() -> {
        try {
          tmpSession.execute("DROP KEYSPACE IF EXISTS reaper_db");
          return true;
        } catch (RuntimeException ex) {
          return false;
        }
      });
      tmpSession.execute(
          "CREATE KEYSPACE reaper_db WITH replication = {" + BasicSteps.buildNetworkTopologyStrategyString(cluster)
          + "}");
    }
  }

  @AfterClass
  public static void tearDown() {
    LOG.info("Stopping reaper service...");
    RUNNER_INSTANCES.forEach(r -> r.runnerInstance.after());
  }

  private static Cluster buildCluster() {
    return Cluster.builder()
        .addContactPoint("127.0.0.1")
        .withSocketOptions(new SocketOptions().setConnectTimeoutMillis(20000).setReadTimeoutMillis(40000))
        .withoutJMXReporting()
        .build();
  }
}
