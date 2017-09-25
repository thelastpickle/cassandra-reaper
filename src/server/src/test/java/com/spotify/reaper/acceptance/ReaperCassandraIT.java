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

package com.spotify.reaper.acceptance;

import com.spotify.reaper.AppContext;
import com.spotify.reaper.acceptance.ReaperTestJettyRunner.ReaperJettyTestSupport;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
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
    features = "classpath:com.spotify.reaper.acceptance/integration_reaper_functionality.feature"
    )
public final class ReaperCassandraIT {

  private static final Logger LOG = LoggerFactory.getLogger(ReaperCassandraIT.class);
  private static final List<ReaperTestJettyRunner> RUNNER_INSTANCES = new CopyOnWriteArrayList<>();
  private static final String CASS_CONFIG_FILE = "cassandra-reaper-cassandra-at.yaml";
  private static final Random RAND = new Random(System.nanoTime());
  private static Thread GRIM_REAPER;

  private ReaperCassandraIT() {}

  @BeforeClass
  public static void setUp() throws Exception {
    LOG.info(
        "setting up testing Reaper runner with {} seed hosts defined and cassandra storage",
        TestContext.TEST_CLUSTER_SEED_HOSTS.size());

    int minReaperInstances = Integer.getInteger("grim.reaper.min", 1);
    int maxReaperInstances = Integer.getInteger("grim.reaper.max", minReaperInstances);

    initSchema();
    for (int i = 0; i < minReaperInstances; ++i) {
      ReaperTestJettyRunner runner = new ReaperTestJettyRunner();
      runner.setup(new AppContext(), CASS_CONFIG_FILE);
      RUNNER_INSTANCES.add(runner);
      BasicSteps.addReaperRunner(runner);
    }

    GRIM_REAPER = new Thread(() -> {
      Thread.currentThread().setName("GRIM REAPER");
      while (!Thread.currentThread().isInterrupted()) { //keep adding/removing reaper instances while test is running
        try {
          if (maxReaperInstances > RUNNER_INSTANCES.size()) {
            ReaperTestJettyRunner runner = new ReaperTestJettyRunner();
            ReaperJettyTestSupport instance = runner.setup(new AppContext(), CASS_CONFIG_FILE);
            RUNNER_INSTANCES.add(runner);
            Thread.sleep(100);
            BasicSteps.addReaperRunner(runner);
          } else {
            int remove = minReaperInstances + RAND.nextInt(maxReaperInstances - minReaperInstances);
            ReaperTestJettyRunner runner = RUNNER_INSTANCES.get(remove);
            BasicSteps.removeReaperRunner(runner);
            Thread.sleep(200);
            runner.runnerInstance.after();
            RUNNER_INSTANCES.remove(runner);
          }
        } catch (RuntimeException | InterruptedException ex) {
          LOG.error("failed adding/removing reaper instance", ex);
        }
      }
    });
    if (minReaperInstances < maxReaperInstances) {
      GRIM_REAPER.start();
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
    GRIM_REAPER.interrupt();
    RUNNER_INSTANCES.forEach(r -> r.runnerInstance.after());
  }

  private static Cluster buildCluster() {
    return Cluster.builder()
        .addContactPoint("127.0.0.1")
        .withSocketOptions(new SocketOptions().setConnectTimeoutMillis(20000).setReadTimeoutMillis(40000))
        .build();
  }
}
