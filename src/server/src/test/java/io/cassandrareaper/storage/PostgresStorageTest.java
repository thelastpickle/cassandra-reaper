/*
 *
 * Copyright 2019-2019 The Last Pickle Ltd
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

package io.cassandrareaper.storage;

import io.cassandrareaper.AppContext;
import io.cassandrareaper.core.GenericMetric;
import io.cassandrareaper.storage.postgresql.IStoragePostgreSql;
import io.cassandrareaper.storage.postgresql.UuidUtil;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.Reader;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.ibatis.common.jdbc.ScriptRunner;
import org.fest.assertions.api.Assertions;
import org.h2.tools.Server;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;

public class PostgresStorageTest {

  private static final String DB_URL = "jdbc:h2:mem:test_mem;MODE=PostgreSQL;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false";

  @Before
  public void setUp() throws SQLException, IOException {
    Server.createTcpServer().start();

    DBI dbi = new DBI(DB_URL);
    Handle handle = dbi.open();
    Connection conn = handle.getConnection();

    // to suppress output of ScriptRunner
    PrintStream tmp = new PrintStream(new OutputStream() {
      @Override
      public void write(int buff) throws IOException {
        // do nothing
      }
    });
    PrintStream console = System.out;
    System.setOut(tmp);

    String cwd = Paths.get("").toAbsolutePath().toString();
    String path = cwd + "/../src/test/resources/db/postgres/V17_0_0__multi_instance.sql";
    ScriptRunner scriptExecutor = new ScriptRunner(conn, false, true);
    Reader reader = new BufferedReader(new FileReader(path));
    scriptExecutor.runScript(reader);

    System.setOut(console);
  }

  @Test
  public void testTakeLead() {
    DBI dbi = new DBI(DB_URL);
    UUID reaperInstanceId = UUID.randomUUID();
    PostgresStorage storage = new PostgresStorage(reaperInstanceId, dbi);
    Assertions.assertThat(storage.isStorageConnected()).isTrue();

    Handle handle = dbi.open();
    handle.execute("DELETE from leader");

    int numEntries = 5;
    Set<UUID> leaderIds = new HashSet<>();
    for (int i = 0; i < numEntries; i++) {
      UUID msbLeaderId = UuidUtil.fromSequenceId(UuidUtil.toSequenceId(UUID.randomUUID()));
      leaderIds.add(msbLeaderId);
    }

    // insert all five leader entries
    for (UUID leaderId : leaderIds) {
      boolean result = storage.takeLead(leaderId);
      Assertions.assertThat(result).isEqualTo(true);
    }

    // make sure fetched leaders has all the inserted leaders
    List<UUID> fetchedLeaderIds = storage.getLeaders();
    for (UUID fetchedLeaderId : fetchedLeaderIds) {
      Assertions.assertThat(leaderIds.contains(fetchedLeaderId)).isTrue();
    }
  }

  @Test
  public void testNoLeaders() {
    DBI dbi = new DBI(DB_URL);
    UUID reaperInstanceId = UUID.randomUUID();
    PostgresStorage storage = new PostgresStorage(reaperInstanceId, dbi);
    Assertions.assertThat(storage.isStorageConnected()).isTrue();

    Handle handle = dbi.open();
    handle.execute("DELETE from leader");

    List<UUID> fetchedLeaderIds = storage.getLeaders();
    Assertions.assertThat(fetchedLeaderIds.size()).isEqualTo(0);
  }

  @Test
  public void testRenewLead() throws InterruptedException {
    DBI dbi = new DBI(DB_URL);
    UUID reaperInstanceId = UUID.randomUUID();
    PostgresStorage storage = new PostgresStorage(reaperInstanceId, dbi);
    Assertions.assertThat(storage.isStorageConnected()).isTrue();

    Handle handle = dbi.open();
    handle.execute("DELETE from leader");

    UUID leaderId = UUID.randomUUID();
    int sleepTime = 3;

    final Instant initialTime = Instant.now();
    storage.takeLead(leaderId);

    // sleep 3 seconds, then renew lead
    TimeUnit.SECONDS.sleep(sleepTime);
    Assertions.assertThat(storage.renewLead(leaderId)).isTrue();

    Instant hbTime = handle.createQuery("SELECT last_heartbeat FROM leader")
        .mapTo(Timestamp.class)
        .first()
        .toInstant();

    Duration between = Duration.between(initialTime, hbTime);
    Assertions.assertThat(between.getSeconds()).isGreaterThanOrEqualTo(sleepTime);
  }

  @Test
  public void testReleaseLead() {
    DBI dbi = new DBI(DB_URL);
    UUID reaperInstanceId = UUID.randomUUID();
    PostgresStorage storage = new PostgresStorage(reaperInstanceId, dbi);
    Assertions.assertThat(storage.isStorageConnected()).isTrue();

    Handle handle = dbi.open();
    handle.execute("DELETE from leader");

    UUID leaderIdForSelf = UUID.randomUUID();
    UUID leaderIdForOther = UUID.randomUUID();

    storage.takeLead(leaderIdForSelf);
    storage.takeLead(leaderIdForOther);

    List<UUID> fetchedLeaderIds = storage.getLeaders();
    Assertions.assertThat(fetchedLeaderIds.size()).isEqualTo(2);

    handle.createStatement("UPDATE leader SET reaper_instance_id = 0 WHERE leader_id = :id")
        .bind("id", UuidUtil.toSequenceId(leaderIdForOther))
        .execute();

    // test that releaseLead succeeds for entry where instance_id = self
    storage.releaseLead(leaderIdForSelf);
    fetchedLeaderIds = storage.getLeaders();
    Assertions.assertThat(fetchedLeaderIds.size()).isEqualTo(1);

    // test that releaseLead fails for entry where instance_id != self
    storage.releaseLead(leaderIdForOther);
    fetchedLeaderIds = storage.getLeaders();
    Assertions.assertThat(fetchedLeaderIds.size()).isEqualTo(1);
  }

  @Test
  public void testSaveHeartbeat() {
    DBI dbi = new DBI(DB_URL);
    UUID reaperInstanceId = UUID.randomUUID();
    PostgresStorage storage = new PostgresStorage(reaperInstanceId, dbi);
    Assertions.assertThat(storage.isStorageConnected()).isTrue();

    Handle handle = dbi.open();
    handle.execute("DELETE from running_reapers");

    storage.saveHeartbeat();
    int numReapers = storage.countRunningReapers();
    Assertions.assertThat(numReapers).isEqualTo(1);
  }

  @Test
  public void testNodeOperations() {
    DBI dbi = new DBI(DB_URL);
    UUID reaperInstanceId = UUID.randomUUID();
    PostgresStorage storage = new PostgresStorage(reaperInstanceId, dbi);
    Assertions.assertThat(storage.isStorageConnected()).isTrue();

    Handle handle = dbi.open();
    handle.execute("DELETE from node_operations");

    storage.storeOperations("fake_cluster", OpType.OP_STREAMING, "fake_host", "data1");
    String data = storage.listOperations("fake_cluster", OpType.OP_STREAMING, "fake_host");
    Assertions.assertThat(data.equals("data1"));
    storage.storeOperations("fake_cluster", OpType.OP_STREAMING, "fake_host", "data2");

    data = storage.listOperations("fake_cluster", OpType.OP_STREAMING, "fake_host");
    Assertions.assertThat(data.equals("data2"));
  }

  @Test
  public void testGenericMetricsByHostandCluster() {
    DBI dbi = new DBI(DB_URL);
    UUID reaperInstanceId = UUID.randomUUID();
    PostgresStorage storage = new PostgresStorage(reaperInstanceId, dbi);
    Assertions.assertThat(storage.isStorageConnected()).isTrue();

    Handle handle = dbi.open();
    handle.execute("DELETE from node_metrics_v2");
    handle.execute("DELETE from node_metrics_v2_source_nodes");
    handle.execute("DELETE from node_metrics_v2_metric_types");

    DateTime now = DateTime.now();
    GenericMetric metric1 = GenericMetric.builder()
        .withClusterName("fake_cluster")
        .withHost("fake_host1")
        .withTs(now)
        .withMetricDomain("org.apache.cassandra.metrics")
        .withMetricType("ThreadPool")
        .withMetricName("PendingTasks")
        .withMetricScope("MutationStage")
        .withMetricAttribute("fake_attribute")
        .withValue(12)
        .build();
    GenericMetric metric2 = GenericMetric.builder()  // different metric, different host
        .withClusterName("fake_cluster")
        .withHost("fake_host2")
        .withTs(now)
        .withMetricDomain("org.apache.cassandra.metrics")
        .withMetricType("ThreadPool")
        .withMetricName("ActiveTasks")
        .withMetricScope("MutationStage")
        .withMetricAttribute("fake_attribute")
        .withValue(14)
        .build();

    storage.storeMetric(metric1);
    storage.storeMetric(metric2);

    // verify that the two metrics above can be queried by cluster name
    Set<String> expectedMetrics = new HashSet<>();
    expectedMetrics.add("PendingTasks");
    expectedMetrics.add("ActiveTasks");
    List<GenericMetric> retrievedMetrics = storage.getMetrics(
        "fake_cluster",
        Optional.empty(),
        "org.apache.cassandra.metrics",
        "ThreadPool",
        now.getMillis()
    );
    for (GenericMetric retrievedMetric : retrievedMetrics) {
      Assertions.assertThat(expectedMetrics.contains(retrievedMetric.getMetricName()));
      expectedMetrics.remove(retrievedMetric.getMetricName());
    }

    // verify that metrics can be queried by host
    retrievedMetrics = storage.getMetrics(
        "fake_cluster",
        Optional.of("fake_host2"),
        "org.apache.cassandra.metrics",
        "ThreadPool",
        now.getMillis()
    );
    Assertions.assertThat(retrievedMetrics.size() == 1);
    GenericMetric retrievedMetric = retrievedMetrics.get(0);
    Assertions.assertThat(retrievedMetric.getMetricName().equals("ActiveTasks"));
  }

  @Test
  public void testGenericMetricExpiration() {
    DBI dbi = new DBI(DB_URL);
    UUID reaperInstanceId = UUID.randomUUID();
    PostgresStorage storage = new PostgresStorage(reaperInstanceId, dbi);
    Assertions.assertThat(storage.isStorageConnected()).isTrue();

    Handle handle = dbi.open();
    handle.execute("DELETE from node_metrics_v2");
    handle.execute("DELETE from node_metrics_v2_source_nodes");
    handle.execute("DELETE from node_metrics_v2_metric_types");


    DateTime expirationTime = DateTime.now().minusMinutes(3);
    GenericMetric expiredMetric = GenericMetric.builder()
        .withClusterName("fake_cluster")
        .withHost("fake_host1")
        .withTs(expirationTime)
        .withMetricDomain("org.apache.cassandra.metrics")
        .withMetricType("ThreadPool")
        .withMetricName("PendingTasks")
        .withMetricScope("MutationStage")
        .withMetricAttribute("fake_attribute")
        .withValue(12)
        .build();
    storage.storeMetric(expiredMetric);

    // verify that the metric was stored in the DB
    List<GenericMetric> retrievedMetrics = storage.getMetrics(
        "fake_cluster",
        Optional.empty(),
        "org.apache.cassandra.metrics",
        "ThreadPool",
        expirationTime.getMillis()
    );
    Assertions.assertThat(retrievedMetrics.size() == 1);
    List<Map<String, Object>> rs = handle.select("SELECT COUNT(*) AS count FROM node_metrics_v2_source_nodes");
    long numSourceNodes = (long) rs.get(0).get("count");
    Assertions.assertThat(numSourceNodes == 1);

    // verify that on purgeMetrics(), metric is purged since it's older than 3 minutes
    storage.purgeMetrics();
    retrievedMetrics = storage.getMetrics(
        "fake_cluster",
        Optional.empty(),
        "org.apache.cassandra.metrics",
        "ThreadPool",
        expirationTime.getMillis()
    );
    Assertions.assertThat(retrievedMetrics.size() == 0);

    // verify that source nodes have also been purged
    rs = handle.select("SELECT COUNT(*) AS count FROM node_metrics_v2_source_nodes");
    numSourceNodes = (long) rs.get(0).get("count");
    Assertions.assertThat(numSourceNodes == 1);
    Assertions.assertThat(numSourceNodes == 0);
  }

  /*
  The following tests rely on timeouts; will take a few minutes to complete
   */
  @Test
  public void testUpdateLeaderEntry() throws InterruptedException {
    System.out.println("Testing leader timeout (this will take a minute)...");
    DBI dbi = new DBI(DB_URL);
    UUID reaperInstanceId = UUID.randomUUID();
    PostgresStorage storage = new PostgresStorage(reaperInstanceId, dbi, 1, 1, 1, 1);
    Assertions.assertThat(storage.isStorageConnected()).isTrue();

    Handle handle = dbi.open();
    handle.execute("DELETE from leader");

    UUID leaderId = UUID.randomUUID();

    storage.takeLead(leaderId);
    List<UUID> fetchedLeaderIds = storage.getLeaders();
    Assertions.assertThat(fetchedLeaderIds.size()).isEqualTo(1);

    boolean result = storage.takeLead(leaderId); // should not work bc entry already exist
    Assertions.assertThat(result).isFalse();

    int rowsUpdated = handle.createStatement(IStoragePostgreSql.SQL_UPDATE_LEAD)
        .bind("reaperInstanceId", UuidUtil.toSequenceId(reaperInstanceId))
        .bind("reaperInstanceHost", AppContext.REAPER_INSTANCE_ADDRESS)
        .bind("leaderId", UuidUtil.toSequenceId(leaderId))
        .bind("expirationTime", Instant.now().minus(Duration.ofSeconds(60)))
        .execute();

    Assertions.assertThat(rowsUpdated).isEqualTo(0);  // should not b/c original entry hasn't expired yet

    TimeUnit.SECONDS.sleep(60);

    rowsUpdated = handle.createStatement(IStoragePostgreSql.SQL_UPDATE_LEAD)
        .bind("reaperInstanceId", UuidUtil.toSequenceId(reaperInstanceId))
        .bind("reaperInstanceHost", AppContext.REAPER_INSTANCE_ADDRESS)
        .bind("leaderId", UuidUtil.toSequenceId(leaderId))
        .bind("expirationTime", Instant.now().minus(Duration.ofSeconds(60)))
        .execute();

    Assertions.assertThat(rowsUpdated).isEqualTo(1);  // should update b/c original entry has expired
  }
}
