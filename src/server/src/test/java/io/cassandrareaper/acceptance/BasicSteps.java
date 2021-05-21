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
import io.cassandrareaper.SimpleReaperClient;
import io.cassandrareaper.core.DiagEventSubscription;
import io.cassandrareaper.core.DroppedMessages;
import io.cassandrareaper.core.MetricsHistogram;
import io.cassandrareaper.core.RepairRun;
import io.cassandrareaper.core.RepairSegment;
import io.cassandrareaper.core.Snapshot;
import io.cassandrareaper.core.ThreadPoolStat;
import io.cassandrareaper.resources.view.RepairRunStatus;
import io.cassandrareaper.resources.view.RepairScheduleStatus;
import io.cassandrareaper.service.RepairRunService;
import io.cassandrareaper.storage.CassandraStorage;
import io.cassandrareaper.storage.PostgresStorage;
import io.cassandrareaper.storage.postgresql.DiagEventSubscriptionMapper;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import javax.ws.rs.core.Response;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.VersionNumber;
import com.datastax.driver.core.exceptions.AlreadyExistsException;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.apache.commons.lang3.StringUtils;
import org.assertj.core.api.Assertions;
import org.awaitility.Duration;
import org.awaitility.core.ConditionTimeoutException;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Basic acceptance test (Cucumber) steps.
 */
@SuppressWarnings("ResultOfMethodCallIgnored")
public final class BasicSteps {

  private static final Logger LOG = LoggerFactory.getLogger(BasicSteps.class);
  private static final Optional<Map<String, String>> EMPTY_PARAMS = Optional.empty();
  private static final Duration POLL_INTERVAL = Duration.TWO_SECONDS;

  private static final List<ReaperTestJettyRunner> RUNNERS = new CopyOnWriteArrayList<>();
  private static final List<SimpleReaperClient> CLIENTS = new CopyOnWriteArrayList<>();
  private static final Random RAND = new Random(System.nanoTime());
  private static final AtomicReference<Upgradable> TEST_INSTANCE = new AtomicReference<>();

  private static final Map<String,String> EVENT_TYPES = ImmutableMap.<String,String>builder()
      .put("AuditEvent", "org.apache.cassandra.audit.AuditEvent")
      .put("BootstrapEvent", "org.apache.cassandra.dht.BootstrapEvent")
      .put("GossiperEvent", "org.apache.cassandra.gms.GossiperEvent")
      .put("HintEvent", "org.apache.cassandra.hints.HintEvent")
      .put("HintsServiceEvent", "org.apache.cassandra.hints.HintsServiceEvent")
      .put("ReadRepairEvent", "org.apache.cassandra.service.reads.repair.ReadRepairEvent")
      .put("SchemaEvent", "org.apache.cassandra.schema.SchemaEvent")
      .build();

  private Optional<String> reaperVersion = Optional.empty();
  private Response lastResponse;
  private TestContext testContext;

  public static synchronized void addReaperRunner(ReaperTestJettyRunner runner) {
    if (!CLIENTS.isEmpty()) {
      Preconditions.checkState(isInstanceOfDistributedStorage(runner.runnerInstance.getContextStorageClassname()));
      RUNNERS.stream()
          .forEach(r ->
              Preconditions.checkState(
                  isInstanceOfDistributedStorage(runner.runnerInstance.getContextStorageClassname())
              ));
    }
    RUNNERS.add(runner);
    CLIENTS.add(runner.getClient());
  }

  public static synchronized void removeReaperRunner(ReaperTestJettyRunner runner) {
    CLIENTS.remove(runner.getClient());
    RUNNERS.remove(runner);
  }

  static void setup(Upgradable testInstance) {
    Preconditions.checkState(null == TEST_INSTANCE.get());
    TEST_INSTANCE.set(testInstance);
  }

  private static void callAndExpect(
      String httpMethod,
      String callPath,
      Optional<Map<String, String>> params,
      Optional<String> expectedDataInResponseData,
      Response.Status... expectedStatuses) {

    RUNNERS.parallelStream().forEach(runner -> {
      Response response = runner.callReaper(httpMethod, callPath, params);
      String responseEntity = response.readEntity(String.class);

      Assertions
          .assertThat(Arrays.asList(expectedStatuses).stream().map(Response.Status::getStatusCode))
          .withFailMessage(responseEntity)
          .contains(response.getStatus())
          .isNotEmpty();

      if (1 == RUNNERS.size() && expectedStatuses[0].getStatusCode() != response.getStatus()) {
        // we can't fail on this because the jersey client sometimes sends
        // duplicate http requests and the wrong request responds firs
        LOG.error(
            "AssertionError: expected: {} but was: {}",
            expectedStatuses[0].getStatusCode(), response.getStatus());
      }

      if (expectedStatuses[0].getStatusCode() == response.getStatus()) {
        if (expectedDataInResponseData.isPresent()) {
          if (Sets.newHashSet("PUT", "POST").contains(httpMethod)) {
            // rest command requests should not response with bodies, follow the location to GET that
            Assertions.assertThat(responseEntity).isEmpty();
            // follow to new location (to GET resource)
            response = runner.callReaper("GET", response.getLocation().toString(), Optional.empty());
            responseEntity = response.readEntity(String.class);
          } else if ("DELETE".equals(httpMethod)) {
            throw new IllegalArgumentException("tests can't expect response body from DELETE request");
          }
          assertTrue(
              "expected data [" + expectedDataInResponseData.get() + "]  not found from the response: ["
                  + responseEntity + "]",
              0 != responseEntity.length() && responseEntity.contains(expectedDataInResponseData.get()));

          LOG.debug("Data \"" + expectedDataInResponseData.get() + "\" was found from response data");
        }
      }
    });
  }

  @Given("^cluster seed host \"([^\"]*)\" points to cluster with name \"([^\"]*)\"$")
  public void cluster_seed_host_points_to_cluster_with_name(String seedHost, String clusterName) throws Throwable {
    synchronized (BasicSteps.class) {
      TestContext.SEED_HOST = seedHost + '@' + clusterName;
      TestContext.addSeedHostToClusterMapping(seedHost, clusterName);
    }
  }

  @Given("^cluster \"([^\"]*)\" has keyspace \"([^\"]*)\" with tables \"([^\"]*)\"$")
  public void ccm_cluster_has_keyspace_with_tables(
      String clusterName,
      String keyspace,
      String tablesListStr) throws Throwable {

    synchronized (BasicSteps.class) {
      Set<String> tables = Sets.newHashSet(RepairRunService.COMMA_SEPARATED_LIST_SPLITTER.split(tablesListStr));
      createKeyspace(keyspace);
      tables.stream().forEach(tableName -> createTable(keyspace, tableName));
      TestContext.addClusterInfo(clusterName, keyspace, tables);
    }
  }

  @Given("^that reaper ([^\"]*) is running$")
  public void start_reaper(String version) throws Throwable {
    synchronized (BasicSteps.class) {
      testContext = new TestContext();
      Optional<String> newVersion = version.trim().isEmpty() ? Optional.empty() : Optional.of(version);
      if (RUNNERS.isEmpty() || !newVersion.equals(reaperVersion)) {
        if (null == TEST_INSTANCE.get()) {
          throw new AssertionError(
              "Running upgrade tests is not supported with this IT. The test must subclass and implement Upgradable");
        }
        reaperVersion = newVersion;
        TEST_INSTANCE.get().upgradeReaperRunner(reaperVersion);
      }
    }
  }

  @When("^reaper is upgraded to latest$")
  public void upgrade_reaper() throws Throwable {
    synchronized (BasicSteps.class) {
      if (reaperVersion.isPresent()) {
        reaperVersion = Optional.empty();
        TEST_INSTANCE.get().upgradeReaperRunner(Optional.empty());
      }
    }
  }

  @And("^reaper has no cluster with name \"([^\"]*)\" in storage$")
  public void reaper_has_no_cluster_with_name_in_storage(String clusterName) throws Throwable {
    synchronized (BasicSteps.class) {
      callAndExpect("GET", "/cluster/" + clusterName,
          Optional.<Map<String, String>>empty(),
          Optional.<String>empty(), Response.Status.NOT_FOUND);
    }
  }

  @And("^reaper has no cluster in storage$")
  public void reaper_has_no_cluster_in_storage() throws Throwable {
    synchronized (BasicSteps.class) {
      RUNNERS.parallelStream().forEach(runner -> {
        Response response = runner.callReaper("GET", "/cluster/", Optional.<Map<String, String>>empty());
        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        String responseData = response.readEntity(String.class);
        Assertions.assertThat(responseData).isNotBlank();
        List<String> clusterNames = SimpleReaperClient.parseClusterNameListJSON(responseData);
        if (!((AppContext) runner.runnerInstance.getContext()).config.isInSidecarMode()) {
          // Sidecar self registers clusters
          assertEquals(0, clusterNames.size());
        }
      });
    }
  }

  @When("^an add-cluster request is made to reaper$")
  public void an_add_cluster_request_is_made_to_reaper() throws Throwable {
    synchronized (BasicSteps.class) {
      RUNNERS.parallelStream().forEach(runner -> {
        Map<String, String> params = Maps.newHashMap();
        params.put("seedHost", TestContext.SEED_HOST);
        params.put("jmxPort", "7100");
        Response response = runner.callReaper("POST", "/cluster", Optional.of(params));
        int responseStatus = response.getStatus();
        String responseEntity = response.readEntity(String.class);

        Assertions.assertThat(
                ImmutableList.of(
                    Response.Status.CREATED.getStatusCode(),
                    Response.Status.NO_CONTENT.getStatusCode(),
                    Response.Status.OK.getStatusCode()))
            .withFailMessage(responseEntity)
            .contains(responseStatus);

        // rest command requests should not response with bodies, follow the location to GET that
        Assertions.assertThat(responseEntity).isEmpty();

        // follow to new location (to GET resource)
        response = runner.callReaper("GET", response.getLocation().toString(), Optional.empty());

        String responseData = response.readEntity(String.class);
        Assertions.assertThat(responseData).isNotBlank();
        Map<String, Object> cluster = SimpleReaperClient.parseClusterStatusJSON(responseData);

        if (Response.Status.CREATED.getStatusCode() == responseStatus
            || ((AppContext) runner.runnerInstance.getContext()).config.isInSidecarMode()) {
          TestContext.TEST_CLUSTER = (String) cluster.get("name");
        }
      });

      callAndExpect(
          "GET",
          "/cluster/" + TestContext.TEST_CLUSTER,
          Optional.<Map<String, String>>empty(),
          Optional.<String>empty(),
          Response.Status.OK);
    }
  }

  @When("^an add-cluster request is made to reaper with authentication$")
  public void an_add_cluster_request_is_made_to_reaper_with_authentication() throws Throwable {
    synchronized (BasicSteps.class) {
      System.setProperty("REAPER_ENCRYPTION_KEY", "reaper");
      RUNNERS.parallelStream().forEach(runner -> {
        Map<String, String> params = Maps.newHashMap();
        params.put("seedHost", TestContext.SEED_HOST);
        params.put("jmxPort", "7100");
        params.put("jmxUsername", "cassandra");
        params.put("jmxPassword", "cassandrapassword");
        Response response = runner.callReaper("POSTFORM", "/cluster/auth", Optional.of(params));
        int responseStatus = response.getStatus();
        String responseEntity = response.readEntity(String.class);

        Assertions.assertThat(
                ImmutableList.of(
                    Response.Status.CREATED.getStatusCode(),
                    Response.Status.NO_CONTENT.getStatusCode(),
                    Response.Status.OK.getStatusCode()))
            .withFailMessage(responseEntity)
            .contains(responseStatus);

        // rest command requests should not response with bodies, follow the location to GET that
        Assertions.assertThat(responseEntity).isEmpty();

        // follow to new location (to GET resource)
        response = runner.callReaper("GET", response.getLocation().toString(), Optional.empty());

        String responseData = response.readEntity(String.class);
        Assertions.assertThat(responseData).isNotBlank();
        Map<String, Object> cluster = SimpleReaperClient.parseClusterStatusJSON(responseData);

        if (Response.Status.CREATED.getStatusCode() == responseStatus
            || ((AppContext) runner.runnerInstance.getContext()).config.isInSidecarMode()) {
          TestContext.TEST_CLUSTER = (String) cluster.get("name");
        }
      });

      callAndExpect(
          "GET",
          "/cluster/" + TestContext.TEST_CLUSTER,
          Optional.<Map<String, String>>empty(),
          Optional.<String>empty(),
          Response.Status.OK);
    }
  }

  @When("^an add-cluster request is made to reaper with authentication and no encryption$")
  public void an_add_cluster_request_is_made_to_reaper_with_authentication_and_no_encryption() throws Throwable {
    synchronized (BasicSteps.class) {
      System.clearProperty("REAPER_ENCRYPTION_KEY");
      RUNNERS.parallelStream().forEach(runner -> {
        Map<String, String> params = Maps.newHashMap();
        params.put("seedHost", TestContext.SEED_HOST);
        params.put("jmxPort", "7100");
        params.put("jmxUsername", "cassandra");
        params.put("jmxPassword", "cassandrapassword");
        Response response = runner.callReaper("POSTFORM", "/cluster/auth", Optional.of(params));
        int responseStatus = response.getStatus();
        String responseEntity = response.readEntity(String.class);

        // This should fail because there's no encryption key in the system env variables
        Assertions.assertThat(
                ImmutableList.of(
                    Response.Status.BAD_REQUEST.getStatusCode(),
                    Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()))
            .withFailMessage(responseEntity)
            .contains(responseStatus);

      });

      callAndExpect(
          "GET",
          "/cluster/" + TestContext.TEST_CLUSTER,
          Optional.<Map<String, String>>empty(),
          Optional.<String>empty(),
          Response.Status.NOT_FOUND);
    }
  }

  @Then("^reaper has a cluster called \"([^\"]*)\" in storage$")
  public void reaper_has_a_cluster_called_in_storage(String clusterName) throws Throwable {
    synchronized (BasicSteps.class) {
      callAndExpect("GET", "/cluster/" + clusterName,
          Optional.<Map<String, String>>empty(),
          Optional.<String>empty(), Response.Status.OK);
    }
  }

  @Then("^reaper has the last added cluster in storage$")
  public void reaper_has_the_last_added_cluster_in_storage() throws Throwable {
    synchronized (BasicSteps.class) {
      callAndExpect("GET", "/cluster/" + TestContext.TEST_CLUSTER,
          Optional.<Map<String, String>>empty(),
          Optional.<String>empty(), Response.Status.OK);
    }
  }

  @And("^reaper has no scheduled repairs for \"([^\"]*)\"$")
  public void reaper_has_no_scheduled_repairs_for(String clusterName) throws Throwable {
    synchronized (BasicSteps.class) {
      callAndExpect("GET", "/repair_schedule/cluster/" + clusterName,
          Optional.<Map<String, String>>empty(),
          Optional.of("[]"), Response.Status.OK);
    }
  }

  @When("^a new daily \"([^\"]*)\" repair schedule is added for \"([^\"]*)\" and keyspace \"([^\"]*)\"$")
  public void a_new_daily_repair_schedule_is_added_for(String repairType, String clusterName, String keyspace)
      throws Throwable {

    synchronized (BasicSteps.class) {
      ReaperTestJettyRunner runner = RUNNERS.get(RAND.nextInt(RUNNERS.size()));
      Map<String, String> params = Maps.newHashMap();
      params.put("clusterName", clusterName);
      params.put("keyspace", keyspace);
      params.put("owner", TestContext.TEST_USER);
      params.put("intensity", "0.9");
      params.put("scheduleDaysBetween", "1");
      params.put("repairParallelism", repairType.equals("incremental") ? "parallel" : "sequential");
      params.put("incrementalRepair", repairType.equals("incremental") ? "True" : "False");
      Response response = runner.callReaper("POST", "/repair_schedule", Optional.of(params));
      int responseStatus = response.getStatus();
      String responseEntity = response.readEntity(String.class);

      Assertions.assertThat(
          ImmutableList.of(
            Response.Status.CREATED.getStatusCode(),
            Response.Status.NO_CONTENT.getStatusCode()))
          .withFailMessage(responseEntity)
          .contains(responseStatus);

      // rest command requests should not response with bodies, follow the location to GET that
      Assertions.assertThat(responseEntity).isEmpty();

      // follow to new location (to GET resource)
      response = runner.callReaper("GET", response.getLocation().toString(), Optional.empty());
      String responseData = response.readEntity(String.class);
      Assertions.assertThat(responseData).isNotBlank();
      RepairScheduleStatus schedule = SimpleReaperClient.parseRepairScheduleStatusJSON(responseData);

      if (Response.Status.CREATED.getStatusCode() == responseStatus) {
        testContext.addCurrentScheduleId(schedule.getId());
      } else {
        // if the original request to create the schedule failed then we have to wait til we can find it
        await().with().pollInterval(POLL_INTERVAL).atMost(1, MINUTES).until(() -> {
          try {
            List<RepairScheduleStatus> schedules = runner.getClient().getRepairSchedulesForCluster(clusterName);
            Assertions.assertThat(schedules).withFailMessage(StringUtils.join(schedules, " , ")).hasSize(1);
            testContext.addCurrentScheduleId(schedules.get(0).getId());
          } catch (AssertionError ex) {
            LOG.warn(ex.getMessage());
            logResponse(runner, "/repair_schedule/cluster/" + TestContext.TEST_CLUSTER);
            return false;
          }
          return true;
        });
      }
    }
  }

  @When("^a new daily \"([^\"]*)\" repair schedule is added for the last added cluster and keyspace \"([^\"]*)\"$")
  public void a_new_daily_repair_schedule_is_added_for_the_last_added_cluster(String repairType, String keyspace)
      throws Throwable {

    synchronized (BasicSteps.class) {
      ReaperTestJettyRunner runner = RUNNERS.get(RAND.nextInt(RUNNERS.size()));
      Map<String, String> params = Maps.newHashMap();
      params.put("clusterName", TestContext.TEST_CLUSTER);
      params.put("keyspace", keyspace);
      params.put("owner", TestContext.TEST_USER);
      params.put("intensity", "0.9");
      params.put("scheduleDaysBetween", "1");
      params.put("repairParallelism", repairType.equals("incremental") ? "parallel" : "sequential");
      params.put("incrementalRepair", repairType.equals("incremental") ? "True" : "False");
      Response response = runner.callReaper("POST", "/repair_schedule", Optional.of(params));
      int responseStatus = response.getStatus();
      String responseEntity = response.readEntity(String.class);

      Assertions
          .assertThat(
            ImmutableList.of(Response.Status.CREATED.getStatusCode(), Response.Status.NO_CONTENT.getStatusCode()))
          .withFailMessage(responseEntity)
          .contains(responseStatus);

      // rest command requests should not response with bodies, follow the location to GET that
      Assertions.assertThat(responseEntity).isEmpty();

      // follow to new location (to GET resource)
      response = runner.callReaper("GET", response.getLocation().toString(), Optional.empty());
      String responseData = response.readEntity(String.class);
      Assertions.assertThat(responseData).isNotBlank();
      RepairScheduleStatus schedule = SimpleReaperClient.parseRepairScheduleStatusJSON(responseData);

      if (Response.Status.CREATED.getStatusCode() == responseStatus) {
        testContext.addCurrentScheduleId(schedule.getId());
      } else {
        // if the original request to create the schedule failed then we have to wait til we can find it
        await().with().pollInterval(POLL_INTERVAL).atMost(1, MINUTES).until(() -> {
          try {
            List<RepairScheduleStatus> schedules
                = runner.getClient().getRepairSchedulesForCluster(TestContext.TEST_CLUSTER);

            Assertions.assertThat(schedules).withFailMessage(StringUtils.join(schedules, " , ")).hasSize(1);
            testContext.addCurrentScheduleId(schedules.get(0).getId());
          } catch (AssertionError ex) {
            LOG.warn(ex.getMessage());
            logResponse(runner, "/repair_schedule/cluster/" + TestContext.TEST_CLUSTER);
            return false;
          }
          return true;
        });
      }
    }
  }

  @When("^a new daily repair schedule is added for the last added cluster "
      + "and keyspace \"([^\"]*)\" with next repair immediately$")
  public void a_new_daily_repair_schedule_is_added_for_the_last_added_cluster_and_keyspace_with_next_repair_immediately(
      String keyspace) throws Throwable {

    synchronized (BasicSteps.class) {
      LOG.info("adding a new daily repair schedule to keyspace: {}", keyspace);
      Map<String, String> params = Maps.newHashMap();
      params.put("clusterName", TestContext.TEST_CLUSTER);
      params.put("keyspace", keyspace);
      params.put("owner", TestContext.TEST_USER);
      params.put("intensity", "0.9");
      params.put("scheduleDaysBetween", "1");
      params.put("scheduleTriggerTime", DateTime.now().plusSeconds(1).toString());
      ReaperTestJettyRunner runner = RUNNERS.get(RAND.nextInt(RUNNERS.size()));
      Response response = runner.callReaper("POST", "/repair_schedule", Optional.of(params));
      int responseStatus = response.getStatus();
      String responseEntity = response.readEntity(String.class);

      Assertions.assertThat(
          ImmutableList.of(
            Response.Status.CREATED.getStatusCode(),
            Response.Status.CONFLICT.getStatusCode()))
          .withFailMessage(responseEntity)
          .contains(responseStatus);

      // non-error rest command requests should not response with bodies
      if (Response.Status.CONFLICT.getStatusCode() != responseStatus) {
        Assertions.assertThat(responseEntity).isEmpty();
      }

      // follow to new location (to GET resource)
      response = runner.callReaper("GET", response.getLocation().toString(), Optional.empty());
      String responseData = response.readEntity(String.class);
      Assertions.assertThat(responseData).isNotBlank();
      RepairScheduleStatus schedule = SimpleReaperClient.parseRepairScheduleStatusJSON(responseData);

      if (Response.Status.CREATED.getStatusCode() == responseStatus) {
        testContext.addCurrentScheduleId(schedule.getId());
      } else {
        // if the original request to create the schedule failed then we have to wait til we can find it
        await().with().pollInterval(POLL_INTERVAL).atMost(1, MINUTES).until(() -> {
          try {
            List<RepairScheduleStatus> schedules
                = runner.getClient().getRepairSchedulesForCluster(TestContext.TEST_CLUSTER);

            Assertions.assertThat(schedules).withFailMessage(StringUtils.join(schedules, " , ")).hasSize(1);
            testContext.addCurrentScheduleId(schedules.get(0).getId());
          } catch (AssertionError ex) {
            LOG.warn(ex.getMessage());
            logResponse(runner, "/repair_schedule/cluster/" + TestContext.TEST_CLUSTER);
            return false;
          }
          return true;
        });
      }
    }
  }

  @And("^we wait for a scheduled repair run has started for cluster \"([^\"]*)\"$")
  public void a_scheduled_repair_run_has_started_for_cluster(String clusterName) throws Throwable {
    synchronized (BasicSteps.class) {
      final Set<UUID> runningRepairs = Sets.newConcurrentHashSet();
      RUNNERS.parallelStream().forEach(runner -> {
        LOG.info("waiting for a scheduled repair run to start for cluster: {}", clusterName);
        await().with().pollInterval(POLL_INTERVAL).atMost(2, MINUTES).until(() -> {

          Response resp = runner.callReaper("GET", "/repair_run/cluster/" + TestContext.TEST_CLUSTER, EMPTY_PARAMS);
          String responseData = resp.readEntity(String.class);

          if (Response.Status.OK.getStatusCode() != resp.getStatus() || StringUtils.isBlank(responseData)) {
            return false;
          }

          List<RepairRunStatus> runs = SimpleReaperClient.parseRepairRunStatusListJSON(responseData)
              .stream()
              .filter(r -> RepairRun.RunState.RUNNING == r.getState() || RepairRun.RunState.DONE == r.getState())
              .filter(r -> r.getCause().contains(testContext.getCurrentScheduleId().toString()))
              .collect(Collectors.toList());

          if (1 < runs.size()) {
            LOG.error("found duplicate repairs from same schedule and trigger time. deleting those behind…");
            logResponse(runner, "/repair_run/cluster/" + TestContext.TEST_CLUSTER);

            UUID toKeep = runs.stream()
                .sorted((r0, r1) -> (r0.getSegmentsRepaired() != r1.getSegmentsRepaired()
                      ? r1.getSegmentsRepaired() - r0.getSegmentsRepaired() : r0.getId().compareTo(r1.getId())))
                .findFirst()
                .get()
                .getId();

            Optional<Map<String,String>> deleteParams = Optional.of(ImmutableMap.of("owner", TestContext.TEST_USER));

            // pause the other repairs first
            runs.stream()
                .filter(r -> !r.getId().equals(toKeep))
                .forEachOrdered(r -> {
                  Response res = runner.callReaper("PUT", "/repair_run/" + r.getId() + "/state/PAUSED", EMPTY_PARAMS);
                  LOG.warn(res.readEntity(String.class));
                });

            // then delete the other repairs
            runs.stream()
                .filter(r -> !r.getId().equals(toKeep))
                .forEachOrdered(r -> {
                  Response res = runner.callReaper("DELETE", "/repair_run/" + r.getId(), deleteParams);
                  LOG.warn(res.readEntity(String.class));
                });

            return false;
          }

          if (runs.isEmpty()) {
            return false;
          }
          runningRepairs.add(runs.get(0).getId());
          return true;
        });
      });
      Assertions.assertThat(runningRepairs).hasSize(1);
      testContext.addCurrentRepairId(runningRepairs.iterator().next());
    }
  }

  @And("^reaper has scheduled repair for cluster called \"([^\"]*)\"$")
  public void reaper_has_scheduled_repair_for_cluster_called(String clusterName) throws Throwable {
    synchronized (BasicSteps.class) {
      callAndExpect(
          "GET",
          "/repair_schedule/cluster/" + clusterName,
          EMPTY_PARAMS,
          Optional.of("\"" + clusterName + "\""),
          Response.Status.OK);
    }
  }

  @And("^a second daily repair schedule is added for \"([^\"]*)\" and keyspace \"([^\"]*)\"$")
  public void a_second_daily_repair_schedule_is_added_for_and_keyspace(String clusterName, String keyspace)
      throws Throwable {

    synchronized (BasicSteps.class) {
      LOG.info("add second daily repair schedule: {}/{}", clusterName, keyspace);
      Map<String, String> params = Maps.newHashMap();
      params.put("clusterName", clusterName);
      params.put("keyspace", keyspace);
      params.put("owner", TestContext.TEST_USER);
      params.put("intensity", "0.8");
      params.put("scheduleDaysBetween", "1");
      ReaperTestJettyRunner runner = RUNNERS.get(RAND.nextInt(RUNNERS.size()));
      Response response = runner.callReaper("POST", "/repair_schedule", Optional.of(params));
      int responseStatus = response.getStatus();
      assertEquals(Response.Status.CREATED.getStatusCode(), responseStatus);
      // rest command requests should not response with bodies, follow the location to GET that
      Assertions.assertThat(response.readEntity(String.class)).isEmpty();
      // follow to new location (to GET resource)
      response = runner.callReaper("GET", response.getLocation().toString(), Optional.empty());
      String responseData = response.readEntity(String.class);
      Assertions.assertThat(responseData).isNotBlank();
      RepairScheduleStatus schedule = SimpleReaperClient.parseRepairScheduleStatusJSON(responseData);
      testContext.addCurrentScheduleId(schedule.getId());
    }
  }

  @And("^reaper has (\\d+) scheduled repairs for cluster called \"([^\"]*)\"$")
  public void reaper_has_scheduled_repairs_for_cluster_called(
      int expectedSchedules,
      String clusterName) throws Throwable {

    synchronized (BasicSteps.class) {
      CLIENTS.parallelStream().forEach(client -> {
        await().with().pollInterval(POLL_INTERVAL).atMost(1, MINUTES).until(() -> {
          try {
            List<RepairScheduleStatus> schedules = client.getRepairSchedulesForCluster(clusterName);

            Assertions
                .assertThat(schedules)
                .withFailMessage(StringUtils.join(schedules, " , "))
                .hasSize(expectedSchedules);
          } catch (AssertionError ex) {
            LOG.warn(ex.getMessage());
            logResponse(RUNNERS.get(0), "/repair_schedule/cluster/" + TestContext.TEST_CLUSTER);
            return false;
          }
          return true;
        });
      });
    }
  }

  @And("^reaper has (\\d+) scheduled repairs for the last added cluster$")
  public void reaper_has_scheduled_repairs_for_the_last_added_cluster(int expectedSchedules) throws Throwable {
    synchronized (BasicSteps.class) {
      CLIENTS.parallelStream().forEach(client -> {
        await().with().pollInterval(POLL_INTERVAL).atMost(1, MINUTES).until(() -> {
          try {
            List<RepairScheduleStatus> schedules = client.getRepairSchedulesForCluster(TestContext.TEST_CLUSTER);

            Assertions.assertThat(schedules)
                .withFailMessage(StringUtils.join(schedules, " , "))
                .hasSize(expectedSchedules);
          } catch (AssertionError ex) {
            LOG.warn(ex.getMessage());
            logResponse(RUNNERS.get(0), "/repair_schedule/cluster/" + TestContext.TEST_CLUSTER);
            return false;
          }
          return true;
        });
      });
    }
  }

  @When("^the last added schedule is deleted for cluster called \"([^\"]*)\"$")
  public void the_last_added_schedule_is_deleted_for_cluster_called(String clusterName) throws Throwable {
    synchronized (BasicSteps.class) {
      LOG.info("pause last added repair schedule with id: {}", testContext.getCurrentScheduleId());
      Map<String, String> params = Maps.newHashMap();
      params.put("state", "paused");

      callAndExpect(
          "PUT",
          "/repair_schedule/" + testContext.getCurrentScheduleId(),
          Optional.of(params),
          Optional.of("\"" + clusterName + "\""),
          Response.Status.OK,
          Response.Status.NO_CONTENT);

      LOG.info("delete last added repair schedule with id: {}", testContext.getCurrentScheduleId());
      params.clear();
      params.put("owner", TestContext.TEST_USER);

      callAndExpect("DELETE",
          "/repair_schedule/" + testContext.getCurrentScheduleId(),
          Optional.of(params),
          Optional.empty(),
          Response.Status.ACCEPTED,
          Response.Status.NOT_FOUND);

      await().with().pollInterval(POLL_INTERVAL).atMost(1, MINUTES).until(() -> {
        try {
          callAndExpect(
              "DELETE",
              "/repair_schedule/" + testContext.getCurrentScheduleId(),
              Optional.of(params),
              Optional.empty(),
              Response.Status.NOT_FOUND);
        } catch (AssertionError ex) {
          LOG.warn("DELETE /repair_schedule/" + testContext.getCurrentScheduleId() + " failed: " + ex.getMessage());
          return false;
        }
        return true;
      });
    }
  }

  @When("^the last added schedule is deleted for the last added cluster$")
  public void the_last_added_schedule_is_deleted_for_the_last_added_cluster() throws Throwable {
    synchronized (BasicSteps.class) {
      LOG.info("pause last added repair schedule with id: {}", testContext.getCurrentScheduleId());
      Map<String, String> params = Maps.newHashMap();
      params.put("state", "paused");

      callAndExpect(
          "PUT",
          "/repair_schedule/" + testContext.getCurrentScheduleId(),
          Optional.of(params),
          Optional.of("\"" + TestContext.TEST_CLUSTER + "\""),
          Response.Status.OK,
          Response.Status.NO_CONTENT);

      LOG.info("delete last added repair schedule with id: {}", testContext.getCurrentScheduleId());
      params.clear();
      params.put("owner", TestContext.TEST_USER);

      callAndExpect(
          "DELETE",
          "/repair_schedule/" + testContext.getCurrentScheduleId(),
          Optional.of(params),
          Optional.empty(),
          Response.Status.ACCEPTED,
          Response.Status.NOT_FOUND);

      await().with().pollInterval(POLL_INTERVAL).atMost(1, MINUTES).until(() -> {
        try {
          callAndExpect(
              "DELETE",
              "/repair_schedule/" + testContext.getCurrentScheduleId(),
              Optional.of(params),
              Optional.empty(),
              Response.Status.NOT_FOUND);
        } catch (AssertionError ex) {
          LOG.warn("DELETE /repair_schedule/" + testContext.getCurrentScheduleId() + " failed: " + ex.getMessage());
          return false;
        }
        return true;
      });
    }
  }

  @When("^all added schedules are deleted for the last added cluster$")
  public void all_added_schedules_are_deleted_for_the_last_added_cluster() throws Throwable {
    synchronized (BasicSteps.class) {
      final Set<RepairScheduleStatus> schedules = Sets.newConcurrentHashSet();

      RUNNERS.parallelStream().forEach(runner -> {
        Response response
            = runner.callReaper("GET", "/repair_schedule/cluster/" + TestContext.TEST_CLUSTER, EMPTY_PARAMS);

        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        String responseData = response.readEntity(String.class);
        Assertions.assertThat(responseData).isNotBlank();
        schedules.addAll(SimpleReaperClient.parseRepairScheduleStatusListJSON(responseData));
      });

      schedules.parallelStream().forEach((schedule) -> {
        LOG.info("pause last added repair schedule with id: {}", schedule.getId());
        Map<String, String> params = Maps.newHashMap();
        params.put("state", "paused");

        callAndExpect(
            "PUT",
            "/repair_schedule/" + schedule.getId(),
            Optional.of(params),
            Optional.empty(),
            Response.Status.OK,
            Response.Status.NO_CONTENT,
            Response.Status.NOT_FOUND);
      });

      schedules.stream().forEach((schedule) -> {
        LOG.info("delete last added repair schedule with id: {}", schedule.getId());
        Map<String, String> params = Maps.newHashMap();
        params.put("owner", TestContext.TEST_USER);

        callAndExpect(
            "DELETE",
            "/repair_schedule/" + schedule.getId(),
            Optional.of(params),
            Optional.empty(),
            Response.Status.ACCEPTED,
            Response.Status.NOT_FOUND);

        await().with().pollInterval(POLL_INTERVAL).atMost(1, MINUTES).until(() -> {
          try {
            callAndExpect(
                "DELETE",
                "/repair_schedule/" + schedule.getId(),
                Optional.of(params),
                Optional.empty(),
                Response.Status.NOT_FOUND);
            return true;
          } catch (AssertionError ex) {
            LOG.warn("DELETE /repair_schedule/" + testContext.getCurrentScheduleId() + " failed: " + ex.getMessage());
            return false;
          }
        });
      });
    }
  }

  @And("^deleting cluster called \"([^\"]*)\" fails$")
  public void deleting_cluster_called_fails(String clusterName) throws Throwable {
    synchronized (BasicSteps.class) {
      await().with().pollInterval(POLL_INTERVAL).atMost(1, MINUTES).until(() -> {
        try {
          callAndExpect(
              "DELETE",
              "/cluster/" + TestContext.TEST_CLUSTER,
              EMPTY_PARAMS,
              Optional.empty(),
              Response.Status.CONFLICT);
        } catch (AssertionError ex) {
          LOG.warn("DELETE /cluster/" + TestContext.TEST_CLUSTER + " failed: " + ex.getMessage());
          return false;
        }
        return true;
      });
    }
  }

  @And("^the last added cluster is (force |)deleted$")
  public void cluster_called_is_deleted(String force) throws Throwable {

    Optional<Map<String,String>> params = "force ".equals(force)
        ? Optional.of(Collections.singletonMap("force", "true"))
        : EMPTY_PARAMS;

    synchronized (BasicSteps.class) {
      callAndExpect(
          "DELETE",
          "/cluster/" + TestContext.TEST_CLUSTER,
          params,
          Optional.<String>empty(),
          Response.Status.ACCEPTED,
          Response.Status.NOT_FOUND);

      await().with().pollInterval(POLL_INTERVAL).atMost(1, MINUTES).until(() -> {
        try {
          callAndExpect(
              "GET",
              "/cluster/" + TestContext.TEST_CLUSTER,
              EMPTY_PARAMS,
              Optional.<String>empty(),
              Response.Status.NOT_FOUND);
        } catch (AssertionError ex) {
          LOG.warn("GET /cluster/" + TestContext.TEST_CLUSTER + " failed: " + ex.getMessage());
          return false;
        }
        return true;
      });
    }
  }

  @Then("^reaper has no cluster called \"([^\"]*)\" in storage$")
  public void reaper_has_no_cluster_called_in_storage(String clusterName) throws Throwable {
    synchronized (BasicSteps.class) {
      callAndExpect(
          "GET",
          "/cluster/" + clusterName,
          Optional.<Map<String, String>>empty(),
          Optional.<String>empty(),
          Response.Status.NOT_FOUND);
    }
  }

  @Then("^reaper has no longer the last added cluster in storage$")
  public void reaper_has_no_longer_the_last_added_cluster_in_storage() throws Throwable {
    synchronized (BasicSteps.class) {
      callAndExpect(
          "GET",
          "/cluster/" + TestContext.TEST_CLUSTER,
          Optional.<Map<String, String>>empty(),
          Optional.<String>empty(),
          Response.Status.NOT_FOUND);
    }
  }

  @And("^a new repair(.*) is added for \"([^\"]*)\" and keyspace \"([^\"]*)\"$")
  public void a_new_repair_is_added_for_and_keyspace(String compaction, String clusterName, String keyspace)
      throws Throwable {

    synchronized (BasicSteps.class) {
      Map<String, String> params = Maps.newHashMap();
      params.put("clusterName", clusterName);
      params.put("keyspace", keyspace);
      params.put("owner", TestContext.TEST_USER);
      if ("with compaction".equals(compaction.trim())) {
        params.put("majorCompaction", "true");
      }

      ReaperTestJettyRunner runner = RUNNERS.get(RAND.nextInt(RUNNERS.size()));
      Response response = runner.callReaper("POST", "/repair_run", Optional.of(params));
      assertEquals(Response.Status.CREATED.getStatusCode(), response.getStatus());
      String responseData = response.readEntity(String.class);
      Assertions.assertThat(responseData).isNotBlank();
      RepairRunStatus run = SimpleReaperClient.parseRepairRunStatusJSON(responseData);
      testContext.addCurrentRepairId(run.getId());
    }
  }

  @When(
      "^a new repair is added for the last added cluster "
          + "and keyspace \"([^\"]*)\" with the table \"([^\"]*)\" blacklisted$")
  public void a_new_repair_is_added_for_and_keyspace_with_blacklisted_table(
      String keyspace, String blacklistedTable) throws Throwable {

    synchronized (BasicSteps.class) {
      ReaperTestJettyRunner runner = RUNNERS.get(RAND.nextInt(RUNNERS.size()));
      Map<String, String> params = Maps.newHashMap();
      params.put("clusterName", TestContext.TEST_CLUSTER);
      params.put("keyspace", keyspace);
      params.put("owner", TestContext.TEST_USER);
      params.put("blacklistedTables", blacklistedTable);
      Response response = runner.callReaper("POST", "/repair_run", Optional.of(params));
      assertEquals(Response.Status.CREATED.getStatusCode(), response.getStatus());
      String responseData = response.readEntity(String.class);
      Assertions.assertThat(responseData).isNotBlank();
      RepairRunStatus run = SimpleReaperClient.parseRepairRunStatusJSON(responseData);
      testContext.addCurrentRepairId(run.getId());
    }
  }

  @When(
      "^a new repair is added for the last added cluster "
          + "and keyspace \"([^\"]*)\" for tables \"([^\"]*)\"$")
  public void a_new_repair_is_added_for_and_keyspace_for_tables(String keyspace, String tables) throws Throwable {
    synchronized (BasicSteps.class) {
      ReaperTestJettyRunner runner = RUNNERS.get(RAND.nextInt(RUNNERS.size()));
      Map<String, String> params = Maps.newHashMap();
      params.put("clusterName", TestContext.TEST_CLUSTER);
      params.put("keyspace", keyspace);
      params.put("owner", TestContext.TEST_USER);
      params.put("tables", tables);
      Response response = runner.callReaper("POST", "/repair_run", Optional.of(params));
      assertEquals(Response.Status.CREATED.getStatusCode(), response.getStatus());
      String responseData = response.readEntity(String.class);
      Assertions.assertThat(responseData).isNotBlank();
      RepairRunStatus run = SimpleReaperClient.parseRepairRunStatusJSON(responseData);
      testContext.addCurrentRepairId(run.getId());
    }
  }

  @When("^a new repair is added for the last added cluster and keyspace \"([^\"]*)\"$")
  public void a_new_repair_is_added_for_the_last_added_cluster_and_keyspace(String keyspace) throws Throwable {
    synchronized (BasicSteps.class) {
      ReaperTestJettyRunner runner = RUNNERS.get(RAND.nextInt(RUNNERS.size()));
      Map<String, String> params = Maps.newHashMap();
      params.put("clusterName", TestContext.TEST_CLUSTER);
      params.put("keyspace", keyspace);
      params.put("owner", TestContext.TEST_USER);
      Response response = runner.callReaper("POST", "/repair_run", Optional.of(params));
      assertEquals(Response.Status.CREATED.getStatusCode(), response.getStatus());
      String responseData = response.readEntity(String.class);
      Assertions.assertThat(responseData).isNotBlank();
      RepairRunStatus run = SimpleReaperClient.parseRepairRunStatusJSON(responseData);
      testContext.addCurrentRepairId(run.getId());
    }
  }

  @And("^the last added repair has table \"([^\"]*)\" in the blacklist$")
  public void the_last_added_repair_has_table_in_the_blacklist(String blacklistedTable) throws Throwable {
    synchronized (BasicSteps.class) {
      RUNNERS.parallelStream().forEach(runner -> {
        Response response = runner.callReaper("GET", "/repair_run/cluster/" + TestContext.TEST_CLUSTER, EMPTY_PARAMS);
        String responseData = response.readEntity(String.class);
        assertEquals(responseData, Response.Status.OK.getStatusCode(), response.getStatus());
        Assertions.assertThat(responseData).isNotBlank();
        List<RepairRunStatus> runs = SimpleReaperClient.parseRepairRunStatusListJSON(responseData);
        assertTrue(runs.get(0).getBlacklistedTables().contains(blacklistedTable));
      });
    }
  }

  @And("^the last added repair has twcs table \"([^\"]*)\" in the blacklist$")
  public void the_last_added_repair_has_twcs_table_in_the_blacklist(String twcsTable) throws Throwable {
    synchronized (BasicSteps.class) {
      final VersionNumber lowestNodeVersion = getCassandraVersion();

      RUNNERS.parallelStream().forEach(runner -> {
        Response response = runner.callReaper("GET", "/repair_run/cluster/" + TestContext.TEST_CLUSTER, EMPTY_PARAMS);
        String responseData = response.readEntity(String.class);
        assertEquals(responseData, Response.Status.OK.getStatusCode(), response.getStatus());
        Assertions.assertThat(responseData).isNotBlank();
        List<RepairRunStatus> runs = SimpleReaperClient.parseRepairRunStatusListJSON(responseData);
        if ((reaperVersion.isPresent()
            && 0 < VersionNumber.parse("1.4.0").compareTo(VersionNumber.parse(reaperVersion.get())))
            // while DTCS is available in 2.0.11 it is not visible over jmx until 2.1
            //  see `Table.DEFAULT_COMPACTION_STRATEGY`
            || VersionNumber.parse("2.1").compareTo(lowestNodeVersion) > 0) {

          Assertions
              .assertThat(runs.get(0).getColumnFamilies().contains(twcsTable))
              .isTrue();
        } else {
          // auto TWCS blacklisting was only added in Reaper 1.4.0, and requires Cassandra >= 2.1
          Assertions
              .assertThat(runs.get(0).getColumnFamilies().contains(twcsTable))
              .isFalse();
        }
      });
    }
  }

  @When("^a new incremental repair is added for \"([^\"]*)\" and keyspace \"([^\"]*)\"$")
  public void a_new_incremental_repair_is_added_for_and_keyspace(String clusterName, String keyspace) throws Throwable {
    synchronized (BasicSteps.class) {
      ReaperTestJettyRunner runner = RUNNERS.get(RAND.nextInt(RUNNERS.size()));
      Map<String, String> params = Maps.newHashMap();
      params.put("clusterName", clusterName);
      params.put("keyspace", keyspace);
      params.put("owner", TestContext.TEST_USER);
      params.put("incrementalRepair", Boolean.TRUE.toString());
      Response response = runner.callReaper("POST", "/repair_run", Optional.of(params));
      String responseData = response.readEntity(String.class);
      assertEquals(responseData, Response.Status.CREATED.getStatusCode(), response.getStatus());
      Assertions.assertThat(responseData).isNotBlank();
      RepairRunStatus run = SimpleReaperClient.parseRepairRunStatusJSON(responseData);
      testContext.addCurrentRepairId(run.getId());
    }
  }

  @When("^a new incremental repair is added for the last added cluster and keyspace \"([^\"]*)\"$")
  public void a_new_incremental_repair_is_added_for_the_last_added_cluster_and_keyspace(String keyspace)
      throws Throwable {
    synchronized (BasicSteps.class) {
      ReaperTestJettyRunner runner = RUNNERS.get(RAND.nextInt(RUNNERS.size()));
      Map<String, String> params = Maps.newHashMap();
      params.put("clusterName", TestContext.TEST_CLUSTER);
      params.put("keyspace", keyspace);
      params.put("owner", TestContext.TEST_USER);
      params.put("incrementalRepair", Boolean.TRUE.toString());
      Response response = runner.callReaper("POST", "/repair_run", Optional.of(params));
      String responseData = response.readEntity(String.class);
      assertEquals(responseData, Response.Status.CREATED.getStatusCode(), response.getStatus());
      Assertions.assertThat(responseData).isNotBlank();
      RepairRunStatus run = SimpleReaperClient.parseRepairRunStatusJSON(responseData);
      testContext.addCurrentRepairId(run.getId());
    }
  }

  @Then("^reaper has (\\d+) repairs for cluster called \"([^\"]*)\"$")
  public void reaper_has_repairs_for_cluster_called(int expected, String clusterName) throws Throwable {
    synchronized (BasicSteps.class) {
      RUNNERS.parallelStream().forEach(runner -> {
        Response response = runner.callReaper("GET", "/repair_run/cluster/" + clusterName, EMPTY_PARAMS);
        String responseData = response.readEntity(String.class);
        assertEquals(responseData, Response.Status.OK.getStatusCode(), response.getStatus());
        Assertions.assertThat(responseData).isNotBlank();
        List<RepairRunStatus> runs = SimpleReaperClient.parseRepairRunStatusListJSON(responseData);

        // a repair can be created multiple times by different reaper processes (duplicates are dealt with in time)
        assertTrue(
            String.format("Expected at least %s repairs. Found %s", expected, runs.size()),
            expected <= runs.size());
      });
    }
  }

  @Then("^reaper has (\\d+) started or done repairs for cluster called \"([^\"]*)\"$")
  public void reaper_has_running_repairs_for_cluster_called(int expected, String clusterName) throws Throwable {

    Set<RepairRun.RunState> startedStates = EnumSet.copyOf(
        Sets.newHashSet(RepairRun.RunState.RUNNING, RepairRun.RunState.DONE));

    synchronized (BasicSteps.class) {
      RUNNERS.parallelStream().forEach(runner -> {
        Response response = runner.callReaper("GET", "/repair_run/cluster/" + clusterName, EMPTY_PARAMS);
        String responseData = response.readEntity(String.class);
        assertEquals(responseData, Response.Status.OK.getStatusCode(), response.getStatus());
        Assertions.assertThat(responseData).isNotBlank();
        List<RepairRunStatus> runs = SimpleReaperClient.parseRepairRunStatusListJSON(responseData);
        long found = runs.stream().filter(rrs -> startedStates.contains(rrs.getState())).count();

        // a repair can be started multiple times by different reaper processes (duplicates are dealt with in time)
        assertTrue(
            String.format("Expected at least %s running or done repair runs. Found %s", expected, found),
            expected <= found);
      });
    }
  }

  @Then("^reaper has (\\d+) repairs for the last added cluster$")
  public void reaper_has_repairs_for_the_last_added_cluster(int expected) throws Throwable {
    synchronized (BasicSteps.class) {
      RUNNERS.parallelStream().forEach(runner -> {
        Response response = runner.callReaper("GET", "/repair_run/cluster/" + TestContext.TEST_CLUSTER, EMPTY_PARAMS);
        String responseData = response.readEntity(String.class);
        assertEquals(responseData, Response.Status.OK.getStatusCode(), response.getStatus());
        Assertions.assertThat(responseData).isNotBlank();
        List<RepairRunStatus> runs = SimpleReaperClient.parseRepairRunStatusListJSON(responseData);

        // a repair can be created multiple times by different reaper processes (duplicates are dealt with in time)
        assertTrue(
            String.format("Expected at least %s repairs. Found %s", expected, runs.size()),
            expected <= runs.size());
      });
    }
  }

  @Then("^reaper has (\\d+) started or done repairs for the last added cluster$")
  public void reaper_has_started_repairs_for_the_last_added_cluster(int expected) throws Throwable {

    Set<RepairRun.RunState> startedStates = EnumSet.copyOf(
        Sets.newHashSet(RepairRun.RunState.RUNNING, RepairRun.RunState.DONE));

    synchronized (BasicSteps.class) {
      RUNNERS.parallelStream().forEach(runner -> {
        Response response = runner.callReaper("GET", "/repair_run/cluster/" + TestContext.TEST_CLUSTER, EMPTY_PARAMS);
        String responseData = response.readEntity(String.class);
        assertEquals(responseData, Response.Status.OK.getStatusCode(), response.getStatus());
        Assertions.assertThat(responseData).isNotBlank();
        List<RepairRunStatus> runs = SimpleReaperClient.parseRepairRunStatusListJSON(responseData);
        long found = runs.stream().filter(rrs -> startedStates.contains(rrs.getState())).count();

        // a repair can be started multiple times by different reaper processes (duplicates are dealt with in time)
        assertTrue(
            String.format("Expected at least %s running or done repair runs. Found %s", expected, found),
            expected <= found);
      });
    }
  }

  @When("^all added repair runs are deleted for the last added cluster$")
  public void all_added_repair_runs_are_deleted_for_the_last_added_cluster() throws Throwable {
    synchronized (BasicSteps.class) {
      final Set<RepairRunStatus> runs = Sets.newConcurrentHashSet();

      RUNNERS.parallelStream().forEach(runner -> {
        Response response
            = runner.callReaper("GET", "/repair_run/cluster/" + TestContext.TEST_CLUSTER, EMPTY_PARAMS);

        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        String responseData = response.readEntity(String.class);
        Assertions.assertThat(responseData).isNotBlank();
        runs.addAll(SimpleReaperClient.parseRepairRunStatusListJSON(responseData));
      });

      runs.stream().forEach((run) -> {
        UUID id = run.getId();
        LOG.info("stopping repair run with id: {}", id);
        stopRepairRun(id);
      });

      Map<String, String> params = Maps.newHashMap();
      params.put("owner", TestContext.TEST_USER);

      runs.stream().forEach((run) -> {
        UUID id = run.getId();
        LOG.info("deleting repair run with id: {}", id);

        callAndExpect(
            "DELETE",
            "/repair_run/" + id,
            Optional.of(params),
            Optional.empty(),
            Response.Status.ACCEPTED,
            Response.Status.NOT_FOUND,
            Response.Status.CONFLICT);

        await().with().pollInterval(POLL_INTERVAL).atMost(1, MINUTES).until(() -> {
          try {
            callAndExpect(
                "DELETE",
                "/repair_run/" + id,
                Optional.of(params),
                Optional.empty(),
                Response.Status.NOT_FOUND);
          } catch (AssertionError ex) {
            LOG.warn("DELETE /repair_run/" + testContext.getCurrentRepairId() + " failed: " + ex.getMessage());
            return false;
          }
          return true;
        });
      });
    }
  }

  @When("^a new daily \"([^\"]*)\" repair schedule is added "
      + "that already exists for \"([^\"]*)\" and keyspace \"([^\"]*)\"$")
  public void a_new_daily_repair_schedule_is_added_that_already_exists_for(
      String repairType,
      String clusterName,
      String keyspace) throws Throwable {

    synchronized (BasicSteps.class) {
      RUNNERS.parallelStream().forEach(runner -> {
        Map<String, String> params = Maps.newHashMap();
        params.put("clusterName", clusterName);
        params.put("keyspace", keyspace);
        params.put("owner", TestContext.TEST_USER);
        params.put("intensity", "0.9");
        params.put("scheduleDaysBetween", "1");
        params.put("repairParallelism", repairType.equals("incremental") ? "parallel" : "sequential");
        params.put("incrementalRepair", repairType.equals("incremental") ? "True" : "False");
        Response response = runner.callReaper("POST", "/repair_schedule", Optional.of(params));

        int status = response.getStatus();
        String responseEntity = response.readEntity(String.class);

        Assertions.assertThat(
              ImmutableList.of(Response.Status.NO_CONTENT.getStatusCode(), Response.Status.CONFLICT.getStatusCode()))
            .withFailMessage(responseEntity)
            .contains(status);
      });
    }
  }

  @And("^the last added repair is activated$")
  public void the_last_added_repair_is_activated_for() throws Throwable {
    synchronized (BasicSteps.class) {
      final AtomicBoolean set = new AtomicBoolean(false);
      RUNNERS.parallelStream().forEach(runner -> {

        Response response = runner.callReaper(
            "PUT",
            "/repair_run/" + testContext.getCurrentRepairId() + "/state/RUNNING",
            Optional.of(Maps.newHashMap()));

        int status = response.getStatus();
        String responseEntity = response.readEntity(String.class);

        Assertions.assertThat(
              ImmutableList.of(Response.Status.OK.getStatusCode(), Response.Status.NO_CONTENT.getStatusCode()))
            .withFailMessage(responseEntity)
            .contains(status);

        // rest command requests should not response with bodies, follow the location to GET that
        Assertions.assertThat(responseEntity).isEmpty();

        // follow to new location (to GET resource)
        response = runner.callReaper("GET", response.getLocation().toString(), Optional.empty());
        String responseData = response.readEntity(String.class);

        if (Response.Status.OK.getStatusCode() == status) {
          Assertions.assertThat(responseData).isNotBlank();
          RepairRunStatus run = SimpleReaperClient.parseRepairRunStatusJSON(responseData);
          testContext.addCurrentRepairId(run.getId());
          set.compareAndSet(false, true);
        }
      });
      Assertions.assertThat(set.get()).isTrue();

      callAndExpect(
          "PUT",
          "/repair_run/" + testContext.getCurrentRepairId() + "/state/RUNNING",
          Optional.empty(),
          Optional.empty(),
          Response.Status.NO_CONTENT);
    }
  }

  @When("^the last added repair is stopped$")
  public void the_last_added_repair_is_stopped_for() throws Throwable {
    synchronized (BasicSteps.class) {
      stopRepairRun(testContext.getCurrentRepairId());
    }
  }

  private void stopRepairRun(UUID repairRunId) {
    // given "state" is same as the current run state
    RUNNERS.parallelStream().forEach(runner -> {
      Response response = runner.callReaper("PUT", "/repair_run/" + repairRunId + "/state/PAUSED", EMPTY_PARAMS);
      int status = response.getStatus();
      String responseEntity = response.readEntity(String.class);

      Assertions.assertThat(
            ImmutableList.of(
                Response.Status.OK.getStatusCode(),
                Response.Status.NO_CONTENT.getStatusCode(),
                Response.Status.NOT_FOUND.getStatusCode(),
                Response.Status.CONFLICT.getStatusCode()))
          .withFailMessage(responseEntity)
          .contains(status);

      // non-error rest command requests should not response with bodies
      if (Response.Status.CONFLICT.getStatusCode() != status) {
        Assertions.assertThat(responseEntity).isEmpty();
      }
    });

    callAndExpect(
        "PUT",
        "/repair_run/" + repairRunId + "/state/PAUSED",
        EMPTY_PARAMS,
        Optional.empty(),
        Response.Status.NO_CONTENT,
        Response.Status.NOT_FOUND,
        Response.Status.CONFLICT);
  }

  @And("^we wait for at least (\\d+) segments to be repaired$")
  public void we_wait_for_at_least_segments_to_be_repaired(int nbSegmentsToBeRepaired) throws Throwable {
    synchronized (BasicSteps.class) {
      RUNNERS.parallelStream().forEach(runner -> {
        final AtomicReference<RepairRunStatus> run = new AtomicReference<>();
        try {
          await().with().pollInterval(POLL_INTERVAL.multiply(2)).atMost(5, MINUTES).until(() -> {
            try {
              Response response = runner
                  .callReaper("GET", "/repair_run/" + testContext.getCurrentRepairId(), EMPTY_PARAMS);

              String responseData = response.readEntity(String.class);

              Assertions
                  .assertThat(response.getStatus())
                  .withFailMessage(responseData)
                  .isEqualTo(Response.Status.OK.getStatusCode());

              Assertions.assertThat(responseData).isNotBlank();
              run.set(SimpleReaperClient.parseRepairRunStatusJSON(responseData));
              return nbSegmentsToBeRepaired <= run.get().getSegmentsRepaired();
            } catch (AssertionError ex) {
              LOG.error("GET /repair_run/" + testContext.getCurrentRepairId() + " failed: " + ex.getMessage());
              if (null != run.get()) {
                LOG.error("last event was: " + run.get().getLastEvent());
              }
              logResponse(runner, "/repair_run/cluster/" + TestContext.TEST_CLUSTER);
              return false;
            }
          });
        } catch (ConditionTimeoutException ex) {
          logResponse(runner, "/repair_run/cluster/" + TestContext.TEST_CLUSTER);
          throw ex;
        }
      });
    }
  }

  private static void logResponse(ReaperTestJettyRunner runner, String path) {
    Response resp = runner.callReaper("GET", path, EMPTY_PARAMS);
    if (Response.Status.OK.getStatusCode() == resp.getStatus()) {
      LOG.error("GET " + path + " returned:\n" + resp.readEntity(String.class));
    } else {
      LOG.error("GET " + path + TestContext.TEST_CLUSTER + " failed: " + resp.readEntity(String.class));
    }
  }

  @Then("^reseting one segment sets its state to not started$")
  public void reseting_one_segment_sets_its_state_to_not_started() throws Throwable {
    synchronized (BasicSteps.class) {
      RUNNERS.parallelStream().forEach(runner -> {
        await().with().pollInterval(1, SECONDS).atMost(2, MINUTES).until(
            () -> {
              Response response = runner.callReaper(
                      "GET",
                      "/repair_run/" + testContext.getCurrentRepairId() + "/segments",
                      EMPTY_PARAMS);

              String responseData = response.readEntity(String.class);
              if (Response.Status.OK.getStatusCode() == response.getStatus() && StringUtils.isNotBlank(responseData)) {
                List<RepairSegment> segments = SimpleReaperClient.parseRepairSegmentsJSON(responseData);

                boolean gotDoneSegments
                    = segments.stream().anyMatch(seg -> seg.getState() == RepairSegment.State.DONE);

                if (gotDoneSegments) {
                  TestContext.FINISHED_SEGMENT = segments
                          .stream()
                          .filter(seg -> seg.getState() == RepairSegment.State.DONE)
                          .map(segment -> segment.getId())
                          .findFirst()
                          .get();
                }
                return gotDoneSegments;
              }
              return false;
            });
      });

      RUNNERS.parallelStream().forEach(runner -> {
        await().with().pollInterval(POLL_INTERVAL).atMost(2, MINUTES).until(
            () -> {
              Response abort = runner.callReaper(
                      "POST",
                      "/repair_run/"
                          + testContext.getCurrentRepairId()
                          + "/segments/abort/"
                          + TestContext.FINISHED_SEGMENT,
                      EMPTY_PARAMS);

              Response response = runner.callReaper(
                      "GET",
                       "/repair_run/" + testContext.getCurrentRepairId() + "/segments",
                      EMPTY_PARAMS);

              String responseData = response.readEntity(String.class);
              if (Response.Status.OK.getStatusCode() == response.getStatus() && StringUtils.isNotBlank(responseData)) {
                List<RepairSegment> segments = SimpleReaperClient.parseRepairSegmentsJSON(responseData);

                return segments.stream().anyMatch(seg ->
                        seg.getId().equals(TestContext.FINISHED_SEGMENT)
                            && seg.getState() == RepairSegment.State.NOT_STARTED);
              }
              return false;
            });
      });
    }
  }

  @And("^the last added cluster has a keyspace called reaper_db$")
  public void the_last_added_cluster_has_a_keyspace_called_reaper_db() throws Throwable {
    synchronized (BasicSteps.class) {
      RUNNERS.parallelStream().forEach(runner -> {
        Response response = runner.callReaper("GET", "/cluster/" + TestContext.TEST_CLUSTER + "/tables", EMPTY_PARAMS);
        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        String responseData = response.readEntity(String.class);
        Assertions.assertThat(responseData).isNotBlank();
        Map<String, List<String>> tablesByKeyspace = SimpleReaperClient.parseTableListJSON(responseData);
        assertTrue(tablesByKeyspace.containsKey("reaper_db"));
        assertTrue(tablesByKeyspace.get("reaper_db").contains("repair_run"));
      });
    }
  }

  @When("^a cluster wide snapshot request is made to Reaper$")
  public void a_cluster_wide_snapshot_request_is_made_to_reaper() throws Throwable {
    synchronized (BasicSteps.class) {
      ReaperTestJettyRunner runner = RUNNERS.get(0);
      Response response = runner.callReaper("POST", "/snapshot/cluster/" + TestContext.TEST_CLUSTER, EMPTY_PARAMS);
      assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    }
  }

  @Then("^there is (\\d+) snapshot returned when listing snapshots$")
  public void there_is_1_snapshot_returned_when_listing_snapshots(int nbSnapshots) throws Throwable {
    synchronized (BasicSteps.class) {
      ReaperTestJettyRunner runner = RUNNERS.get(0);

      Response response = runner.callReaper(
              "GET",
              "/snapshot/" + TestContext.TEST_CLUSTER + "/" + TestContext.SEED_HOST.split("@")[0],
              EMPTY_PARAMS);

      assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
      String responseData = response.readEntity(String.class);
      Assertions.assertThat(responseData).isNotBlank();
      Map<String, List<Snapshot>> snapshots = SimpleReaperClient.parseSnapshotMapJSON(responseData);
      assertEquals(nbSnapshots, snapshots.keySet().size());
    }
  }

  @When("^a request is made to clear the existing snapshot cluster wide$")
  public void a_request_is_made_to_clear_the_existing_snapshots_cluster_wide() throws Throwable {
    synchronized (BasicSteps.class) {
      ReaperTestJettyRunner runner = RUNNERS.get(0);

      Response response = runner.callReaper(
              "GET",
              "/snapshot/" + TestContext.TEST_CLUSTER + "/" + TestContext.SEED_HOST.split("@")[0],
              EMPTY_PARAMS);

      assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
      String responseData = response.readEntity(String.class);
      Assertions.assertThat(responseData).isNotBlank();
      Map<String, List<Snapshot>> snapshots = SimpleReaperClient.parseSnapshotMapJSON(responseData);

      snapshots.keySet().stream().forEach(snapshot -> {
        callAndExpect(
            "DELETE",
            "/snapshot/cluster/" + TestContext.TEST_CLUSTER + "/" + snapshot,
            Optional.empty(),
            Optional.empty(),
            Response.Status.ACCEPTED,
            Response.Status.NOT_FOUND);
      });
    }
  }

  @When("^a snapshot request for the seed host is made to Reaper$")
  public void a_host_snapshot_request_is_made_to_reaper() throws Throwable {
    synchronized (BasicSteps.class) {
      ReaperTestJettyRunner runner = RUNNERS.get(0);

      Response response = runner.callReaper(
              "POST",
              "/snapshot/" + TestContext.TEST_CLUSTER + "/" + TestContext.SEED_HOST.split("@")[0],
              EMPTY_PARAMS);

      assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    }
  }

  @When("^a request is made to clear the seed host existing snapshots$")
  public void a_request_is_made_to_clear_the_existing_host_snapshots() throws Throwable {
    synchronized (BasicSteps.class) {
      ReaperTestJettyRunner runner = RUNNERS.get(0);

      Response response = runner.callReaper(
              "GET",
              "/snapshot/" + TestContext.TEST_CLUSTER + "/" + TestContext.SEED_HOST.split("@")[0],
              EMPTY_PARAMS);

      assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
      String responseData = response.readEntity(String.class);
      Assertions.assertThat(responseData).isNotBlank();
      Map<String, List<Snapshot>> snapshots = SimpleReaperClient.parseSnapshotMapJSON(responseData);

      snapshots.keySet().stream().forEach(snapshot -> {
        callAndExpect(
            "DELETE",
            "/snapshot/" + TestContext.TEST_CLUSTER
                + "/" + TestContext.SEED_HOST.split("@")[0] + "/" + snapshot,
            Optional.empty(),
            Optional.empty(),
            Response.Status.ACCEPTED,
            Response.Status.NOT_FOUND);
      });
    }
  }

  @When("^a (GET|POST|PUT|DELETE) ([^\"]*) is made$")
  public void aRequestIsMade(String method, String requestPath) throws Throwable {
    RUNNERS.parallelStream().forEach(runner -> lastResponse = runner.callReaper(method, requestPath, EMPTY_PARAMS));
  }

  @Then("^a \"([^\"]*)\" response is returned$")
  public void aResponseIsReturned(String statusDescription) throws Throwable {
    Assertions.assertThat(lastResponse.getStatus()).isEqualTo(httpStatus(statusDescription));
  }

  @Then("^the response was redirected to the login page$")
  public void theResponseWasRedirectedToTheLoginPage() throws Throwable {
    assertTrue(lastResponse.hasEntity());
    assertTrue(lastResponse.readEntity(String.class).contains("<title>Not a real login page</title>"));
  }

  @And("^we can collect the tpstats from a seed node$")
  public void we_can_collect_the_tpstats_from_the_seed_node() throws Throwable {
    synchronized (BasicSteps.class) {
      // XXX – this assertion does not work in upgrade tests. unknown reason.
      if (!reaperVersion.isPresent()) {
        // any collected tpstats via any reaper satifies this test
        AtomicBoolean collected = new AtomicBoolean(false);
        List<String> seeds = ImmutableList.copyOf(TestContext.TEST_CLUSTER_SEED_HOSTS.keySet());

        RUNNERS.parallelStream().forEach(runner -> {
          await().with().pollInterval(POLL_INTERVAL).atMost(2, MINUTES).until(() -> {
            String seed = seeds.get(RAND.nextInt(seeds.size()));

            Response response = runner.callReaper(
                    "GET",
                    "/node/tpstats/" + TestContext.TEST_CLUSTER + "/" + seed,
                    EMPTY_PARAMS);

            String responseData = response.readEntity(String.class);
            if (Response.Status.OK.getStatusCode() == response.getStatus() && StringUtils.isNotBlank(responseData)) {
              List<ThreadPoolStat> tpstats = SimpleReaperClient.parseTpStatJSON(responseData);
              if (tpstats.isEmpty()) {
                LOG.error("Got empty response from {}", seed);
              }
              long readStageTotal = tpstats.stream().filter(tpstat -> tpstat.getName().equals("ReadStage")).count();

              long readStageCompleted = tpstats.stream()
                  .filter(tpstat -> tpstat.getName().equals("ReadStage"))
                  .filter(tpstat -> tpstat.getCurrentlyBlockedTasks() == 0)
                  .filter(tpstat -> tpstat.getCompletedTasks() > 0)
                  .count();

              collected.compareAndSet(false, 1 == readStageTotal && 1 == readStageCompleted);
            }
            return collected.get();
          });
        });
      }
    }
  }

  @And("^we can collect the dropped messages stats from a seed node$")
  public void we_can_collect_the_dropped_messages_stats_from_the_seed_node() throws Throwable {
    synchronized (BasicSteps.class) {
      // XXX – this assertion does not work in upgrade tests. unknown reason.
      if (!reaperVersion.isPresent()) {
        // any collected dropped messages via any reaper satifies this test
        AtomicBoolean collected = new AtomicBoolean(false);
        List<String> seeds = ImmutableList.copyOf(TestContext.TEST_CLUSTER_SEED_HOSTS.keySet());

        RUNNERS.parallelStream().forEach(runner -> {
          await().with().pollInterval(POLL_INTERVAL).atMost(2, MINUTES).until(() -> {
            String seed = seeds.get(RAND.nextInt(seeds.size()));

            Response response = runner.callReaper(
                    "GET",
                    "/node/dropped/" + TestContext.TEST_CLUSTER + "/" + seed,
                    EMPTY_PARAMS);

            String responseData = response.readEntity(String.class);
            if (Response.Status.OK.getStatusCode() == response.getStatus() && StringUtils.isNotBlank(responseData)) {
              List<DroppedMessages> dropped = SimpleReaperClient.parseDroppedMessagesJSON(responseData);
              if (dropped.isEmpty()) {
                LOG.error("Got empty response from {}", seed);
              }

              long readDroppedTotal = dropped.stream()
                  .filter(drop -> "READ".equals(drop.getName()))
                  .count();

              long readDroppedCount = dropped.stream()
                  .filter(drop -> "READ".equals(drop.getName()))
                  .filter(drop -> drop.getCount() >= 0)
                  .count();

              collected.compareAndSet(false, 1 == readDroppedTotal && 1 == readDroppedCount);
            }
            return collected.get();
          });
        });
      }
    }
  }

  @And("^we can collect the client request metrics from a seed node$")
  public void we_can_collect_the_client_request_metrics_from_the_seed_node() throws Throwable {
    synchronized (BasicSteps.class) {
      // XXX – this assertion does not work in upgrade tests. unknown reason.
      if (!reaperVersion.isPresent()) {
        final AtomicBoolean collected = new AtomicBoolean(false);
        List<String> seeds = ImmutableList.copyOf(TestContext.TEST_CLUSTER_SEED_HOSTS.keySet());

        RUNNERS.parallelStream().forEach(runner -> {
          await().with().pollInterval(POLL_INTERVAL).atMost(2, MINUTES).until(() -> {
            String seed = seeds.get(RAND.nextInt(seeds.size()));

            Response response = runner.callReaper(
                "GET",
                "/node/clientRequestLatencies/" + TestContext.TEST_CLUSTER + "/" + seed,
                EMPTY_PARAMS);

            String responseData = response.readEntity(String.class);
            if (Response.Status.OK.getStatusCode() == response.getStatus() && StringUtils.isNotBlank(responseData)) {
              List<MetricsHistogram> metrics = SimpleReaperClient.parseClientRequestMetricsJSON(responseData);
              if (metrics.isEmpty()) {
                LOG.error("Got empty response from {}", seed);
              }
              long writeMetrics = metrics.stream().filter(metric -> metric.getName().startsWith("Write")).count();
              collected.compareAndSet(false, 1 <= writeMetrics);
            }
            return collected.get();
          });
        });
      }
    }
  }

  @And("^the seed node has vnodes$")
  public void the_seed_node_has_vnodes() {
    synchronized (BasicSteps.class) {
      RUNNERS.parallelStream()
          .forEach(
              runner -> {
                Response response = runner.callReaper(
                        "GET",
                        "/node/tokens/" + TestContext.TEST_CLUSTER + "/" + TestContext.SEED_HOST.split("@")[0],
                        EMPTY_PARAMS);

                assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
                String responseData = response.readEntity(String.class);
                Assertions.assertThat(responseData).isNotBlank();
                List<String> tokens = SimpleReaperClient.parseTokenListJSON(responseData);
                assertTrue(tokens.size() >= 1);
              });
    }
  }

  private static int httpStatus(String statusCodeDescriptions) {
    String enumName = statusCodeDescriptions.toUpperCase().replace(' ', '_');
    return Response.Status.valueOf(enumName).getStatusCode();
  }

  private static void createKeyspace(String keyspaceName) {
    try (Cluster cluster = buildCluster(); Session tmpSession = cluster.connect()) {
      VersionNumber lowestNodeVersion = getCassandraVersion(tmpSession);

      try {
        if (null == tmpSession.getCluster().getMetadata().getKeyspace(keyspaceName)) {
          tmpSession.execute(
              "CREATE KEYSPACE "
                  + (VersionNumber.parse("2.0").compareTo(lowestNodeVersion) <= 0 ? "IF NOT EXISTS " : "")
                  + keyspaceName
                + " WITH replication = {" + buildNetworkTopologyStrategyString(cluster) + "}");
        }
      } catch (AlreadyExistsException ignore) { }
    }
  }

  static String buildNetworkTopologyStrategyString(Cluster cluster) {
    Map<String, Integer> ntsMap = Maps.newHashMap();
    for (Host host : cluster.getMetadata().getAllHosts()) {
      String dc = host.getDatacenter();
      ntsMap.put(dc, 1 + ntsMap.getOrDefault(dc, 0));
    }
    StringBuilder builder = new StringBuilder("'class':'NetworkTopologyStrategy',");
    for (Map.Entry<String, Integer> e : ntsMap.entrySet()) {
      builder.append("'").append(e.getKey()).append("':").append(e.getValue()).append(",");
    }
    return builder.substring(0, builder.length() - 1);
  }

  private static Cluster buildCluster() {
    return Cluster.builder()
        .addContactPoint("127.0.0.1")
        .withSocketOptions(new SocketOptions().setConnectTimeoutMillis(20000).setReadTimeoutMillis(40000))
        .withoutJMXReporting()
        .build();
  }

  private static VersionNumber getCassandraVersion() {
    try (Cluster cluster = buildCluster(); Session tmpSession = cluster.connect()) {
      return getCassandraVersion(tmpSession);
    }
  }

  private static VersionNumber getCassandraVersion(Session tmpSession) {

    return tmpSession
            .getCluster()
            .getMetadata()
            .getAllHosts()
            .stream()
            .map(host -> host.getCassandraVersion())
            .min(VersionNumber::compareTo)
            .get();
  }

  private static void createTable(String keyspaceName, String tableName) {
    try (Cluster cluster = buildCluster(); Session tmpSession = cluster.connect()) {
      VersionNumber lowestNodeVersion = getCassandraVersion(tmpSession);

      String createTableStmt
          = "CREATE TABLE "
              + (VersionNumber.parse("2.0").compareTo(lowestNodeVersion) <= 0 ? "IF NOT EXISTS " : "")
              + keyspaceName
              + "."
              + tableName
              + "(id int PRIMARY KEY, value text)";

      if (tableName.endsWith("twcs")) {
        if (((VersionNumber.parse("3.0.8").compareTo(lowestNodeVersion) <= 0
            && VersionNumber.parse("3.0.99").compareTo(lowestNodeVersion) >= 0)
            || VersionNumber.parse("3.8").compareTo(lowestNodeVersion) <= 0)) {
          // TWCS is available by default
          createTableStmt
              += " WITH compaction = {'class':'TimeWindowCompactionStrategy',"
                  + "'compaction_window_size': '1', "
                  + "'compaction_window_unit': 'MINUTES'}";
        } else if (VersionNumber.parse("2.0.11").compareTo(lowestNodeVersion) <= 0) {
          createTableStmt += " WITH compaction = {'class':'DateTieredCompactionStrategy'}";
        }
      }

      try {
        if (null == tmpSession.getCluster().getMetadata().getKeyspace(keyspaceName).getTable(tableName)) {
          tmpSession.execute(createTableStmt);
        }
      } catch (AlreadyExistsException ignore) { }

      for (int i = 0; i < 100; i++) {
        tmpSession.execute(
            "INSERT INTO " + keyspaceName + "." + tableName + "(id, value) VALUES(" + i + ",'" + i + "')");
      }
    }
  }

  @When("^a get all subscriptions request is made$")
  public void aGetAllSubscriptionsRequestIsMade() throws Throwable {
    synchronized (BasicSteps.class) {
      testContext.updateRetrievedEventSubscriptions(
          callSubscription("GET", Optional.empty(), Optional.empty(), Response.Status.OK));
    }
  }

  @When("^a get-subscriptions request is made for cluster \"([^\"]*)\"$")
  public void aGetSubscriptionsRequestIsMadeForCluster(String clusterName) throws Throwable {
    synchronized (BasicSteps.class) {
      HashMap<String, String> params = new HashMap<>();
      params.put("clusterName", clusterName);

      testContext.updateRetrievedEventSubscriptions(
          callSubscription("GET", Optional.of(params), Optional.empty(), Response.Status.OK));
    }
  }

  @When("^a get-subscription request is made for the last inserted ID$")
  public void aGetSubscriptionRequestIsMadeForTheLastInsertedID() throws Throwable {
    synchronized (BasicSteps.class) {
      DiagEventSubscription last = testContext.getCurrentEventSubscription();

      testContext.updateRetrievedEventSubscriptions(
          callSubscription(
              "GET",
              Optional.empty(),
              Optional.ofNullable(last.getId().orElse(null)),
              Response.Status.OK));
    }
  }

  @When("^the last created subscription is deleted$")
  public void theLastCreatedSubscriptionIsDeleted() throws Throwable {
    synchronized (BasicSteps.class) {
      DiagEventSubscription last = testContext.removeCurrentEventSubscription();

      callAndExpect(
          "DELETE",
          "/diag_event/subscription/" + last.getId().get(),
          Optional.empty(),
          Optional.empty(),
          Response.Status.ACCEPTED,
          Response.Status.NOT_FOUND);

      await().with().pollInterval(POLL_INTERVAL).atMost(1, MINUTES).until(() -> {
        try {
          callAndExpect(
              "DELETE",
              "/diag_event/subscription/" + last.getId().get(),
              Optional.empty(),
              Optional.empty(),
              Response.Status.NOT_FOUND);
        } catch (AssertionError ex) {
          LOG.warn("DELETE /diag_event/subscription/" + last.getId().get() + " failed: " + ex.getMessage());
          return false;
        }
        return true;
      });
    }
  }

  @When("^all created subscriptions are deleted$")
  public void allCreatedSubscriptionsAreDeleted() throws Throwable {
    synchronized (BasicSteps.class) {
      aGetAllSubscriptionsRequestIsMade();
      testContext.getRetrievedEventSubscriptions()
          .parallelStream()
          .forEach((sub) -> {
            callAndExpect(
                "DELETE",
                "/diag_event/subscription/" + sub.getId().get(),
                Optional.empty(),
                Optional.empty(),
                Response.Status.ACCEPTED,
                Response.Status.NOT_FOUND);
            try {
              await().with().pollInterval(POLL_INTERVAL).atMost(1, MINUTES).until(() -> {
                try {
                  callAndExpect(
                      "DELETE",
                      "/diag_event/subscription/" + sub.getId().get(),
                      Optional.empty(),
                      Optional.empty(),
                      Response.Status.NOT_FOUND);
                } catch (AssertionError ex) {
                  LOG.warn("DELETE /diag_event/subscription/" + sub.getId().get() + " failed: " + ex.getMessage());
                  return false;
                }
                return true;
              });
            } catch (ConditionTimeoutException ex) {
              logResponse(RUNNERS.get(RAND.nextInt(RUNNERS.size())) , "/diag_event/subscription");
              throw ex;
            }
          });
    }
  }

  @And("^the following subscriptions are created:$")
  public void theFollowingSubscriptionsExist(List<Map<String, String>> subscriptions) throws Throwable {
    synchronized (BasicSteps.class) {
      ReaperTestJettyRunner runner = RUNNERS.get(RAND.nextInt(RUNNERS.size()));
      for (Map<String, String> sub : subscriptions) {
        // see org.apache.cassandra.diag.DiagnosticEventPersistence.java#L137
        sub = Maps.newHashMap(sub);
        for (Map.Entry<String,String> eventType : EVENT_TYPES.entrySet()) {
          sub.put("events", sub.get("events").replace(eventType.getKey(), eventType.getValue()));
        }
        Response response = runner.callReaper("POST", "/diag_event/subscription", Optional.of(sub));

        Assertions
            .assertThat(response.getStatus())
            .isIn(Response.Status.CREATED.getStatusCode(), Response.Status.NO_CONTENT.getStatusCode());

        // rest command requests should not response with bodies, follow the location to GET that
        Assertions.assertThat(response.hasEntity()).isFalse();
        // follow to new location (to GET resource)
        response = runner.callReaper("GET", response.getLocation().toString(), Optional.empty());
        Assertions.assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
        Assertions.assertThat(response.hasEntity()).isTrue();
        String responseData = response.readEntity(String.class);
        DiagEventSubscription created = SimpleReaperClient.parseEventSubscriptionJSON(responseData);
        DiagEventSubscription provided = DiagEventSubscriptionMapper.fromParamMap(sub);
        Assertions.assertThat(provided.withId(created.getId().get())).isEqualTo(created);
        testContext.addCurrentEventSubscription(created);
      }
    }
  }

  @Then("^the returned list of subscriptions is empty$")
  public void reaperReturnsAnEmptyListOfSubscriptions() {
    synchronized (BasicSteps.class) {
      Assertions.assertThat(testContext.getRetrievedEventSubscriptions()).isEmpty();
    }
  }

  @Then("^the returned list of subscriptions is:$")
  public void theReturnedListOfSubscriptionsIs(List<Map<String, String>> subscriptions) throws Throwable {
    synchronized (BasicSteps.class) {

      List<DiagEventSubscription> expected = subscriptions
          .stream()
          .map((sub) -> {
            // see org.apache.cassandra.diag.DiagnosticEventPersistence.java#L137
            EVENT_TYPES.forEach((name, fqn) -> sub.get("events").replace(name, fqn));
            return sub;
          })
          .map(DiagEventSubscriptionMapper::fromParamMap)
          .collect(Collectors.toList());

      Assertions.assertThat(testContext.getRetrievedEventSubscriptions().size()).isEqualTo(expected.size());

      List<DiagEventSubscription> lastRetrieved = testContext.getRetrievedEventSubscriptions()
          .stream()
          .map(s ->
              new DiagEventSubscription(
                  Optional.empty(),
                  s.getCluster(),
                  Optional.of(s.getDescription()),
                  s.getNodes(),
                  s.getEvents(),
                  s.getExportSse(),
                  s.getExportFileLogger(),
                  s.getExportHttpEndpoint()))
          .collect(Collectors.toList());

      for (DiagEventSubscription sub : expected) {
        Assertions.assertThat(lastRetrieved).contains(sub);
      }
    }
  }

  private List<DiagEventSubscription> callSubscription(
      String method,
      Optional<Map<String, String>> params,
      Optional<UUID> id,
      Response.Status... expectedStatuses) {

    ReaperTestJettyRunner runner = RUNNERS.get(RAND.nextInt(RUNNERS.size()));
    String path = "/diag_event/subscription" + (id.isPresent() ? "/" + id.get() : "");
    Response response = runner.callReaper(method, path, params);

    Assertions
            .assertThat(Arrays.stream(expectedStatuses).map(Response.Status::getStatusCode))
            .contains(response.getStatus());

    String responseData = response.readEntity(String.class);

    return id.isPresent()
        ? ImmutableList.of(SimpleReaperClient.parseEventSubscriptionJSON(responseData))
        : SimpleReaperClient.parseEventSubscriptionsListJSON(responseData);
  }

  private static boolean isInstanceOfDistributedStorage(String storageClassname) {
    String csCls = CassandraStorage.class.getName();
    String pgCls = PostgresStorage.class.getName();
    return csCls.equals(storageClassname) || pgCls.equals(storageClassname);
  }

  @And("^percent repaired metrics get collected for the existing schedule$")
  public void percentRepairedMetricsGetCollectedForTheExistingSchedule() {
    synchronized (BasicSteps.class) {
      CLIENTS.parallelStream().forEach(client -> {
        await().with().pollInterval(POLL_INTERVAL).atMost(5, MINUTES).until(() -> {
          try {
            List<RepairScheduleStatus> schedules = client.getRepairSchedulesForCluster(TestContext.TEST_CLUSTER);
            callAndExpect(
                "GET",
                "/repair_schedule/" + TestContext.TEST_CLUSTER + "/"
                  + schedules.get(0).getId().toString() + "/percent_repaired",
                Optional.empty(),
                Optional.of(schedules.get(0).getId().toString()),
                Response.Status.OK
            );
          } catch (AssertionError ex) {
            LOG.warn(ex.getMessage());
            return false;
          }
          return true;
        });
      });
    }
  }
}
