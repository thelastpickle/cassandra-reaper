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

import io.cassandrareaper.SimpleReaperClient;
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

import java.util.Arrays;
import java.util.EnumSet;
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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.assertj.core.api.Assertions;
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
public final class BasicSteps {

  private static final Logger LOG = LoggerFactory.getLogger(BasicSteps.class);
  private static final Optional<Map<String, String>> EMPTY_PARAMS = Optional.empty();

  private static final List<ReaperTestJettyRunner> RUNNERS = new CopyOnWriteArrayList<>();
  private static final List<SimpleReaperClient> CLIENTS = new CopyOnWriteArrayList<>();
  private static final Random RAND = new Random(System.nanoTime());
  private static final AtomicReference<Upgradable> TEST_INSTANCE = new AtomicReference<>();

  private Optional<String> reaperVersion = Optional.empty();
  private Response lastResponse;

  public static synchronized void addReaperRunner(ReaperTestJettyRunner runner) {

    if (!CLIENTS.isEmpty()) {
      Preconditions.checkState(runner.runnerInstance.context.storage instanceof CassandraStorage);

      RUNNERS.stream()
          .forEach(r -> Preconditions.checkState(r.runnerInstance.context.storage instanceof CassandraStorage));
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
          .contains(response.getStatus())
          .withFailMessage(responseEntity)
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
              "expected data not found from the response: " + expectedDataInResponseData.get(),
              0 != responseEntity.length() && responseEntity.contains(expectedDataInResponseData.get()));

          LOG.debug("Data \"" + expectedDataInResponseData.get() + "\" was found from response data");
        }
      }
    });
  }

  @Given("^cluster seed host \"([^\"]*)\" points to cluster with name \"([^\"]*)\"$")
  public void cluster_seed_host_points_to_cluster_with_name(String seedHost, String clusterName) throws Throwable {
    synchronized (BasicSteps.class) {
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

  @Given("^that we are going to use \"([^\"]*)\" as cluster seed host$")
  public void that_we_are_going_to_use_as_cluster_seed_host(String seedHost) throws Throwable {
    synchronized (BasicSteps.class) {
      assert seedHost.contains("@");
      TestContext.SEED_HOST = seedHost;
    }
  }

  @Given("^that reaper ([^\"]*) is running$")
  public void start_reaper(String version) throws Throwable {
    synchronized (BasicSteps.class) {
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
        List<String> clusterNames = SimpleReaperClient.parseClusterNameListJSON(responseData);
        assertEquals(0, clusterNames.size());
      });
    }
  }

  @When("^an add-cluster request is made to reaper$")
  public void an_add_cluster_request_is_made_to_reaper() throws Throwable {
    synchronized (BasicSteps.class) {

      RUNNERS
          .parallelStream()
          .forEach(
              runner -> {
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
                    .contains(responseStatus)
                    .withFailMessage(responseEntity);

                // rest command requests should not response with bodies, follow the location to GET that
                Assertions.assertThat(responseEntity).isEmpty();

                // follow to new location (to GET resource)
                response = runner.callReaper("GET", response.getLocation().toString(), Optional.empty());

                String responseData = response.readEntity(String.class);
                Assertions.assertThat(responseData).isNotEmpty();
                Map<String, Object> cluster
                    = SimpleReaperClient.parseClusterStatusJSON(responseData);

                if (Response.Status.CREATED.getStatusCode() == responseStatus) {
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
          .contains(responseStatus)
          .withFailMessage(responseEntity);

      // rest command requests should not response with bodies, follow the location to GET that
      Assertions.assertThat(responseEntity).isEmpty();

      // follow to new location (to GET resource)
      response = runner.callReaper("GET", response.getLocation().toString(), Optional.empty());
      String responseData = response.readEntity(String.class);
      RepairScheduleStatus schedule = SimpleReaperClient.parseRepairScheduleStatusJSON(responseData);

      if (Response.Status.CREATED.getStatusCode() == responseStatus) {
        TestContext.LAST_MODIFIED_ID = schedule.getId();
      } else {
        // if the original request to create the schedule failed then we have to wait til we can find it
        await().with().pollInterval(1, SECONDS).atMost(1, MINUTES).until(() -> {
          try {
            List<RepairScheduleStatus> schedules = runner.getClient().getRepairSchedulesForCluster(clusterName);
            LOG.info("Got " + schedules.size() + " schedules");
            Assertions.assertThat(schedules).hasSize(1);
            TestContext.LAST_MODIFIED_ID = schedules.get(0).getId();
          } catch (AssertionError ex) {
            LOG.warn(ex.getMessage());
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

      Assertions.assertThat(
          ImmutableList.of(
            Response.Status.CREATED.getStatusCode(),
            Response.Status.NO_CONTENT.getStatusCode()))
          .contains(responseStatus)
          .withFailMessage(responseEntity);

      // rest command requests should not response with bodies, follow the location to GET that
      Assertions.assertThat(responseEntity).isEmpty();

      // follow to new location (to GET resource)
      response = runner.callReaper("GET", response.getLocation().toString(), Optional.empty());
      String responseData = response.readEntity(String.class);
      RepairScheduleStatus schedule = SimpleReaperClient.parseRepairScheduleStatusJSON(responseData);

      if (Response.Status.CREATED.getStatusCode() == responseStatus) {
        TestContext.LAST_MODIFIED_ID = schedule.getId();
      } else {
        // if the original request to create the schedule failed then we have to wait til we can find it
        await().with().pollInterval(1, SECONDS).atMost(1, MINUTES).until(() -> {
          try {
            List<RepairScheduleStatus> schedules
                = runner.getClient().getRepairSchedulesForCluster(TestContext.TEST_CLUSTER);

            LOG.info("Got " + schedules.size() + " schedules");
            Assertions.assertThat(schedules).hasSize(1);
            TestContext.LAST_MODIFIED_ID = schedules.get(0).getId();
          } catch (AssertionError ex) {
            LOG.warn(ex.getMessage());
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
          .contains(responseStatus)
          .withFailMessage(responseEntity);

      // non-error rest command requests should not response with bodies
      if (Response.Status.CONFLICT.getStatusCode() != responseStatus) {
        Assertions.assertThat(responseEntity).isEmpty();
      }

      // follow to new location (to GET resource)
      response = runner.callReaper("GET", response.getLocation().toString(), Optional.empty());
      String responseData = response.readEntity(String.class);
      RepairScheduleStatus schedule = SimpleReaperClient.parseRepairScheduleStatusJSON(responseData);

      if (Response.Status.CREATED.getStatusCode() == responseStatus) {
        TestContext.LAST_MODIFIED_ID = schedule.getId();
      } else {
        // if the original request to create the schedule failed then we have to wait til we can find it
        await().with().pollInterval(1, SECONDS).atMost(1, MINUTES).until(() -> {
          try {
            List<RepairScheduleStatus> schedules
                = runner.getClient().getRepairSchedulesForCluster(TestContext.TEST_CLUSTER);

            LOG.info("Got " + schedules.size() + " schedules");
            Assertions.assertThat(schedules).hasSize(1);
            TestContext.LAST_MODIFIED_ID = schedules.get(0).getId();
          } catch (AssertionError ex) {
            LOG.warn(ex.getMessage());
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
      RUNNERS.parallelStream().forEach(runner -> {
        LOG.info("waiting for a scheduled repair run to start for cluster: {}", clusterName);
        await().with().pollInterval(10, SECONDS).atMost(2, MINUTES).until(() -> {
          try {
            Response response = runner
                .callReaper("GET", "/repair_run/cluster/" + TestContext.TEST_CLUSTER, EMPTY_PARAMS);

            Assertions.assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
            String responseData = response.readEntity(String.class);
            List<RepairRunStatus> runs = SimpleReaperClient.parseRepairRunStatusListJSON(responseData);
            if (!runs.isEmpty()) {
              TestContext.LAST_MODIFIED_ID = runs.get(0).getId();
            }
            return !runs.isEmpty();
          } catch (AssertionError ex) {
            LOG.warn(ex.getMessage());
            return false;
          }
        });
      });
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
      RepairScheduleStatus schedule = SimpleReaperClient.parseRepairScheduleStatusJSON(responseData);
      TestContext.LAST_MODIFIED_ID = schedule.getId();
    }
  }

  @And("^reaper has (\\d+) scheduled repairs for cluster called \"([^\"]*)\"$")
  public void reaper_has_scheduled_repairs_for_cluster_called(
      int expectedSchedules,
      String clusterName) throws Throwable {

    synchronized (BasicSteps.class) {
      CLIENTS.parallelStream().forEach(client -> {
        await().with().pollInterval(1, SECONDS).atMost(1, MINUTES).until(() -> {
          try {
            List<RepairScheduleStatus> schedules = client.getRepairSchedulesForCluster(clusterName);
            LOG.info("Got " + schedules.size() + " schedules");
            Assertions.assertThat(schedules).hasSize(expectedSchedules);
          } catch (AssertionError ex) {
            LOG.warn(ex.getMessage());
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
        await().with().pollInterval(1, SECONDS).atMost(1, MINUTES).until(() -> {
          try {
            List<RepairScheduleStatus> schedules = client.getRepairSchedulesForCluster(TestContext.TEST_CLUSTER);
            LOG.info("Got " + schedules.size() + " schedules");
            Assertions.assertThat(schedules).hasSize(expectedSchedules);
          } catch (AssertionError ex) {
            LOG.warn(ex.getMessage());
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
      LOG.info("pause last added repair schedule with id: {}", TestContext.LAST_MODIFIED_ID);
      Map<String, String> params = Maps.newHashMap();
      params.put("state", "paused");

      callAndExpect(
          "PUT",
          "/repair_schedule/" + TestContext.LAST_MODIFIED_ID,
          Optional.of(params),
          Optional.of("\"" + clusterName + "\""),
          Response.Status.OK,
          Response.Status.NO_CONTENT);

      LOG.info("delete last added repair schedule with id: {}", TestContext.LAST_MODIFIED_ID);
      params.clear();
      params.put("owner", TestContext.TEST_USER);

      callAndExpect("DELETE",
          "/repair_schedule/" + TestContext.LAST_MODIFIED_ID,
          Optional.of(params),
          Optional.empty(),
          Response.Status.ACCEPTED,
          Response.Status.NOT_FOUND);

      await().with().pollInterval(1, SECONDS).atMost(1, MINUTES).until(() -> {
        try {
          callAndExpect(
              "DELETE",
              "/repair_schedule/" + TestContext.LAST_MODIFIED_ID,
              Optional.of(params),
              Optional.empty(),
              Response.Status.NOT_FOUND);
        } catch (AssertionError ex) {
          LOG.warn(ex.getMessage());
          return false;
        }
        return true;
      });
    }
  }

  @When("^the last added schedule is deleted for the last added cluster$")
  public void the_last_added_schedule_is_deleted_for_the_last_added_cluster() throws Throwable {
    synchronized (BasicSteps.class) {
      LOG.info("pause last added repair schedule with id: {}", TestContext.LAST_MODIFIED_ID);
      Map<String, String> params = Maps.newHashMap();
      params.put("state", "paused");

      callAndExpect(
          "PUT",
          "/repair_schedule/" + TestContext.LAST_MODIFIED_ID,
          Optional.of(params),
          Optional.of("\"" + TestContext.TEST_CLUSTER + "\""),
          Response.Status.OK,
          Response.Status.NO_CONTENT);

      LOG.info("delete last added repair schedule with id: {}", TestContext.LAST_MODIFIED_ID);
      params.clear();
      params.put("owner", TestContext.TEST_USER);

      callAndExpect(
          "DELETE",
          "/repair_schedule/" + TestContext.LAST_MODIFIED_ID,
          Optional.of(params),
          Optional.empty(),
          Response.Status.ACCEPTED,
          Response.Status.NOT_FOUND);

      await().with().pollInterval(1, SECONDS).atMost(1, MINUTES).until(() -> {
        try {
          callAndExpect(
              "DELETE",
              "/repair_schedule/" + TestContext.LAST_MODIFIED_ID,
              Optional.of(params),
              Optional.empty(),
              Response.Status.NOT_FOUND);
        } catch (AssertionError ex) {
          LOG.warn(ex.getMessage());
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

        await().with().pollInterval(1, SECONDS).atMost(1, MINUTES).until(() -> {
          try {
            callAndExpect(
                "DELETE",
                "/repair_schedule/" + schedule.getId(),
                Optional.of(params),
                Optional.empty(),
                Response.Status.NOT_FOUND);
            return true;
          } catch (AssertionError ex) {
            LOG.warn(ex.getMessage());
            return false;
          }
        });
      });
    }
  }

  @And("^deleting cluster called \"([^\"]*)\" fails$")
  public void deleting_cluster_called_fails(String clusterName) throws Throwable {
    synchronized (BasicSteps.class) {
      callAndExpect(
          "DELETE",
          "/cluster/" + clusterName,
          EMPTY_PARAMS,
          Optional.empty(),
          Response.Status.CONFLICT);
    }
  }

  @And("^deleting the last added cluster fails$")
  public void deleting_the_last_added_cluster_fails() throws Throwable {
    synchronized (BasicSteps.class) {
      await().with().pollInterval(1, SECONDS).atMost(1, MINUTES).until(() -> {
        try {
          callAndExpect(
              "DELETE",
              "/cluster/" + TestContext.TEST_CLUSTER,
              EMPTY_PARAMS,
              Optional.empty(),
              Response.Status.CONFLICT);
        } catch (AssertionError ex) {
          LOG.warn(ex.getMessage());
          return false;
        }
        return true;
      });
    }
  }

  @And("^cluster called \"([^\"]*)\" is deleted$")
  public void cluster_called_is_deleted(String clusterName) throws Throwable {
    synchronized (BasicSteps.class) {
      callAndExpect(
          "DELETE",
          "/cluster/" + clusterName,
          EMPTY_PARAMS,
          Optional.<String>empty(),
          Response.Status.ACCEPTED,
          Response.Status.NOT_FOUND);

      await().with().pollInterval(1, SECONDS).atMost(1, MINUTES).until(() -> {
        try {
          callAndExpect(
              "GET",
              "/cluster/" + clusterName,
              EMPTY_PARAMS,
              Optional.<String>empty(),
              Response.Status.NOT_FOUND);
        } catch (AssertionError ex) {
          LOG.warn(ex.getMessage());
          return false;
        }
        return true;
      });
    }
  }

  @And("^the last added cluster is deleted$")
  public void cluster_called_is_deleted() throws Throwable {
    synchronized (BasicSteps.class) {
      callAndExpect(
          "DELETE",
          "/cluster/" + TestContext.TEST_CLUSTER,
          EMPTY_PARAMS,
          Optional.<String>empty(),
          Response.Status.ACCEPTED,
          Response.Status.NOT_FOUND);

      await().with().pollInterval(1, SECONDS).atMost(1, MINUTES).until(() -> {
        try {
          callAndExpect(
              "GET",
              "/cluster/" + TestContext.TEST_CLUSTER,
              EMPTY_PARAMS,
              Optional.<String>empty(),
              Response.Status.NOT_FOUND);
        } catch (AssertionError ex) {
          LOG.warn(ex.getMessage());
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
      RepairRunStatus run = SimpleReaperClient.parseRepairRunStatusJSON(responseData);
      TestContext.LAST_MODIFIED_ID = run.getId();
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
      RepairRunStatus run = SimpleReaperClient.parseRepairRunStatusJSON(responseData);
      TestContext.LAST_MODIFIED_ID = run.getId();
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
      RepairRunStatus run = SimpleReaperClient.parseRepairRunStatusJSON(responseData);
      TestContext.LAST_MODIFIED_ID = run.getId();
    }
  }

  @And("^the last added repair has table \"([^\"]*)\" in the blacklist$")
  public void the_last_added_repair_has_table_in_the_blacklist(String blacklistedTable)
      throws Throwable {
    synchronized (BasicSteps.class) {
      RUNNERS
          .parallelStream()
          .forEach(
              runner -> {
                Response response = runner.callReaper(
                        "GET", "/repair_run/cluster/" + TestContext.TEST_CLUSTER, EMPTY_PARAMS);
                String responseData = response.readEntity(String.class);
                assertEquals(responseData, Response.Status.OK.getStatusCode(), response.getStatus());
                List<RepairRunStatus> runs = SimpleReaperClient.parseRepairRunStatusListJSON(responseData);
                assertTrue(runs.get(0).getBlacklistedTables().contains(blacklistedTable));
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
      RepairRunStatus run = SimpleReaperClient.parseRepairRunStatusJSON(responseData);
      TestContext.LAST_MODIFIED_ID = run.getId();
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
      RepairRunStatus run = SimpleReaperClient.parseRepairRunStatusJSON(responseData);
      TestContext.LAST_MODIFIED_ID = run.getId();
    }
  }

  @Then("^reaper has (\\d+) repairs for cluster called \"([^\"]*)\"$")
  public void reaper_has_repairs_for_cluster_called(int runAmount, String clusterName) throws Throwable {
    synchronized (BasicSteps.class) {
      RUNNERS.parallelStream().forEach(runner -> {
        Response response = runner.callReaper("GET", "/repair_run/cluster/" + clusterName, EMPTY_PARAMS);
        String responseData = response.readEntity(String.class);
        assertEquals(responseData, Response.Status.OK.getStatusCode(), response.getStatus());
        List<RepairRunStatus> runs = SimpleReaperClient.parseRepairRunStatusListJSON(responseData);
        assertEquals(runAmount, runs.size());
      });
    }
  }

  @Then("^reaper has (\\d+) started or done repairs for cluster called \"([^\"]*)\"$")
  public void reaper_has_running_repairs_for_cluster_called(int runAmount, String clusterName) throws Throwable {

    Set<RepairRun.RunState> startedStates = EnumSet.copyOf(
        Sets.newHashSet(RepairRun.RunState.RUNNING, RepairRun.RunState.DONE));

    synchronized (BasicSteps.class) {
      RUNNERS.parallelStream().forEach(runner -> {
        Response response = runner.callReaper("GET", "/repair_run/cluster/" + clusterName, EMPTY_PARAMS);
        String responseData = response.readEntity(String.class);
        assertEquals(responseData, Response.Status.OK.getStatusCode(), response.getStatus());
        List<RepairRunStatus> runs = SimpleReaperClient.parseRepairRunStatusListJSON(responseData);
        assertEquals(runAmount, runs.stream().filter(rrs -> startedStates.contains(rrs.getState())).count());
      });
    }
  }

  @Then("^reaper has (\\d+) repairs for the last added cluster$")
  public void reaper_has_repairs_for_the_last_added_cluster(int runAmount) throws Throwable {
    synchronized (BasicSteps.class) {
      RUNNERS.parallelStream().forEach(runner -> {
        Response response = runner.callReaper("GET", "/repair_run/cluster/" + TestContext.TEST_CLUSTER, EMPTY_PARAMS);
        String responseData = response.readEntity(String.class);
        assertEquals(responseData, Response.Status.OK.getStatusCode(), response.getStatus());
        List<RepairRunStatus> runs = SimpleReaperClient.parseRepairRunStatusListJSON(responseData);
        assertEquals(runAmount, runs.size());
      });
    }
  }

  @Then("^reaper has (\\d+) started or done repairs for the last added cluster$")
  public void reaper_has_started_repairs_for_the_last_added_cluster(int runAmount) throws Throwable {

    Set<RepairRun.RunState> startedStates = EnumSet.copyOf(
        Sets.newHashSet(RepairRun.RunState.RUNNING, RepairRun.RunState.DONE));

    synchronized (BasicSteps.class) {
      RUNNERS.parallelStream().forEach(runner -> {
        Response response = runner.callReaper("GET", "/repair_run/cluster/" + TestContext.TEST_CLUSTER, EMPTY_PARAMS);
        String responseData = response.readEntity(String.class);
        assertEquals(responseData, Response.Status.OK.getStatusCode(), response.getStatus());
        List<RepairRunStatus> runs = SimpleReaperClient.parseRepairRunStatusListJSON(responseData);
        assertEquals(runAmount, runs.stream().filter(rrs -> startedStates.contains(rrs.getState())).count());
      });
    }
  }

  @When("^the last added repair run is deleted$")
  public void the_last_added_repair_run_is_deleted_for_cluster_called() throws Throwable {
    synchronized (BasicSteps.class) {
      LOG.info("delete last added repair run with id: {}", TestContext.LAST_MODIFIED_ID);
      Map<String, String> params = Maps.newHashMap();
      params.put("owner", TestContext.TEST_USER);

      callAndExpect(
          "DELETE",
          "/repair_run/" + TestContext.LAST_MODIFIED_ID,
          Optional.of(params),
          Optional.empty(),
          Response.Status.ACCEPTED,
          Response.Status.NOT_FOUND,
          Response.Status.CONFLICT);

      await().with().pollInterval(1, SECONDS).atMost(1, MINUTES).until(() -> {
        try {
          callAndExpect(
              "DELETE",
              "/repair_run/" + TestContext.LAST_MODIFIED_ID,
              Optional.of(params),
              Optional.empty(),
              Response.Status.NOT_FOUND);
        } catch (AssertionError ex) {
          LOG.warn(ex.getMessage());
          return false;
        }
        return true;
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

        await().with().pollInterval(1, SECONDS).atMost(1, MINUTES).until(() -> {
          try {
            callAndExpect(
                "DELETE",
                "/repair_run/" + id,
                Optional.of(params),
                Optional.empty(),
                Response.Status.NOT_FOUND);
          } catch (AssertionError ex) {
            LOG.warn(ex.getMessage());
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
            .contains(status)
            .withFailMessage(responseEntity);
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
            "/repair_run/" + TestContext.LAST_MODIFIED_ID + "/state/RUNNING",
            Optional.of(Maps.newHashMap()));

        int status = response.getStatus();
        String responseEntity = response.readEntity(String.class);

        Assertions.assertThat(
              ImmutableList.of(Response.Status.OK.getStatusCode(), Response.Status.NO_CONTENT.getStatusCode()))
            .contains(status)
            .withFailMessage(responseEntity);

        // rest command requests should not response with bodies, follow the location to GET that
        Assertions.assertThat(responseEntity).isEmpty();

        // follow to new location (to GET resource)
        response = runner.callReaper("GET", response.getLocation().toString(), Optional.empty());

        if (Response.Status.OK.getStatusCode() == status) {
          String responseData = response.readEntity(String.class);
          RepairRunStatus run = SimpleReaperClient.parseRepairRunStatusJSON(responseData);
          TestContext.LAST_MODIFIED_ID = run.getId();
          set.compareAndSet(false, true);
        }
      });
      Assertions.assertThat(set.get()).isTrue();

      callAndExpect(
          "PUT",
          "/repair_run/" + TestContext.LAST_MODIFIED_ID + "/state/RUNNING",
          Optional.empty(),
          Optional.empty(),
          Response.Status.NO_CONTENT);
    }
  }

  @When("^the last added repair is stopped$")
  public void the_last_added_repair_is_stopped_for() throws Throwable {
    synchronized (BasicSteps.class) {
      stopRepairRun(TestContext.LAST_MODIFIED_ID);
    }
  }

  private void stopRepairRun(UUID repairRunId) {
    // given "state" is same as the current run state
    RUNNERS.parallelStream().forEach(runner -> {
      Map<String, String> params = Maps.newHashMap();

      Response response = runner.callReaper(
          "PUT",
          "/repair_run/" + repairRunId + "/state/PAUSED",
          Optional.of(params));

      int status = response.getStatus();
      String responseEntity = response.readEntity(String.class);

      Assertions.assertThat(
            ImmutableList.of(
                Response.Status.OK.getStatusCode(),
                Response.Status.NO_CONTENT.getStatusCode(),
                Response.Status.CONFLICT.getStatusCode()))
          .contains(status)
          .withFailMessage(responseEntity);

      // non-error rest command requests should not response with bodies
      if (Response.Status.CONFLICT.getStatusCode() != status) {
        Assertions.assertThat(responseEntity).isEmpty();
      }
    });

    callAndExpect(
        "PUT",
        "/repair_run/" + repairRunId + "/state/PAUSED",
        Optional.empty(),
        Optional.empty(),
        Response.Status.NO_CONTENT,
        Response.Status.CONFLICT);
  }

  @And("^we wait for at least (\\d+) segments to be repaired$")
  public void we_wait_for_at_least_segments_to_be_repaired(int nbSegmentsToBeRepaired) throws Throwable {
    synchronized (BasicSteps.class) {
      RUNNERS.parallelStream().forEach(runner -> {
        await().with().pollInterval(10, SECONDS).atMost(2, MINUTES).until(() -> {
          try {
            Response response = runner
                .callReaper("GET", "/repair_run/" + TestContext.LAST_MODIFIED_ID, EMPTY_PARAMS);

            Assertions.assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
            String responseData = response.readEntity(String.class);
            RepairRunStatus run = SimpleReaperClient.parseRepairRunStatusJSON(responseData);
            return nbSegmentsToBeRepaired <= run.getSegmentsRepaired();
          } catch (AssertionError ex) {
            LOG.warn(ex.getMessage());
            return false;
          }
        });
      });
    }
  }

  @Then("^reseting one segment sets its state to not started$")
  public void reseting_one_segment_sets_its_state_to_not_started() throws Throwable {
    synchronized (BasicSteps.class) {
      RUNNERS
          .parallelStream()
          .forEach(
              runner -> {
                await()
                    .with()
                    .pollInterval(10, SECONDS)
                    .atMost(2, MINUTES)
                    .until(
                        () -> {
                          Response response = runner.callReaper(
                                  "GET",
                                  "/repair_run/" + TestContext.LAST_MODIFIED_ID + "/segments",
                                  EMPTY_PARAMS);

                          assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
                          String responseData = response.readEntity(String.class);
                          List<RepairSegment> segments = SimpleReaperClient.parseRepairSegmentsJSON(responseData);

                          boolean gotDoneSegments = segments
                                      .stream()
                                      .filter(seg -> seg.getState() == RepairSegment.State.DONE)
                                      .count()
                                  > 0;

                          if (gotDoneSegments) {
                            TestContext.FINISHED_SEGMENT = segments
                                    .stream()
                                    .filter(seg -> seg.getState() == RepairSegment.State.DONE)
                                    .map(segment -> segment.getId())
                                    .collect(Collectors.toList())
                                    .get(0);
                          }

                          return gotDoneSegments;
                        });
              });

      RUNNERS
          .parallelStream()
          .forEach(
              runner -> {
                await()
                    .with()
                    .pollInterval(10, SECONDS)
                    .atMost(2, MINUTES)
                    .until(
                        () -> {
                          Response abort = runner.callReaper(
                                  "GET",
                                  "/repair_run/"
                                      + TestContext.LAST_MODIFIED_ID
                                      + "/segments/abort/"
                                      + TestContext.FINISHED_SEGMENT,
                                  EMPTY_PARAMS);

                          Response response = runner.callReaper(
                                  "GET",
                                  "/repair_run/" + TestContext.LAST_MODIFIED_ID + "/segments",
                                  EMPTY_PARAMS);

                          assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
                          String responseData = response.readEntity(String.class);
                          List<RepairSegment> segments = SimpleReaperClient.parseRepairSegmentsJSON(responseData);

                          return segments
                                  .stream()
                                  .filter(
                                      seg ->
                                          seg.getId().equals(TestContext.FINISHED_SEGMENT)
                                              && seg.getState() == RepairSegment.State.NOT_STARTED)
                                  .count()
                              > 0;
                        });
              });
    }
  }

  @And("^the last added cluster has a keyspace called reaper_db$")
  public void the_last_added_cluster_has_a_keyspace_called_reaper_db() throws Throwable {
    synchronized (BasicSteps.class) {
      RUNNERS
          .parallelStream()
          .forEach(
              runner -> {
                Response response = runner.callReaper(
                        "GET", "/cluster/" + TestContext.TEST_CLUSTER + "/tables", EMPTY_PARAMS);
                assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
                String responseData = response.readEntity(String.class);
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

      Response response
          = runner.callReaper("POST", "/snapshot/cluster/" + TestContext.TEST_CLUSTER, EMPTY_PARAMS);

      assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    }
  }

  @Then("^there is (\\d+) snapshot returned when listing snapshots$")
  public void there_is_1_snapshot_returned_when_listing_snapshots(int nbSnapshots)
      throws Throwable {
    synchronized (BasicSteps.class) {
      ReaperTestJettyRunner runner = RUNNERS.get(0);
      Response response = runner.callReaper(
              "GET",
              "/snapshot/" + TestContext.TEST_CLUSTER + "/" + TestContext.SEED_HOST.split("@")[0],
              EMPTY_PARAMS);
      assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
      String responseData = response.readEntity(String.class);
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
      Map<String, List<Snapshot>> snapshots = SimpleReaperClient.parseSnapshotMapJSON(responseData);

      snapshots
          .keySet()
          .stream()
          .forEach(
              snapshot -> {
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
      Map<String, List<Snapshot>> snapshots = SimpleReaperClient.parseSnapshotMapJSON(responseData);

      snapshots
          .keySet()
          .stream()
          .forEach(
              snapshot -> {
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
    RUNNERS
        .parallelStream()
        .forEach(
            runner -> {
              lastResponse = runner.callReaper(method, requestPath, EMPTY_PARAMS);
            });
  }

  @Then("^a \"([^\"]*)\" response is returned$")
  public void aResponseIsReturned(String statusDescription) throws Throwable {
    assertEquals(lastResponse.getStatus(), httpStatus(statusDescription));
  }

  @Then("^the response was redirected to the login page$")
  public void theResponseWasRedirectedToTheLoginPage() throws Throwable {
    System.out.println("response = " + lastResponse.toString());
    assertTrue(lastResponse.hasEntity());
    assertTrue(
        lastResponse.readEntity(String.class).contains("<title>Not a real login page</title>"));
  }

  @And("^we can collect the tpstats from the seed node$")
  public void we_can_collect_the_tpstats_from_the_seed_node() throws Throwable {
    synchronized (BasicSteps.class) {
      RUNNERS
          .parallelStream()
          .forEach(
              runner -> {
                Response response = runner.callReaper(
                        "GET",
                        "/node/tpstats/"
                            + TestContext.TEST_CLUSTER
                            + "/"
                            + TestContext.SEED_HOST.split("@")[0],
                        EMPTY_PARAMS);
                assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
                String responseData = response.readEntity(String.class);
                List<ThreadPoolStat> tpstats = SimpleReaperClient.parseTpStatJSON(responseData);

                assertTrue(
                    tpstats.stream().filter(tpstat -> tpstat.getName().equals("ReadStage")).count()
                        == 1);
                assertTrue(
                    tpstats
                            .stream()
                            .filter(tpstat -> tpstat.getName().equals("ReadStage"))
                            .filter(tpstat -> tpstat.getCurrentlyBlockedTasks() == 0)
                            .filter(tpstat -> tpstat.getCompletedTasks() > 0)
                            .count()
                        == 1);
              });
    }
  }

  @And("^we can collect the dropped messages stats from the seed node$")
  public void we_can_collect_the_dropped_messages_stats_from_the_seed_node() throws Throwable {
    synchronized (BasicSteps.class) {
      RUNNERS
          .parallelStream()
          .forEach(
              runner -> {
                Response response = runner.callReaper(
                        "GET",
                        "/node/dropped/"
                            + TestContext.TEST_CLUSTER
                            + "/"
                            + TestContext.SEED_HOST.split("@")[0],
                        EMPTY_PARAMS);
                assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
                String responseData = response.readEntity(String.class);
                List<DroppedMessages> tpstats = SimpleReaperClient.parseDroppedMessagesJSON(responseData);

                assertTrue(
                    tpstats.stream().filter(tpstat -> tpstat.getName().equals("READ")).count()
                        == 1);
                assertTrue(
                    tpstats
                            .stream()
                            .filter(tpstat -> tpstat.getName().equals("READ"))
                            .filter(tpstat -> tpstat.getCount() >= 0)
                            .count()
                        == 1);
              });
    }
  }

  @And("^we can collect the client request metrics from the seed node$")
  public void we_can_collect_the_client_request_metrics_from_the_seed_node() throws Throwable {
    synchronized (BasicSteps.class) {
      RUNNERS
          .parallelStream()
          .forEach(
              runner -> {
                Response response = runner.callReaper(
                        "GET",
                        "/node/clientRequestLatencies/"
                            + TestContext.TEST_CLUSTER
                            + "/"
                            + TestContext.SEED_HOST.split("@")[0],
                        EMPTY_PARAMS);
                assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
                String responseData = response.readEntity(String.class);
                List<MetricsHistogram> clientRequestMetrics
                    = SimpleReaperClient.parseClientRequestMetricsJSON(responseData);

                assertTrue(
                    clientRequestMetrics
                            .stream()
                            .filter(metric -> metric.getName().startsWith("Write"))
                            .count()
                        >= 1);
              });
    }
  }

  @And("^the seed node has vnodes$")
  public void the_seed_node_has_vnodes() {
    synchronized (BasicSteps.class) {
      RUNNERS
          .parallelStream()
          .forEach(
              runner -> {
                Response response
                    = runner.callReaper(
                        "GET",
                        "/node/tokens/"
                            + TestContext.TEST_CLUSTER
                            + "/"
                            + TestContext.SEED_HOST.split("@")[0],
                        EMPTY_PARAMS);
                assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
                String responseData = response.readEntity(String.class);
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
      tmpSession.execute("CREATE KEYSPACE IF NOT EXISTS " + keyspaceName
          + " WITH replication = {" + buildNetworkTopologyStrategyString(cluster) + "}");
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
        .build();
  }

  private static void createTable(String keyspaceName, String tableName) {
    try (Cluster cluster = buildCluster(); Session tmpSession = cluster.connect()) {
      tmpSession.execute(
          "CREATE TABLE IF NOT EXISTS " + keyspaceName + "." + tableName + "(id int PRIMARY KEY, value text)");

      for (int i = 0; i < 100; i++) {
        tmpSession.execute(
            "INSERT INTO " + keyspaceName + "." + tableName + "(id, value) VALUES(" + i + ",'" + i + "')");
      }
    }
  }
}
