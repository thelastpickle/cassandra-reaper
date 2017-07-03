package com.spotify.reaper.acceptance;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.spotify.reaper.AppContext;
import com.spotify.reaper.SimpleReaperClient;
import com.spotify.reaper.cassandra.JmxConnectionFactory;
import com.spotify.reaper.cassandra.JmxProxy;
import com.spotify.reaper.resources.CommonTools;
import com.spotify.reaper.resources.view.RepairRunStatus;
import com.spotify.reaper.resources.view.RepairScheduleStatus;
import com.spotify.reaper.storage.CassandraStorage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.ws.rs.core.Response;

import cucumber.api.java.Before;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import org.assertj.core.api.Assert;
import org.assertj.core.api.Assertions;
import org.joda.time.DateTime;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.awaitility.Awaitility.*;
import static java.util.concurrent.TimeUnit.*;

/**
 * Basic acceptance test (Cucumber) steps.
 */
public final class BasicSteps {

  private static final Logger LOG = LoggerFactory.getLogger(BasicSteps.class);
  private static final Optional<Map<String, String>> EMPTY_PARAMS = Optional.absent();
  private static final String MEMORY_CONFIG_FILE="cassandra-reaper-at.yaml";

  private static final List<ReaperTestJettyRunner> RUNNERS = new CopyOnWriteArrayList<>();
  private static final List<SimpleReaperClient> CLIENTS = new CopyOnWriteArrayList<>();
  private static final Random RAND = new Random(System.nanoTime());

  public static void addReaperRunner(ReaperTestJettyRunner runner) {

    if (!CLIENTS.isEmpty()) {
        Preconditions.checkState(runner.runnerInstance.context.storage instanceof CassandraStorage);

        RUNNERS.stream()
                .forEach(r -> Preconditions.checkState(r.runnerInstance.context.storage instanceof CassandraStorage));
    }
    RUNNERS.add(runner);
    CLIENTS.add(runner.getClient());
  }

  @Before
  public static void setup() {
    // actual setup is done in setupReaperTestRunner step
  }

  private void setupReaperTestRunner() throws Exception {
    if(CLIENTS.isEmpty()) {
        assert RUNNERS.isEmpty();
        LOG.info("setting up testing Reaper runner with {} seed hosts defined",
                 TestContext.TEST_CLUSTER_SEED_HOSTS.size());
        AppContext context = new AppContext();
        context.jmxConnectionFactory = mock(JmxConnectionFactory.class);
        for (String seedHost : TestContext.TEST_CLUSTER_SEED_HOSTS.keySet()) {
          String clusterName = TestContext.TEST_CLUSTER_SEED_HOSTS.get(seedHost);
          Map<String, Set<String>> clusterKeyspaces = TestContext.TEST_CLUSTER_INFO.get(clusterName);
          JmxProxy jmx = mock(JmxProxy.class);
          when(jmx.getClusterName()).thenReturn(clusterName);
          when(jmx.getPartitioner()).thenReturn("org.apache.cassandra.dht.RandomPartitioner");
          when(jmx.getKeyspaces()).thenReturn(Lists.newArrayList(clusterKeyspaces.keySet()));
          when(jmx.getTokens()).thenReturn(Lists.newArrayList(new BigInteger("0")));
          when(jmx.getLiveNodes()).thenReturn(Arrays.asList(seedHost));

          for (String keyspace : clusterKeyspaces.keySet()) {
            when(jmx.getTableNamesForKeyspace(keyspace)).thenReturn(clusterKeyspaces.get(keyspace));
          }
          when(context.jmxConnectionFactory.connect(org.mockito.Matchers.<Optional>any(), eq(seedHost), context.config.getJmxConnectionTimeoutInSeconds()))
              .thenReturn(jmx);
        }
        ReaperTestJettyRunner runner = new ReaperTestJettyRunner();
        runner.setup(context, MEMORY_CONFIG_FILE);
        addReaperRunner(runner);
    }
  }

  private void setupReaperIntegrationTestRunner() throws Exception {
    if(CLIENTS.isEmpty()) {
        assert RUNNERS.isEmpty();
        LOG.info("setting up testing Reaper runner with {} seed hosts defined",
                 TestContext.TEST_CLUSTER_SEED_HOSTS.size());
        AppContext context = new AppContext();
        ReaperTestJettyRunner runner = new ReaperTestJettyRunner();
        runner.setup(context, MEMORY_CONFIG_FILE);
        addReaperRunner(runner);
    }
  }

  public void callAndExpect(
          String httpMethod,
          String callPath,
          Optional<Map<String, String>> params,
          Optional<String> expectedDataInResponseData,
          Response.Status... expectedStatuses) {

      final StringBuffer responseData = new StringBuffer();

      RUNNERS.parallelStream().forEach(runner -> {
        Response response = runner.callReaper(httpMethod, callPath, params);

        if (1 < RUNNERS.size()) {
            Assertions
                .assertThat(Arrays.asList(expectedStatuses).stream().map(Response.Status::getStatusCode))
                .contains(response.getStatus());
        } else {
            assertEquals(expectedStatuses[0].getStatusCode(), response.getStatus());
        }
        if (expectedStatuses[0].getStatusCode() == response.getStatus()) {
            responseData.append(response.readEntity(String.class));
            LOG.info("Got response data: " + responseData);
            if (expectedDataInResponseData.isPresent()) {
              assertTrue(
                      "expected data not found from the response: " + expectedDataInResponseData.get(),
                      0 != responseData.length() && responseData.toString().contains(expectedDataInResponseData.get()));

              LOG.debug("Data \"" + expectedDataInResponseData.get() + "\" was found from response data");
            }
        }
      });
  }

  @Given("^a reaper service is running$")
  public void a_reaper_service_is_running() throws Throwable {
    setupReaperTestRunner();
    //callAndExpect("GET", "/ping", Optional.<Map<String, String>>absent(),
    //              Response.Status.OK, Optional.<String>absent());
  }

  @Given("^a real reaper service is running$")
  public void a_real_reaper_service_is_running() throws Throwable {
    setupReaperIntegrationTestRunner();
    callAndExpect("GET", "/ping", Optional.<Map<String, String>>absent(), Optional.<String>absent(), Response.Status.OK);
  }

  @Given("^cluster seed host \"([^\"]*)\" points to cluster with name \"([^\"]*)\"$")
  public void cluster_seed_host_points_to_cluster_with_name(String seedHost, String clusterName)
      throws Throwable {
    TestContext.addSeedHostToClusterMapping(seedHost, clusterName);
  }

  @Given("^cluster \"([^\"]*)\" has keyspace \"([^\"]*)\" with tables \"([^\"]*)\"$")
  public void cluster_has_keyspace_with_tables(String clusterName, String keyspace,
                                               String tablesListStr) throws Throwable {
    Set<String> tables =
        Sets.newHashSet(CommonTools.COMMA_SEPARATED_LIST_SPLITTER.split(tablesListStr));
    TestContext.addClusterInfo(clusterName, keyspace, tables);
  }

  @Given("^ccm cluster \"([^\"]*)\" has keyspace \"([^\"]*)\" with tables \"([^\"]*)\"$")
  public void ccm_cluster_has_keyspace_with_tables(String clusterName, String keyspace,
                                               String tablesListStr) throws Throwable {
    Set<String> tables =
        Sets.newHashSet(CommonTools.COMMA_SEPARATED_LIST_SPLITTER.split(tablesListStr));
    TestUtils.getInstance().createKeyspace(keyspace);
    tables.stream().forEach(tableName -> TestUtils.getInstance().createTable(keyspace, tableName));
    TestContext.addClusterInfo(clusterName, keyspace, tables);
  }

  @Given("^that we are going to use \"([^\"]*)\" as cluster seed host$")
  public void that_we_are_going_to_use_as_cluster_seed_host(String seedHost) throws Throwable {
    TestContext.SEED_HOST = seedHost;
  }

  @And("^reaper has no cluster with name \"([^\"]*)\" in storage$")
  public void reaper_has_no_cluster_with_name_in_storage(String clusterName) throws Throwable {
    callAndExpect("GET", "/cluster/" + clusterName,
                  Optional.<Map<String, String>>absent(),
                  Optional.<String>absent(), Response.Status.NOT_FOUND);
  }

  @And("^reaper has no cluster in storage$")
  public void reaper_has_no_cluster_in_storage() throws Throwable {
      RUNNERS.parallelStream().forEach(runner -> {
        Response response =
            runner.callReaper("GET", "/cluster/",
                      Optional.<Map<String, String>>absent());
        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        String responseData = response.readEntity(String.class);
        List<String> clusterNames = SimpleReaperClient.parseClusterNameListJSON(responseData);
        assertEquals(clusterNames.size(), 0);
      });
  }

  @When("^an add-cluster request is made to reaper$")
  public void an_add_cluster_request_is_made_to_reaper() throws Throwable {
    final StringBuffer responseData = new StringBuffer();

    RUNNERS.parallelStream().forEach(runner -> {
        Map<String, String> params = Maps.newHashMap();
        params.put("seedHost", TestContext.SEED_HOST);
        /*callAndExpect("POST", "/cluster", Optional.of(params), Response.Status.CREATED,
                      Optional.<String>absent());*/

        Response response = runner.callReaper("POST", "/cluster", Optional.of(params));
        if (1 < RUNNERS.size()) {
            System.out.println("POST /cluster : " +  response.getEntity().toString());
            Assertions.assertThat(ImmutableList.of(
                    Response.Status.CREATED.getStatusCode(),
                    Response.Status.FORBIDDEN.getStatusCode()))
                .contains(response.getStatus());
        } else {
            assertEquals(Response.Status.CREATED.getStatusCode(), response.getStatus());
        }
        if (Response.Status.CREATED.getStatusCode() == response.getStatus()) {
            responseData.append(response.readEntity(String.class));
            Map<String, Object>  cluster = SimpleReaperClient.parseClusterStatusJSON(responseData.toString());
            TestContext.TEST_CLUSTER = (String) cluster.get("name");
        }
    });

    Assertions.assertThat(responseData).isNotEmpty();

    callAndExpect("GET", "/cluster/" + TestContext.TEST_CLUSTER,
                  Optional.<Map<String, String>>absent(),
                  Optional.<String>absent(), Response.Status.OK);
  }

  @Then("^reaper has a cluster called \"([^\"]*)\" in storage$")
  public void reaper_has_a_cluster_called_in_storage(String clusterName) throws Throwable {
    callAndExpect("GET", "/cluster/" + clusterName,
                  Optional.<Map<String, String>>absent(),
                  Optional.<String>absent(), Response.Status.OK);
  }

  @Then("^reaper has the last added cluster in storage$")
  public void reaper_has_the_last_added_cluster_in_storage() throws Throwable {
    callAndExpect("GET", "/cluster/" + TestContext.TEST_CLUSTER,
                  Optional.<Map<String, String>>absent(),
                  Optional.<String>absent(), Response.Status.OK);
  }

  @And("^reaper has no scheduled repairs for \"([^\"]*)\"$")
  public void reaper_has_no_scheduled_repairs_for(String clusterName) throws Throwable {
    callAndExpect("GET", "/repair_schedule/cluster/" + clusterName,
                  Optional.<Map<String, String>>absent(),
                  Optional.of("[]"), Response.Status.OK);
  }

  @When("^a new daily repair schedule is added for \"([^\"]*)\" and keyspace \"([^\"]*)\"$")
  public void a_new_daily_repair_schedule_is_added_for(String clusterName, String keyspace)
      throws Throwable {

    ReaperTestJettyRunner runner = RUNNERS.get(RAND.nextInt(RUNNERS.size()));
    Map<String, String> params = Maps.newHashMap();
    params.put("clusterName", clusterName);
    params.put("keyspace", keyspace);
    params.put("owner", TestContext.TEST_USER);
    params.put("intensity", "0.9");
    params.put("scheduleDaysBetween", "1");
    Response response =
        runner.callReaper("POST", "/repair_schedule", Optional.of(params));
    assertEquals(Response.Status.CREATED.getStatusCode(), response.getStatus());
    String responseData = response.readEntity(String.class);
    RepairScheduleStatus schedule = SimpleReaperClient.parseRepairScheduleStatusJSON(responseData);
    TestContext.LAST_MODIFIED_ID = schedule.getId();
  }

  @When("^a new daily repair schedule is added for the last added cluster and keyspace \"([^\"]*)\"$")
  public void a_new_daily_repair_schedule_is_added_for_the_last_added_cluster(String keyspace)
      throws Throwable {

    ReaperTestJettyRunner runner = RUNNERS.get(RAND.nextInt(RUNNERS.size()));
    Map<String, String> params = Maps.newHashMap();
    params.put("clusterName", TestContext.TEST_CLUSTER);
    params.put("keyspace", keyspace);
    params.put("owner", TestContext.TEST_USER);
    params.put("intensity", "0.9");
    params.put("scheduleDaysBetween", "1");
    Response response =
        runner.callReaper("POST", "/repair_schedule", Optional.of(params));
    assertEquals(Response.Status.CREATED.getStatusCode(), response.getStatus());
    String responseData = response.readEntity(String.class);
    RepairScheduleStatus schedule = SimpleReaperClient.parseRepairScheduleStatusJSON(responseData);
    TestContext.LAST_MODIFIED_ID = schedule.getId();
  }

  @When("^a new daily repair schedule is added for the last added cluster and keyspace \"([^\"]*)\" with next repair immediately$")
  public void a_new_daily_repair_schedule_is_added_for_the_last_added_cluster_and_keyspace_with_next_repair_immediately(
          String keyspace)
          throws Throwable {

    ReaperTestJettyRunner runner = RUNNERS.get(RAND.nextInt(RUNNERS.size()));
    LOG.info("adding a new daily repair schedule to keyspace: {}", keyspace);
    Map<String, String> params = Maps.newHashMap();
    params.put("clusterName", TestContext.TEST_CLUSTER);
    params.put("keyspace", keyspace);
    params.put("owner", TestContext.TEST_USER);
    params.put("intensity", "0.9");
    params.put("scheduleDaysBetween", "1");
    params.put("scheduleTriggerTime", DateTime.now().plusSeconds(1).toString());
    Response response =
        runner.callReaper("POST", "/repair_schedule", Optional.of(params));
    assertEquals(Response.Status.CREATED.getStatusCode(), response.getStatus());
    String responseData = response.readEntity(String.class);
    RepairScheduleStatus schedule = SimpleReaperClient.parseRepairScheduleStatusJSON(responseData);
    TestContext.LAST_MODIFIED_ID = schedule.getId();
  }

  @And("^we wait for a scheduled repair run has started for cluster \"([^\"]*)\"$")
  public void a_scheduled_repair_run_has_started_for_cluster(String clusterName) throws Throwable {

      RUNNERS.parallelStream().forEach(runner -> {
        LOG.info("waiting for a scheduled repair run to start for cluster: {}", clusterName);
        await().with().pollInterval(10, SECONDS).atMost(2, MINUTES).until(() ->
        {
            Response response =
                runner.callReaper("GET", "/repair_run/cluster/" + TestContext.TEST_CLUSTER, EMPTY_PARAMS);

            assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
            String responseData = response.readEntity(String.class);
            List<RepairRunStatus> runs = SimpleReaperClient.parseRepairRunStatusListJSON(responseData);
            if (!runs.isEmpty()) {
                TestContext.LAST_MODIFIED_ID = runs.get(0).getId();
            }
            return !runs.isEmpty();
        });
      });
  }

  @And("^reaper has scheduled repair for cluster called \"([^\"]*)\"$")
  public void reaper_has_scheduled_repair_for_cluster_called(String clusterName) throws Throwable {
    callAndExpect("GET", "/repair_schedule/cluster/" + clusterName,
                  EMPTY_PARAMS,
                  Optional.of("\"" + clusterName + "\""), Response.Status.OK);
  }

  @And("^a second daily repair schedule is added for \"([^\"]*)\" and keyspace \"([^\"]*)\"$")
  public void a_second_daily_repair_schedule_is_added_for_and_keyspace(String clusterName,
                                                                       String keyspace)
      throws Throwable {


    ReaperTestJettyRunner runner = RUNNERS.get(RAND.nextInt(RUNNERS.size()));
    LOG.info("add second daily repair schedule: {}/{}", clusterName, keyspace);
    Map<String, String> params = Maps.newHashMap();
    params.put("clusterName", clusterName);
    params.put("keyspace", keyspace);
    params.put("owner", TestContext.TEST_USER);
    params.put("intensity", "0.8");
    params.put("scheduleDaysBetween", "1");
    Response response =
        runner.callReaper("POST", "/repair_schedule", Optional.of(params));
    assertEquals(Response.Status.CREATED.getStatusCode(), response.getStatus());
    String responseData = response.readEntity(String.class);
    RepairScheduleStatus schedule = SimpleReaperClient.parseRepairScheduleStatusJSON(responseData);
    TestContext.LAST_MODIFIED_ID = schedule.getId();
  }

  @And("^reaper has (\\d+) scheduled repairs for cluster called \"([^\"]*)\"$")
  public void reaper_has_scheduled_repairs_for_cluster_called(int repairAmount,
                                                              String clusterName)
      throws Throwable {

      CLIENTS.parallelStream().forEach(client -> {
        List<RepairScheduleStatus> schedules = client.getRepairSchedulesForCluster(clusterName);
        LOG.info("Got " + schedules.size() + " schedules");
        assertEquals(repairAmount, schedules.size());
      });
  }


  @And("^reaper has (\\d+) scheduled repairs for the last added cluster$")
  public void reaper_has_scheduled_repairs_for_the_last_added_cluster(int repairAmount)
      throws Throwable {

      CLIENTS.parallelStream().forEach(client -> {
        List<RepairScheduleStatus> schedules = client.getRepairSchedulesForCluster(TestContext.TEST_CLUSTER);
        LOG.info("Got " + schedules.size() + " schedules");
        assertEquals(repairAmount, schedules.size());
      });
  }

  @When("^the last added schedule is deleted for cluster called \"([^\"]*)\"$")
  public void the_last_added_schedule_is_deleted_for_cluster_called(String clusterName)
      throws Throwable {
    LOG.info("pause last added repair schedule with id: {}", TestContext.LAST_MODIFIED_ID);
    Map<String, String> params = Maps.newHashMap();
    params.put("state", "paused");

    callAndExpect(
            "PUT",
            "/repair_schedule/" + TestContext.LAST_MODIFIED_ID,
            Optional.of(params),
            Optional.of("\"" + clusterName + "\""),
            Response.Status.OK);

    LOG.info("delete last added repair schedule with id: {}", TestContext.LAST_MODIFIED_ID);
    params.clear();
    params.put("owner", TestContext.TEST_USER);

    callAndExpect("DELETE",
            "/repair_schedule/" + TestContext.LAST_MODIFIED_ID,
            Optional.of(params),
            Optional.of("\"" + clusterName + "\""),
            Response.Status.OK, Response.Status.NOT_FOUND);
  }

  @When("^the last added schedule is deleted for the last added cluster$")
  public void the_last_added_schedule_is_deleted_for_the_last_added_cluster()
      throws Throwable {
    LOG.info("pause last added repair schedule with id: {}", TestContext.LAST_MODIFIED_ID);
    Map<String, String> params = Maps.newHashMap();
    params.put("state", "paused");

    callAndExpect(
            "PUT",
            "/repair_schedule/" + TestContext.LAST_MODIFIED_ID,
            Optional.of(params), 
            Optional.of("\"" + TestContext.TEST_CLUSTER + "\""),
            Response.Status.OK, Response.Status.NOT_MODIFIED);

    LOG.info("delete last added repair schedule with id: {}", TestContext.LAST_MODIFIED_ID);
    params.clear();
    params.put("owner", TestContext.TEST_USER);

    callAndExpect(
            "DELETE",
            "/repair_schedule/" + TestContext.LAST_MODIFIED_ID,
            Optional.of(params),
            Optional.of("\"" + TestContext.TEST_CLUSTER + "\""),
            Response.Status.OK, Response.Status.NOT_FOUND);
  }

  @When("^all added schedules are deleted for the last added cluster$")
  public void all_added_schedules_are_deleted_for_the_last_added_cluster()
      throws Throwable {

      RUNNERS.parallelStream().forEach(runner -> {
        Response response =
                runner.callReaper("GET", "/repair_schedule/cluster/" + TestContext.TEST_CLUSTER, EMPTY_PARAMS);

        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        String responseData = response.readEntity(String.class);
        SimpleReaperClient.parseRepairScheduleStatusListJSON(responseData).stream().forEach(schedule -> {
              LOG.info("pause last added repair schedule with id: {}", schedule.getId());
              Map<String, String> params = Maps.newHashMap();
              params.put("state", "paused");

              callAndExpect(
                      "PUT",
                      "/repair_schedule/" + schedule.getId(),
                      Optional.of(params), 
                      Optional.of("\"" + TestContext.TEST_CLUSTER + "\""),
                      Response.Status.OK, Response.Status.NOT_MODIFIED, Response.Status.NOT_FOUND);

              LOG.info("delete last added repair schedule with id: {}", schedule.getId());
              params.clear();
              params.put("owner", TestContext.TEST_USER);

              callAndExpect(
                      "DELETE",
                      "/repair_schedule/" + schedule.getId(),
                      Optional.of(params),
                      Optional.of("\"" + TestContext.TEST_CLUSTER + "\""),
                      Response.Status.OK, Response.Status.NOT_FOUND);
          });
      });
  }

  @And("^deleting cluster called \"([^\"]*)\" fails$")
  public void deleting_cluster_called_fails(String clusterName) throws Throwable {
    callAndExpect(
            "DELETE",
            "/cluster/" + clusterName,
            EMPTY_PARAMS, 
            Optional.of("\"" + clusterName + "\""),
            Response.Status.FORBIDDEN);
  }

  @And("^deleting the last added cluster fails$")
  public void deleting_the_last_added_cluster_fails() throws Throwable {
    callAndExpect(
            "DELETE",
            "/cluster/" + TestContext.TEST_CLUSTER,
            EMPTY_PARAMS, 
            Optional.of("\"" + TestContext.TEST_CLUSTER + "\""),
            Response.Status.FORBIDDEN);
  }

  @And("^cluster called \"([^\"]*)\" is deleted$")
  public void cluster_called_is_deleted(String clusterName) throws Throwable {
    callAndExpect(
            "DELETE",
            "/cluster/" + clusterName,
            EMPTY_PARAMS,
            Optional.<String>absent(),
            Response.Status.OK, Response.Status.NOT_FOUND);

    callAndExpect(
            "DELETE",
            "/cluster/" + clusterName,
            EMPTY_PARAMS,
            Optional.<String>absent(),
            Response.Status.NOT_FOUND);
  }

  @And("^the last added cluster is deleted$")
  public void cluster_called_is_deleted() throws Throwable {
    callAndExpect(
            "DELETE",
            "/cluster/" + TestContext.TEST_CLUSTER,
            EMPTY_PARAMS,
            Optional.<String>absent(),
            Response.Status.OK, Response.Status.NOT_FOUND);

    callAndExpect(
            "DELETE",
            "/cluster/" + TestContext.TEST_CLUSTER,
            EMPTY_PARAMS,
            Optional.<String>absent(),
            Response.Status.NOT_FOUND);
  }

  @Then("^reaper has no cluster called \"([^\"]*)\" in storage$")
  public void reaper_has_no_cluster_called_in_storage(String clusterName) throws Throwable {
    callAndExpect(
            "GET",
            "/cluster/" + clusterName,
            Optional.<Map<String, String>>absent(),
            Optional.<String>absent(),
            Response.Status.NOT_FOUND);
  }


  @Then("^reaper has no longer the last added cluster in storage$")
  public void reaper_has_no_longer_the_last_added_cluster_in_storage() throws Throwable {
    callAndExpect(
            "GET",
            "/cluster/" + TestContext.TEST_CLUSTER,
            Optional.<Map<String, String>>absent(),
            Optional.<String>absent(),
            Response.Status.NOT_FOUND);
  }

  @And("^a new repair is added for \"([^\"]*)\" and keyspace \"([^\"]*)\"$")
  public void a_new_repair_is_added_for_and_keyspace(String clusterName, String keyspace)
      throws Throwable {

    ReaperTestJettyRunner runner = RUNNERS.get(RAND.nextInt(RUNNERS.size()));
    Map<String, String> params = Maps.newHashMap();
    params.put("clusterName", clusterName);
    params.put("keyspace", keyspace);
    params.put("owner", TestContext.TEST_USER);
    Response response =
        runner.callReaper("POST", "/repair_run", Optional.of(params));
    assertEquals(Response.Status.CREATED.getStatusCode(), response.getStatus());
    String responseData = response.readEntity(String.class);
    RepairRunStatus run = SimpleReaperClient.parseRepairRunStatusJSON(responseData);
    TestContext.LAST_MODIFIED_ID = run.getId();
  }

  @When("^a new repair is added for the last added cluster and keyspace \"([^\"]*)\"$")
  public void a_new_repair_is_added_for_the_last_added_cluster_and_keyspace(String keyspace)
      throws Throwable {

    ReaperTestJettyRunner runner = RUNNERS.get(RAND.nextInt(RUNNERS.size()));
    Map<String, String> params = Maps.newHashMap();
    params.put("clusterName", TestContext.TEST_CLUSTER);
    params.put("keyspace", keyspace);
    params.put("owner", TestContext.TEST_USER);
    Response response =
        runner.callReaper("POST", "/repair_run", Optional.of(params));
    assertEquals(Response.Status.CREATED.getStatusCode(), response.getStatus());
    String responseData = response.readEntity(String.class);
    RepairRunStatus run = SimpleReaperClient.parseRepairRunStatusJSON(responseData);
    TestContext.LAST_MODIFIED_ID = run.getId();
  }

  @When("^a new incremental repair is added for \"([^\"]*)\" and keyspace \"([^\"]*)\"$")
  public void a_new_incremental_repair_is_added_for_and_keyspace(String clusterName, String keyspace)
      throws Throwable {

    ReaperTestJettyRunner runner = RUNNERS.get(RAND.nextInt(RUNNERS.size()));
    Map<String, String> params = Maps.newHashMap();
    params.put("clusterName", clusterName);
    params.put("keyspace", keyspace);
    params.put("owner", TestContext.TEST_USER);
    params.put("incrementalRepair", Boolean.TRUE.toString());
    Response response =
        runner.callReaper("POST", "/repair_run", Optional.of(params));
    assertEquals(Response.Status.CREATED.getStatusCode(), response.getStatus());
    String responseData = response.readEntity(String.class);
    RepairRunStatus run = SimpleReaperClient.parseRepairRunStatusJSON(responseData);
    TestContext.LAST_MODIFIED_ID = run.getId();
  }

  @When("^a new incremental repair is added for the last added cluster and keyspace \"([^\"]*)\"$")
  public void a_new_incremental_repair_is_added_for_the_last_added_cluster_and_keyspace(String keyspace)
      throws Throwable {

    ReaperTestJettyRunner runner = RUNNERS.get(RAND.nextInt(RUNNERS.size()));
    Map<String, String> params = Maps.newHashMap();
    params.put("clusterName", TestContext.TEST_CLUSTER);
    params.put("keyspace", keyspace);
    params.put("owner", TestContext.TEST_USER);
    params.put("incrementalRepair", Boolean.TRUE.toString());
    Response response =
        runner.callReaper("POST", "/repair_run", Optional.of(params));
    assertEquals(Response.Status.CREATED.getStatusCode(), response.getStatus());
    String responseData = response.readEntity(String.class);
    RepairRunStatus run = SimpleReaperClient.parseRepairRunStatusJSON(responseData);
    TestContext.LAST_MODIFIED_ID = run.getId();
  }

  @Then("^reaper has (\\d+) repairs for cluster called \"([^\"]*)\"$")
  public void reaper_has_repairs_for_cluster_called(int runAmount, String clusterName)
      throws Throwable {

      RUNNERS.parallelStream().forEach(runner -> {
        Response response =
            runner.callReaper("GET", "/repair_run/cluster/" + clusterName,
                                             EMPTY_PARAMS);
        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        String responseData = response.readEntity(String.class);
        List<RepairRunStatus> runs = SimpleReaperClient.parseRepairRunStatusListJSON(responseData);
        assertEquals(runAmount, runs.size());
      });
  }

  @Then("^reaper has (\\d+) repairs for the last added cluster$")
  public void reaper_has_repairs_for_the_last_added_cluster(int runAmount)
      throws Throwable {

      RUNNERS.parallelStream().forEach(runner -> {
        Response response =
            runner.callReaper("GET", "/repair_run/cluster/" + TestContext.TEST_CLUSTER,
                                             EMPTY_PARAMS);
        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        String responseData = response.readEntity(String.class);
        List<RepairRunStatus> runs = SimpleReaperClient.parseRepairRunStatusListJSON(responseData);
        assertEquals(runAmount, runs.size());
      });
  }

  @When("^the last added repair run is deleted$")
  public void the_last_added_repair_run_is_deleted_for_cluster_called() throws Throwable {

    LOG.info("delete last added repair run with id: {}", TestContext.LAST_MODIFIED_ID);
    Map<String, String> params = Maps.newHashMap();
    params.put("owner", TestContext.TEST_USER);

    callAndExpect(
            "DELETE",
            "/repair_run/" + TestContext.LAST_MODIFIED_ID,
            Optional.of(params),
            Optional.of("\"" + TestContext.TEST_CLUSTER + "\""),
            Response.Status.OK, Response.Status.NOT_FOUND);
  }

 @When("^a new daily repair schedule is added that already exists for \"([^\"]*)\" and keyspace \"([^\"]*)\"$")
  public void a_new_daily_repair_schedule_is_added_that_already_exists_for(String clusterName, String keyspace)
      throws Throwable {

      RUNNERS.parallelStream().forEach(runner -> {
        Map<String, String> params = Maps.newHashMap();
        params.put("clusterName", clusterName);
        params.put("keyspace", keyspace);
        params.put("owner", TestContext.TEST_USER);
        params.put("intensity", "0.9");
        params.put("scheduleDaysBetween", "1");
        Response response =
            runner.callReaper("POST", "/repair_schedule", Optional.of(params));
        assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
      });
  }

 @And("^the last added repair is activated$")
 public void the_last_added_repair_is_activated_for()
     throws Throwable {

      final AtomicBoolean set = new AtomicBoolean(false);
      RUNNERS.parallelStream().forEach(runner -> {
        
        Response response = runner.callReaper(
                "PUT",
                "/repair_run/" + TestContext.LAST_MODIFIED_ID + "?state=RUNNING",
                Optional.of(Maps.newHashMap()));

        if (1 < RUNNERS.size()) {
            Assertions
                .assertThat(ImmutableList.of(
                    Response.Status.OK.getStatusCode(),
                    Response.Status.NOT_MODIFIED.getStatusCode()))
                .contains(response.getStatus());
        } else {
            assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        }

        if (Response.Status.OK.getStatusCode() == response.getStatus()) {
            String responseData = response.readEntity(String.class);
            RepairRunStatus run = SimpleReaperClient.parseRepairRunStatusJSON(responseData);
            TestContext.LAST_MODIFIED_ID = run.getId();
            set.compareAndSet(false, true);
        }
      });
      Assertions.assertThat(set.get());
 }

 @When("^the last added repair is stopped$")
 public void the_last_added_repair_is_stopped_for() throws Throwable {
// given "state" is same as the current run state
      RUNNERS.parallelStream().forEach(runner -> {
        Map<String, String> params = Maps.newHashMap();
        
        Response response = runner.callReaper(
                "PUT",
                "/repair_run/" + TestContext.LAST_MODIFIED_ID + "?state=PAUSED",
                Optional.of(params));

        if (1 < RUNNERS.size()) {
            Assertions.assertThat(ImmutableList.of(
                    Response.Status.OK.getStatusCode(),
                    Response.Status.NOT_MODIFIED.getStatusCode()))
                .contains(response.getStatus());
        } else {
            assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        }

        if (Response.Status.OK.getStatusCode() == response.getStatus()) {
            String responseData = response.readEntity(String.class);
            RepairRunStatus run = SimpleReaperClient.parseRepairRunStatusJSON(responseData);
            TestContext.LAST_MODIFIED_ID = run.getId();
        }
      });
 }

 @And("^we wait for at least (\\d+) segments to be repaired$")
 public void we_wait_for_at_least_segments_to_be_repaired(int nbSegmentsToBeRepaired) throws Throwable {

      RUNNERS.parallelStream().forEach(runner -> {
        await().with().pollInterval(10, SECONDS).atMost(2, MINUTES).until(() ->
        {
          Response response = runner.callReaper("GET", "/repair_run/" + TestContext.LAST_MODIFIED_ID, EMPTY_PARAMS);
          assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
          String responseData = response.readEntity(String.class);
          RepairRunStatus run = SimpleReaperClient.parseRepairRunStatusJSON(responseData);
          return nbSegmentsToBeRepaired == run.getSegmentsRepaired();
        });
      });
 }

}
