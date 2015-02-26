package com.spotify.reaper.acceptance;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.spotify.reaper.AppContext;
import com.spotify.reaper.ReaperException;
import com.spotify.reaper.SimpleReaperClient;
import com.spotify.reaper.cassandra.JmxConnectionFactory;
import com.spotify.reaper.cassandra.JmxProxy;
import com.spotify.reaper.resources.CommonTools;
import com.spotify.reaper.resources.view.RepairRunStatus;
import com.spotify.reaper.resources.view.RepairScheduleStatus;
import com.sun.jersey.api.client.ClientResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.core.Response;

import cucumber.api.PendingException;
import cucumber.api.java.Before;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Basic acceptance test (Cucumber) steps.
 */
public class BasicSteps {

  private static final Logger LOG = LoggerFactory.getLogger(BasicSteps.class);

  private static Optional<Map<String, String>> EMPTY_PARAMS = Optional.absent();

  private static SimpleReaperClient client;

  @Before
  public static void setup() {
    // actual setup is done in setupReaperTestRunner step
  }

  public static void setupReaperTestRunner() throws Exception {
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
      for (String keyspace : clusterKeyspaces.keySet()) {
        when(jmx.getTableNamesForKeyspace(keyspace)).thenReturn(clusterKeyspaces.get(keyspace));
      }
      when(context.jmxConnectionFactory.connect(org.mockito.Matchers.<Optional>any(), eq(seedHost)))
          .thenReturn(jmx);
    }
    ReaperTestJettyRunner.setup(context);
    client = ReaperTestJettyRunner.getClient();
  }

  public void callAndExpect(String httpMethod, String callPath,
                            Optional<Map<String, String>> params, Response.Status expectedStatus,
                            Optional<String> expectedDataInResponseData) throws ReaperException {
    ClientResponse response = ReaperTestJettyRunner.callReaper(httpMethod, callPath, params);
    String responseData = response.getEntity(String.class);
    LOG.info("Got response data: " + responseData);
    assertEquals(expectedStatus.getStatusCode(), response.getStatus());
    if (expectedDataInResponseData.isPresent()) {
      assertTrue("expected data not found from the response: " + expectedDataInResponseData.get(),
                 null != responseData && responseData.contains(expectedDataInResponseData.get()));
      LOG.debug("Data \"" + expectedDataInResponseData.get() + "\" was found from response data");
    }
  }

  @Given("^a reaper service is running$")
  public void a_reaper_service_is_running() throws Throwable {
    setupReaperTestRunner();
    callAndExpect("GET", "/ping", Optional.<Map<String, String>>absent(),
                  Response.Status.OK, Optional.<String>absent());
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

  @Given("^that we are going to use \"([^\"]*)\" as cluster seed host$")
  public void that_we_are_going_to_use_as_cluster_seed_host(String seedHost) throws Throwable {
    TestContext.SEED_HOST = seedHost;
  }

  @And("^reaper has no cluster with name \"([^\"]*)\" in storage$")
  public void reaper_has_no_cluster_with_name_in_storage(String clusterName) throws Throwable {
    callAndExpect("GET", "/cluster/" + clusterName,
                  Optional.<Map<String, String>>absent(), Response.Status.NOT_FOUND,
                  Optional.<String>absent());
  }

  @When("^an add-cluster request is made to reaper$")
  public void an_add_cluster_request_is_made_to_reaper() throws Throwable {
    Map<String, String> params = Maps.newHashMap();
    params.put("seedHost", TestContext.SEED_HOST);
    callAndExpect("POST", "/cluster", Optional.of(params), Response.Status.CREATED,
                  Optional.<String>absent());
  }

  @Then("^reaper has a cluster called \"([^\"]*)\" in storage$")
  public void reaper_has_a_cluster_called_in_storage(String clusterName) throws Throwable {
    callAndExpect("GET", "/cluster/" + clusterName,
                  Optional.<Map<String, String>>absent(), Response.Status.OK,
                  Optional.<String>absent());
  }

  @And("^reaper has no scheduled repairs for \"([^\"]*)\"$")
  public void reaper_has_no_scheduled_repairs_for(String clusterName) throws Throwable {
    callAndExpect("GET", "/repair_schedule/cluster/" + clusterName,
                  Optional.<Map<String, String>>absent(), Response.Status.OK,
                  Optional.of("[]"));
  }

  @When("^a new daily repair schedule is added for \"([^\"]*)\" and keyspace \"([^\"]*)\"$")
  public void a_new_daily_repair_schedule_is_added_for(String clusterName, String keyspace)
      throws Throwable {
    Map<String, String> params = Maps.newHashMap();
    params.put("clusterName", clusterName);
    params.put("keyspace", keyspace);
    params.put("owner", TestContext.TEST_USER);
    params.put("intensity", "0.9");
    params.put("scheduleDaysBetween", "1");
    ClientResponse response =
        ReaperTestJettyRunner.callReaper("POST", "/repair_schedule", Optional.of(params));
    assertEquals(Response.Status.CREATED.getStatusCode(), response.getStatus());
    String responseData = response.getEntity(String.class);
    RepairScheduleStatus schedule = SimpleReaperClient.parseRepairScheduleStatusJSON(responseData);
    TestContext.LAST_MODIFIED_ID = schedule.getId();
  }

  @And("^reaper has scheduled repair for cluster called \"([^\"]*)\"$")
  public void reaper_has_scheduled_repair_for_cluster_called(String clusterName) throws Throwable {
    callAndExpect("GET", "/repair_schedule/cluster/" + clusterName,
                  EMPTY_PARAMS, Response.Status.OK,
                  Optional.of("\"" + clusterName + "\""));
  }

  @And("^a second daily repair schedule is added for \"([^\"]*)\" and keyspace \"([^\"]*)\"$")
  public void a_second_daily_repair_schedule_is_added_for_and_keyspace(String clusterName,
                                                                       String keyspace)
      throws Throwable {
    LOG.info("add second daily repair schedule: {}/{}", clusterName, keyspace);
    Map<String, String> params = Maps.newHashMap();
    params.put("clusterName", clusterName);
    params.put("keyspace", keyspace);
    params.put("owner", TestContext.TEST_USER);
    params.put("intensity", "0.8");
    params.put("scheduleDaysBetween", "1");
    ClientResponse response =
        ReaperTestJettyRunner.callReaper("POST", "/repair_schedule", Optional.of(params));
    assertEquals(Response.Status.CREATED.getStatusCode(), response.getStatus());
    String responseData = response.getEntity(String.class);
    RepairScheduleStatus schedule = SimpleReaperClient.parseRepairScheduleStatusJSON(responseData);
    TestContext.LAST_MODIFIED_ID = schedule.getId();
  }

  @And("^reaper has (\\d+) scheduled repairs for cluster called \"([^\"]*)\"$")
  public void reaper_has_scheduled_repairs_for_cluster_called(int repairAmount,
                                                              String clusterName)
      throws Throwable {
    List<RepairScheduleStatus> schedules = client.getRepairSchedulesForCluster(clusterName);
    LOG.info("Got " + schedules.size() + " schedules");
    assertEquals(repairAmount, schedules.size());
  }

  @When("^the last added schedule is deleted for cluster called \"([^\"]*)\"$")
  public void the_last_added_schedule_is_deleted_for_cluster_called(String clusterName)
      throws Throwable {
    LOG.info("pause last added repair schedule with id: {}", TestContext.LAST_MODIFIED_ID);
    Map<String, String> params = Maps.newHashMap();
    params.put("state", "paused");
    callAndExpect("PUT", "/repair_schedule/" + TestContext.LAST_MODIFIED_ID,
                  Optional.of(params), Response.Status.OK, Optional.of("\"" + clusterName + "\""));

    LOG.info("delete last added repair schedule with id: {}", TestContext.LAST_MODIFIED_ID);
    params.clear();
    params.put("owner", TestContext.TEST_USER);
    callAndExpect("DELETE", "/repair_schedule/" + TestContext.LAST_MODIFIED_ID,
                  Optional.of(params), Response.Status.OK, Optional.of("\"" + clusterName + "\""));
  }

  @And("^deleting cluster called \"([^\"]*)\" fails$")
  public void deleting_cluster_called_fails(String clusterName) throws Throwable {
    callAndExpect("DELETE", "/cluster/" + clusterName,
                  EMPTY_PARAMS, Response.Status.FORBIDDEN, Optional.of("\"" + clusterName + "\""));
  }

  @And("^cluster called \"([^\"]*)\" is deleted$")
  public void cluster_called_is_deleted(String clusterName) throws Throwable {
    callAndExpect("DELETE", "/cluster/" + clusterName,
                  EMPTY_PARAMS, Response.Status.OK, Optional.of("\"" + clusterName + "\""));
  }

  @Then("^reaper has no cluster called \"([^\"]*)\" in storage$")
  public void reaper_has_no_cluster_called_in_storage(String clusterName) throws Throwable {
    callAndExpect("GET", "/cluster/" + clusterName,
                  Optional.<Map<String, String>>absent(), Response.Status.NOT_FOUND,
                  Optional.<String>absent());
  }

  @And("^a new repair is added for \"([^\"]*)\" and keyspace \"([^\"]*)\"$")
  public void a_new_repair_is_added_for_and_keyspace(String clusterName, String keyspace)
      throws Throwable {
    Map<String, String> params = Maps.newHashMap();
    params.put("clusterName", clusterName);
    params.put("keyspace", keyspace);
    params.put("owner", TestContext.TEST_USER);
    ClientResponse response =
        ReaperTestJettyRunner.callReaper("POST", "/repair_run", Optional.of(params));
    assertEquals(Response.Status.CREATED.getStatusCode(), response.getStatus());
    String responseData = response.getEntity(String.class);
    RepairRunStatus run = SimpleReaperClient.parseRepairRunStatusJSON(responseData);
    TestContext.LAST_MODIFIED_ID = run.getId();
  }

  @Then("^reaper has (\\d+) repairs for cluster called \"([^\"]*)\"$")
  public void reaper_has_repairs_for_cluster_called(int runAmount, String clusterName)
      throws Throwable {
    ClientResponse response =
        ReaperTestJettyRunner.callReaper("GET", "/repair_run/cluster/" + clusterName,
                                         EMPTY_PARAMS);
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    String responseData = response.getEntity(String.class);
    List<RepairRunStatus> runs = SimpleReaperClient.parseRepairRunStatusListJSON(responseData);
    assertEquals(runAmount, runs.size());
  }

  @When("^the last added repair run is deleted for cluster called \"([^\"]*)\"$")
  public void the_last_added_repair_run_is_deleted_for_cluster_called(String clusterName)
      throws Throwable {
    LOG.info("delete last added repair run with id: {}", TestContext.LAST_MODIFIED_ID);
    Map<String, String> params = Maps.newHashMap();
    params.put("owner", TestContext.TEST_USER);
    callAndExpect("DELETE", "/repair_run/" + TestContext.LAST_MODIFIED_ID,
                  Optional.of(params), Response.Status.OK, Optional.of("\"" + clusterName + "\""));
  }
}
