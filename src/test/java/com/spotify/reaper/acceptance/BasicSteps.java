package com.spotify.reaper.acceptance;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.spotify.reaper.AppContext;
import com.spotify.reaper.ReaperException;
import com.spotify.reaper.cassandra.JmxConnectionFactory;
import com.spotify.reaper.cassandra.JmxProxy;
import com.spotify.reaper.cassandra.RepairStatusHandler;
import com.sun.jersey.api.client.ClientResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;

import javax.ws.rs.core.Response;

import cucumber.api.PendingException;
import cucumber.api.java.Before;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Basic acceptance test (Cucumber) steps.
 */
public class BasicSteps {

  private static final Logger LOG = LoggerFactory.getLogger(BasicSteps.class);

  @Before
  public static void setup() throws Exception {
    LOG.info("running testing setup at @Before annotated method");
    AppContext context = new AppContext();
    context.jmxConnectionFactory = new JmxConnectionFactory() {
      @Override
      public JmxProxy connect(Optional<RepairStatusHandler> handler, String host)
          throws ReaperException {
        JmxProxy jmx = mock(JmxProxy.class);
        when(jmx.getClusterName()).thenReturn("testcluster");
        when(jmx.getKeyspaces()).thenReturn(Arrays.asList("testkeyspace", "system"));
        when(jmx.getTableNamesForKeyspace("testkeyspace")).thenReturn(
            Sets.newHashSet("testtable", "othertesttable"));
        return jmx;
      }
    };
    ReaperTestJettyRunner.setup(context);
  }

  public void callAndExpect(String httpMethod, String callPath,
                            Optional<Map<String, String>> params, Response.Status expectedStatus,
                            Optional<String> expectedDataInResponseData) {
    ClientResponse response = ReaperTestJettyRunner.callReaper(httpMethod, callPath, params);
    String responseData = response.getEntity(String.class);
    LOG.info("Got response data: " + responseData);
    if (expectedDataInResponseData.isPresent()) {
      assertTrue("expected data not found from the response: " + expectedDataInResponseData.get(),
                 null != responseData && responseData.contains(expectedDataInResponseData.get()));
      LOG.debug("Data \"" + expectedDataInResponseData.get() + "\" was found from response data");
    }
    assertEquals(expectedStatus.getStatusCode(), response.getStatus());
  }

  @Given("^a reaper service is running$")
  public void a_reaper_service_is_running() throws Throwable {
    callAndExpect("GET", "/ping", Optional.<Map<String, String>>absent(),
                  Response.Status.OK, Optional.<String>absent());
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
    params.put("owner", "test_user");
    params.put("intensity", "0.9");
    params.put("scheduleDaysBetween", "1");
    callAndExpect("POST", "/repair_schedule", Optional.of(params), Response.Status.CREATED,
                  Optional.of("\"" + keyspace + "\""));
  }

  @And("^reaper has scheduled repair for cluster called \"([^\"]*)\"$")
  public void reaper_has_scheduled_repair_for_cluster_called(String clusterName) throws Throwable {
    callAndExpect("GET", "/repair_schedule/cluster/" + clusterName,
                  Optional.<Map<String, String>>absent(), Response.Status.OK,
                  Optional.of("\"" + clusterName + "\""));
  }

}
