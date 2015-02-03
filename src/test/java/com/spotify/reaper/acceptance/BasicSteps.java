package com.spotify.reaper.acceptance;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;

import java.util.Map;

import javax.ws.rs.core.Response;

import cucumber.api.java.Before;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;

/**
 * Basic acceptance test (Cucumber) steps.
 */
public class BasicSteps {

  @Before
  public static void setup() throws Exception {
    ReaperTestJettyRunner.setup();
  }

  @Given("^a reaper service is running$")
  public void a_reaper_service_is_running() throws Throwable {
    ReaperTestJettyRunner.callAndExpect(
        "GET", "/ping", Optional.<Map<String, String>>absent(), Response.Status.OK);
  }

  @Given("^that we are going to use \"([^\"]*)\" as cluster seed host$")
  public void that_we_are_going_to_use_as_cluster_seed_host(String seedHost) throws Throwable {
    TestContext.SEED_HOST = seedHost;
  }

  @And("^reaper has no cluster with name \"([^\"]*)\" in storage$")
  public void reaper_has_no_cluster_with_name_in_storage(String clusterName) throws Throwable {
    ReaperTestJettyRunner.callAndExpect(
        "GET", "/cluster/" + clusterName,
        Optional.<Map<String, String>>absent(), Response.Status.NOT_FOUND);
  }

  @When("^an add-cluster request is made to reaper$")
  public void an_add_cluster_request_is_made_to_reaper() throws Throwable {
    Map<String, String> params = Maps.newHashMap();
    params.put("seedHost", TestContext.SEED_HOST);
    ReaperTestJettyRunner.callAndExpect(
        "POST", "/cluster", Optional.of(params), Response.Status.CREATED);
  }

  @Then("^reaper has a cluster called \"([^\"]*)\" in storage$")
  public void reaper_has_a_cluster_called_in_storage(String clusterName) throws Throwable {
    ReaperTestJettyRunner.callAndExpect(
        "GET", "/cluster/" + clusterName,
        Optional.<Map<String, String>>absent(), Response.Status.OK.getStatusCode());
  }

}
