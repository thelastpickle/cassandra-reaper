package com.spotify.reaper;

import com.google.common.base.Optional;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.reaper.resources.view.RepairRunStatus;
import com.spotify.reaper.resources.view.RepairScheduleStatus;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * This is a simple client for testing usage, that calls the Reaper REST API
 * and turns the resulting JSON into Reaper core entity instances.
 */
public class SimpleReaperClient {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleReaperClient.class);

  private static Optional<Map<String, String>> EMPTY_PARAMS = Optional.absent();

  public static ClientResponse doHttpCall(String httpMethod, String host, int port, String urlPath,
                                          Optional<Map<String, String>> params) {
    String reaperBase = "http://" + host.toLowerCase() + ":" + port + "/";
    URI uri;
    try {
      uri = new URL(new URL(reaperBase), urlPath).toURI();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    Client client = new Client();
    WebResource resource = client.resource(uri);
    LOG.info("calling (" + httpMethod + ") Reaper in resource: " + resource.getURI());
    if (params.isPresent()) {
      for (Map.Entry<String, String> entry : params.get().entrySet()) {
        resource = resource.queryParam(entry.getKey(), entry.getValue());
      }
    }
    ClientResponse response;
    if ("GET".equalsIgnoreCase(httpMethod)) {
      response = resource.get(ClientResponse.class);
    } else if ("POST".equalsIgnoreCase(httpMethod)) {
      response = resource.post(ClientResponse.class);
    } else if ("PUT".equalsIgnoreCase(httpMethod)) {
      response = resource.put(ClientResponse.class);
    } else if ("DELETE".equalsIgnoreCase(httpMethod)) {
      response = resource.delete(ClientResponse.class);
    } else if ("OPTIONS".equalsIgnoreCase(httpMethod)) {
      response = resource.options(ClientResponse.class);
    } else {
      throw new RuntimeException("Invalid HTTP method: " + httpMethod);
    }
    return response;
  }

  private static <T> T parseJSON(String json, TypeReference<T> ref) {
    T parsed;
    ObjectMapper mapper = new ObjectMapper();
    try {
      parsed = mapper.readValue(json, ref);
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    return parsed;
  }

  public static List<RepairScheduleStatus> parseRepairScheduleStatusListJSON(String json) {
    return parseJSON(json, new TypeReference<List<RepairScheduleStatus>>() {
    });
  }

  public static RepairScheduleStatus parseRepairScheduleStatusJSON(String json) {
    return parseJSON(json, new TypeReference<RepairScheduleStatus>() {
    });
  }

  public static List<RepairRunStatus> parseRepairRunStatusListJSON(String json) {
    return parseJSON(json, new TypeReference<List<RepairRunStatus>>() {
    });
  }

  public static RepairRunStatus parseRepairRunStatusJSON(String json) {
    return parseJSON(json, new TypeReference<RepairRunStatus>() {
    });
  }

  private String reaperHost;
  private int reaperPort;

  public SimpleReaperClient(String reaperHost, int reaperPort) {
    this.reaperHost = reaperHost;
    this.reaperPort = reaperPort;
  }

  public List<RepairScheduleStatus> getRepairSchedulesForCluster(String clusterName) {
    ClientResponse response = doHttpCall("GET", reaperHost, reaperPort,
                                         "/repair_schedule/cluster/" + clusterName, EMPTY_PARAMS);
    assertEquals(200, response.getStatus());
    String responseData = response.getEntity(String.class);
    return parseRepairScheduleStatusListJSON(responseData);
  }

}
