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

package io.cassandrareaper;

import io.cassandrareaper.core.DiagEventSubscription;
import io.cassandrareaper.core.DroppedMessages;
import io.cassandrareaper.core.MetricsHistogram;
import io.cassandrareaper.core.RepairSegment;
import io.cassandrareaper.core.Snapshot;
import io.cassandrareaper.core.ThreadPoolStat;
import io.cassandrareaper.resources.view.RepairRunStatus;
import io.cassandrareaper.resources.view.RepairScheduleStatus;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a simple client for testing usage, that calls the Reaper REST API and turns the resulting
 * JSON into Reaper core entity instances.
 */
public final class SimpleReaperClient {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleReaperClient.class);
  private static final Optional<Map<String, String>> EMPTY_PARAMS = Optional.empty();

  private final String reaperHost;
  private final int reaperPort;

  public SimpleReaperClient(String reaperHost, int reaperPort) {
    this.reaperHost = reaperHost;
    this.reaperPort = reaperPort;
  }

  public static Response doHttpCall(
      String httpMethod,
      String host,
      int port,
      String urlPath,
      Optional<Map<String, String>> params) {

    String reaperBase = "http://" + host.toLowerCase() + ":" + port + "/";
    URI uri;
    try {
      uri = new URL(new URL(reaperBase), urlPath).toURI();
    } catch (MalformedURLException | URISyntaxException ex) {
      throw new RuntimeException(ex);
    }

    Client client = ClientBuilder.newClient();
    client.property("http.autoredirect", true);
    WebTarget webTarget = client.target(uri);

    LOG.info("calling (" + httpMethod + ") Reaper in resource: " + webTarget.getUri());
    Form form = new Form();
    if ("POSTFORM".equalsIgnoreCase(httpMethod)) {
      for (Map.Entry<String, String> entry : params.get().entrySet()) {
        form.param(entry.getKey(), entry.getValue());
      }
    } else if (params.isPresent()) {
      for (Map.Entry<String, String> entry : params.get().entrySet()) {
        webTarget = webTarget.queryParam(entry.getKey(), entry.getValue());
      }
    }

    Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);

    Response response;
    if ("GET".equalsIgnoreCase(httpMethod)) {
      response = invocationBuilder.get();
    } else if ("HEAD".equalsIgnoreCase(httpMethod)) {
      response = invocationBuilder.head();
    } else if ("POST".equalsIgnoreCase(httpMethod)) {
      response = invocationBuilder.post(null);
    } else if ("POSTFORM".equalsIgnoreCase(httpMethod)) {
      response = invocationBuilder.post(Entity.form(form));
    } else if ("PUT".equalsIgnoreCase(httpMethod)) {
      response = invocationBuilder.put(Entity.entity("", MediaType.APPLICATION_JSON));
    } else if ("DELETE".equalsIgnoreCase(httpMethod)) {
      response = invocationBuilder.delete();
    } else if ("OPTIONS".equalsIgnoreCase(httpMethod)) {
      response = invocationBuilder.options();
    } else {
      throw new RuntimeException("Invalid HTTP method: " + httpMethod);
    }

    return response;
  }

  private static <T> T parseJSON(String json, TypeReference<T> ref) {
    try {
      return new ObjectMapper().registerModule(new JodaModule()).readValue(json, ref);
    } catch (IOException e) {
      LOG.error("error parsing json", e);
      throw new RuntimeException(e);
    }
  }

  public static List<RepairScheduleStatus> parseRepairScheduleStatusListJSON(String json) {
    return parseJSON(json, new TypeReference<List<RepairScheduleStatus>>() {});
  }

  public static RepairScheduleStatus parseRepairScheduleStatusJSON(String json) {
    return parseJSON(json, new TypeReference<RepairScheduleStatus>() {});
  }

  public static List<RepairRunStatus> parseRepairRunStatusListJSON(String json) {
    return parseJSON(json, new TypeReference<List<RepairRunStatus>>() {});
  }

  public static RepairRunStatus parseRepairRunStatusJSON(String json) {
    return parseJSON(json, new TypeReference<RepairRunStatus>() {});
  }

  public static Map<String, Object> parseClusterStatusJSON(String json) {
    return parseJSON(json, new TypeReference<Map<String, Object>>() {});
  }

  public static List<String> parseClusterNameListJSON(String json) {
    return parseJSON(json, new TypeReference<List<String>>() {});
  }

  public static Map<String, List<String>> parseTableListJSON(String json) {
    return parseJSON(json, new TypeReference<Map<String, List<String>>>() {});
  }

  public static List<RepairSegment> parseRepairSegmentsJSON(String json) {
    return parseJSON(json, new TypeReference<List<RepairSegment>>() {});
  }

  public List<RepairScheduleStatus> getRepairSchedulesForCluster(String clusterName) {
    Response response =
        doHttpCall(
            "GET", reaperHost, reaperPort, "/repair_schedule/cluster/" + clusterName, EMPTY_PARAMS);
    assertEquals(200, response.getStatus());
    String responseData = response.readEntity(String.class);
    return parseRepairScheduleStatusListJSON(responseData);
  }

  public List<RepairScheduleStatus> getRepairSchedulesForClusterAndKs(
      String clusterName, String keyspaceName) {
    Map<String, String> params = Maps.newHashMap();
    if (!clusterName.isEmpty()) {
      params.put("clusterName", clusterName);
    }
    if (!keyspaceName.isEmpty()) {
      params.put("keyspace", keyspaceName);
    }
    Response response =
        doHttpCall("GET", reaperHost, reaperPort, "/repair_schedule", Optional.of(params));
    assertEquals(200, response.getStatus());
    String responseData = response.readEntity(String.class);
    return parseRepairScheduleStatusListJSON(responseData);
  }

  public static Map<String, List<Snapshot>> parseSnapshotMapJSON(String json) {
    return parseJSON(json, new TypeReference<Map<String, List<Snapshot>>>() {});
  }

  public static Map<String, Map<String, List<Snapshot>>> parseClusterSnapshotMapJSON(String json) {
    return parseJSON(json, new TypeReference<Map<String, Map<String, List<Snapshot>>>>() {});
  }

  public static List<ThreadPoolStat> parseTpStatJSON(String json) {
    return parseJSON(json, new TypeReference<List<ThreadPoolStat>>() {});
  }

  public static List<DroppedMessages> parseDroppedMessagesJSON(String json) {
    return parseJSON(json, new TypeReference<List<DroppedMessages>>() {});
  }

  public static List<MetricsHistogram> parseClientRequestMetricsJSON(String json) {
    return parseJSON(json, new TypeReference<List<MetricsHistogram>>() {});
  }

  public static List<String> parseTokenListJSON(String json) {
    return parseJSON(json, new TypeReference<List<String>>() {});
  }

  public static List<DiagEventSubscription> parseEventSubscriptionsListJSON(String json) {
    return parseJSON(json, new TypeReference<List<DiagEventSubscription>>() {});
  }

  public static DiagEventSubscription parseEventSubscriptionJSON(String json) {
    return parseJSON(json, new TypeReference<DiagEventSubscription>() {});
  }
}
