/*
 * Copyright 2017-2017 Spotify AB
 * Copyright 2017-2019 The Last Pickle Ltd
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

package io.cassandrareaper.resources.view;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.datastax.mgmtapi.client.model.EndpointStates;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public final class NodesStatus {

  private static final List<Pattern> ENDPOINT_NAME_PATTERNS = Lists.newArrayList();
  private static final List<Pattern> ENDPOINT_STATUS_PATTERNS = Lists.newArrayList();
  private static final List<Pattern> ENDPOINT_DC_PATTERNS = Lists.newArrayList();
  private static final List<Pattern> ENDPOINT_RACK_PATTERNS = Lists.newArrayList();
  private static final List<Pattern> ENDPOINT_LOAD_PATTERNS = Lists.newArrayList();
  private static final List<Pattern> ENDPOINT_RELEASE_PATTERNS = Lists.newArrayList();
  private static final List<Pattern> ENDPOINT_SEVERITY_PATTERNS = Lists.newArrayList();
  private static final List<Pattern> ENDPOINT_HOSTID_PATTERNS = Lists.newArrayList();
  private static final List<Pattern> ENDPOINT_TOKENS_PATTERNS = Lists.newArrayList();
  private static final List<Pattern> ENDPOINT_TYPE_PATTERNS = Lists.newArrayList();

  private static final Pattern ENDPOINT_NAME_PATTERN_IP4
      = Pattern.compile("^([0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3})", Pattern.MULTILINE | Pattern.DOTALL);
  private static final Pattern ENDPOINT_NAME_PATTERN_IP6
      = Pattern.compile("^([0-9:a-fA-F\\]\\[]{3,41})", Pattern.MULTILINE | Pattern.DOTALL);
  private static final Pattern ENDPOINT_STATUS_22_PATTERN = Pattern.compile("(STATUS):([0-9]*):(\\w+)");
  private static final Pattern ENDPOINT_STATUS_40_PATTERN = Pattern.compile("(STATUS_WITH_PORT):([0-9]*):(\\w+)");
  private static final Pattern ENDPOINT_DC_22_PATTERN = Pattern.compile("(DC):([0-9]*):([0-9a-zA-Z-_\\.]+)");
  private static final Pattern ENDPOINT_RACK_22_PATTERN = Pattern.compile("(RACK):([0-9]*):([0-9a-zA-Z-_\\.]+)");
  private static final Pattern ENDPOINT_LOAD_22_PATTERN = Pattern.compile("(LOAD):([0-9]*):([0-9eE.]+)");
  private static final Pattern ENDPOINT_RELEASE_22_PATTERN = Pattern.compile("(RELEASE_VERSION):([0-9]*):([0-9.]+)");
  private static final Pattern ENDPOINT_SEVERITY_22_PATTERN = Pattern.compile("(SEVERITY):([0-9]*):([0-9.]+)");
  private static final Pattern ENDPOINT_HOSTID_22_PATTERN = Pattern.compile("(HOST_ID):([0-9]*):([0-9a-z-]+)");
  private static final Pattern ENDPOINT_TOKENS_22_PATTERN = Pattern.compile("(TOKENS):([0-9]*)");
  private static final Pattern ENDPOINT_STATUS_21_PATTERN = Pattern.compile("(STATUS)(:)(\\w+)");
  private static final Pattern ENDPOINT_DC_21_PATTERN = Pattern.compile("(DC)(:)([0-9a-zA-Z-_\\.]+)");
  private static final Pattern ENDPOINT_RACK_21_PATTERN = Pattern.compile("(RACK)(:)([0-9a-zA-Z-_\\.]+)");
  private static final Pattern ENDPOINT_LOAD_21_PATTERN = Pattern.compile("(LOAD)(:)([0-9eE.]+)");
  private static final Pattern ENDPOINT_RELEASE_21_PATTERN = Pattern.compile("(RELEASE_VERSION)(:)([0-9.]+)");
  private static final Pattern ENDPOINT_SEVERITY_21_PATTERN = Pattern.compile("(SEVERITY)(:)([0-9.]+)");
  private static final Pattern ENDPOINT_HOSTID_21_PATTERN = Pattern.compile("(HOST_ID)(:)([0-9a-z-]+)");
  private static final Pattern ENDPOINT_LOAD_SCYLLA_44_PATTERN = Pattern.compile("(LOAD)(:)([0-9eE.\\+]+)");
  private static final Pattern ENDPOINT_TYPE_STARGATE_PATTERN = Pattern.compile("(X10):([0-9]*):(stargate)");

  private static final String NOT_AVAILABLE = "Not available";

  @JsonProperty
  public final List<GossipInfo> endpointStates;

  static {
    initPatterns();
  }

  public NodesStatus(List<GossipInfo> endpointStates) {
    this.endpointStates = endpointStates;
  }

  public NodesStatus(String sourceNode, String allEndpointStates, Map<String, String> simpleStates) {
    this.endpointStates = Lists.newArrayList();
    this.endpointStates.add(parseEndpointStatesString(sourceNode, allEndpointStates, simpleStates));
  }

  public NodesStatus(String sourceNode, EndpointStates endpointStates) {
    this.endpointStates = Lists.newArrayList();
    this.endpointStates.add(processEndpointStates(sourceNode, endpointStates));
  }

  private GossipInfo parseEndpointStatesString(
      String sourceNode,
      String allEndpointStates,
      Map<String, String> simpleStates) {

    List<EndpointState> endpointStates = Lists.newArrayList();
    Set<String> endpoints = Sets.newHashSet();
    Matcher matcher;

    // Split into endpointState record strings
    String[] endpointLines = allEndpointStates.split("\n");
    List<String> strEndpoints = Lists.newArrayList();
    StringBuilder recordBuilder = null;
    for (String line : endpointLines) {
      if (!line.startsWith("  ")) {
        if (recordBuilder != null) {
          strEndpoints.add(recordBuilder.toString());
        }
        recordBuilder = new StringBuilder(line.substring(line.indexOf('/') + 1));
      } else if (recordBuilder != null) {
        recordBuilder.append('\n');
        recordBuilder.append(line);
      }
    }
    if (recordBuilder != null) {
      strEndpoints.add(recordBuilder.toString());
    }

    // Cleanup hostnames from simpleStates keys
    Map<String, String> simpleStatesCopy = new HashMap<>();
    for (Map.Entry<String, String> entry : simpleStates.entrySet()) {
      String entryKey = "/" + entry.getKey();
      if (entry.getKey().indexOf('/') != -1) {
        entryKey = entry.getKey().substring(entry.getKey().indexOf('/'));
      }
      simpleStatesCopy.put(entryKey, entry.getValue());
    }
    simpleStates = simpleStatesCopy;

    Double totalLoad = 0.0;

    for (String endpointString : strEndpoints) {
      Optional<String> status = Optional.empty();
      Optional<String> endpoint = parseEndpointState(ENDPOINT_NAME_PATTERNS, endpointString, 1, String.class);

      for (Pattern endpointStatusPattern : ENDPOINT_STATUS_PATTERNS) {
        matcher = endpointStatusPattern.matcher(endpointString);
        if (matcher.find() && matcher.groupCount() >= 3) {
          status = Optional
              .of(matcher.group(3) + " - " + simpleStates.getOrDefault("/" + endpoint.orElse(""), "UNKNOWN"));
          break;
        }
      }

      Optional<String> dc = parseEndpointState(ENDPOINT_DC_PATTERNS, endpointString, 3, String.class);
      Optional<String> rack = parseEndpointState(ENDPOINT_RACK_PATTERNS, endpointString, 3, String.class);
      Optional<Double> severity = parseEndpointState(ENDPOINT_SEVERITY_PATTERNS, endpointString, 3, Double.class);
      Optional<String> releaseVersion = parseEndpointState(ENDPOINT_RELEASE_PATTERNS, endpointString, 3, String.class);
      Optional<String> hostId = parseEndpointState(ENDPOINT_HOSTID_PATTERNS, endpointString, 3, String.class);
      Optional<String> tokens = parseEndpointState(ENDPOINT_TOKENS_PATTERNS, endpointString, 2, String.class);
      Optional<Double> load = parseEndpointState(ENDPOINT_LOAD_PATTERNS, endpointString, 3, Double.class);
      totalLoad += load.orElse(0.0);
      Optional<String> stargate = parseEndpointState(ENDPOINT_TYPE_PATTERNS, endpointString, 3, String.class);

      EndpointState endpointState = new EndpointState(
          endpoint.orElse(NOT_AVAILABLE),
          hostId.orElse(NOT_AVAILABLE),
          dc.orElse(NOT_AVAILABLE),
          rack.orElse(NOT_AVAILABLE),
          status.orElse(NOT_AVAILABLE),
          severity.orElse(0.0),
          releaseVersion.orElse(NOT_AVAILABLE),
          tokens.orElse(NOT_AVAILABLE),
          load.orElse(0.0),
          stargate.isPresent() ? NodeType.STARGATE : NodeType.CASSANDRA);

      if (!status.orElse(NOT_AVAILABLE).toLowerCase().contains("left")
          && !status.orElse(NOT_AVAILABLE).toLowerCase().contains("removed")) {
        // Only add nodes that haven't left the cluster (they could still appear in Gossip state for a while)
        endpoints.add(endpoint.orElse(NOT_AVAILABLE));
        endpointStates.add(endpointState);
      }
    }

    return new GossipInfo(sourceNode, sortByDcAndRack(endpointStates), totalLoad, endpoints);
  }

  private <T> Optional<T> parseEndpointState(List<Pattern> patterns, String endpointString, int group, Class<T> type) {
    Optional<T> result = Optional.empty();
    for (Pattern pattern : patterns) {
      Matcher matcher = pattern.matcher(endpointString);
      if (matcher.find() && matcher.groupCount() >= group) {
        result = (Optional<T>) Optional.of(matcher.group(group));
        if (type.equals(Double.class)) {
          result = (Optional<T>) Optional.of(Double.parseDouble(matcher.group(group)));
        }
        break;
      }
    }

    return result;
  }

  private static Map<String, Map<String, List<EndpointState>>> sortByDcAndRack(List<EndpointState> endpointStates) {
    Map<String, Map<String, List<EndpointState>>> endpointsByDcAndRack = Maps.newHashMap();
    Map<String, List<EndpointState>> endpointsByDc
        = endpointStates.stream().collect(Collectors.groupingBy(EndpointState::getDc, Collectors.toList()));

    for (String dc : endpointsByDc.keySet()) {
      Map<String, List<EndpointState>> endpointsByRack
          = endpointsByDc.get(dc).stream().collect(Collectors.groupingBy(EndpointState::getRack, Collectors.toList()));
      endpointsByDcAndRack.put(dc, endpointsByRack);
    }
    return endpointsByDcAndRack;
  }

  private static GossipInfo processEndpointStates(String sourceNode, EndpointStates maps) {
    List<EndpointState> endpointStates = Lists.newArrayList();
    Set<String> endpoints = Sets.newHashSet();
    double totalLoad = 0.0;

    for (Map<String, String> map : maps.getEntity()) {
      final String endpoint = map.getOrDefault("ENDPOINT_IP", NOT_AVAILABLE);

      final String simpleState;
      if (map.containsKey("IS_ALIVE")) {
        simpleState = Boolean.parseBoolean(map.get("IS_ALIVE")) ? "UP" : "DOWN";
      } else {
        simpleState = "UNKNOWN";
      }

      final String status;
      String rawStatus = map.getOrDefault("STATUS_WITH_PORT", map.get("STATUS"));
      if (rawStatus != null) {
        if (rawStatus.contains(",")) {
          rawStatus = rawStatus.substring(0, rawStatus.indexOf(","));
        }
        status = rawStatus + " - " + simpleState;
      } else {
        status = NOT_AVAILABLE;
      }
      // Only add nodes that haven't left the cluster (they could still appear in Gossip state for a while)
      if (status.toLowerCase().contains("left") || status.toLowerCase().contains("removed")) {
        continue;
      }

      final String dc = map.getOrDefault("DC", NOT_AVAILABLE);
      final String rack = map.getOrDefault("RACK", NOT_AVAILABLE);
      double severity;
      try {
        severity = Double.parseDouble(map.getOrDefault("SEVERITY", "0"));
      } catch (NumberFormatException e) {
        severity = 0.0;
      }
      final String releaseVersion = map.getOrDefault("RELEASE_VERSION", NOT_AVAILABLE);
      final String hostId = map.getOrDefault("HOST_ID", NOT_AVAILABLE);
      final String tokens = map.getOrDefault("TOKENS", NOT_AVAILABLE);

      double load;
      try {
        load = Double.parseDouble(map.getOrDefault("LOAD", "0"));
      } catch (NumberFormatException e) {
        load = 0.0;
      }
      totalLoad += load;
      final NodeType nodeType = "stargate".equals(map.get("X10")) ? NodeType.STARGATE : NodeType.CASSANDRA;

      endpoints.add(endpoint);
      endpointStates.add(new EndpointState(
          endpoint, hostId, dc, rack, status, severity, releaseVersion, tokens, load, nodeType));
    }

    return new GossipInfo(sourceNode, sortByDcAndRack(endpointStates), totalLoad, endpoints);
  }

  private static void initPatterns() {
    ENDPOINT_NAME_PATTERNS.addAll(Arrays.asList(ENDPOINT_NAME_PATTERN_IP4, ENDPOINT_NAME_PATTERN_IP6));
    ENDPOINT_STATUS_PATTERNS.addAll(
        Arrays.asList(ENDPOINT_STATUS_40_PATTERN, ENDPOINT_STATUS_22_PATTERN, ENDPOINT_STATUS_21_PATTERN));
    ENDPOINT_DC_PATTERNS.addAll(Arrays.asList(ENDPOINT_DC_22_PATTERN, ENDPOINT_DC_21_PATTERN));
    ENDPOINT_RACK_PATTERNS.addAll(Arrays.asList(ENDPOINT_RACK_22_PATTERN, ENDPOINT_RACK_21_PATTERN));
    ENDPOINT_LOAD_PATTERNS.addAll(Arrays.asList(ENDPOINT_LOAD_22_PATTERN, ENDPOINT_LOAD_SCYLLA_44_PATTERN,
        ENDPOINT_LOAD_21_PATTERN));
    ENDPOINT_RELEASE_PATTERNS.addAll(Arrays.asList(ENDPOINT_RELEASE_22_PATTERN, ENDPOINT_RELEASE_21_PATTERN));
    ENDPOINT_SEVERITY_PATTERNS.addAll(Arrays.asList(ENDPOINT_SEVERITY_22_PATTERN, ENDPOINT_SEVERITY_21_PATTERN));
    ENDPOINT_HOSTID_PATTERNS.addAll(Arrays.asList(ENDPOINT_HOSTID_22_PATTERN, ENDPOINT_HOSTID_21_PATTERN));
    ENDPOINT_TOKENS_PATTERNS.add(ENDPOINT_TOKENS_22_PATTERN);
    ENDPOINT_TYPE_PATTERNS.add(ENDPOINT_TYPE_STARGATE_PATTERN);
  }

  public static final class GossipInfo {

    @JsonProperty
    public final String sourceNode;

    @JsonProperty
    public final Map<String, Map<String, List<EndpointState>>> endpoints;

    @JsonProperty
    public final Double totalLoad;

    @JsonProperty
    public final Set<String> endpointNames;

    public GossipInfo(
        String sourceNode,
        Map<String, Map<String, List<EndpointState>>> endpoints,
        Double totalLoad,
        Set<String> endpointNames) {

      this.sourceNode = sourceNode;
      this.endpoints = endpoints;
      this.totalLoad = totalLoad;
      this.endpointNames = endpointNames;
    }
  }

  public enum NodeType {
    CASSANDRA,
    STARGATE;
  }

  public static final class EndpointState {

    @JsonProperty
    public final String endpoint;

    @JsonProperty
    public final String hostId;

    @JsonProperty
    public final String dc;

    @JsonProperty
    public final String rack;

    @JsonProperty
    public final String status;

    @JsonProperty
    public final Double severity;

    @JsonProperty
    public final String releaseVersion;

    @JsonProperty
    public final String tokens;

    @JsonProperty
    public final Double load;

    @JsonProperty
    public final NodeType type;

    public EndpointState(
        String endpoint,
        String hostId,
        String dc,
        String rack,
        String status,
        Double severity,
        String releaseVersion,
        String tokens,
        Double load,
        NodeType type) {

      this.endpoint = endpoint;
      this.hostId = hostId;
      this.dc = dc;
      this.rack = rack;
      this.status = status;
      this.severity = severity;
      this.releaseVersion = releaseVersion;
      this.tokens = tokens;
      this.load = load;
      this.type = type;
    }

    public String getDc() {
      return this.dc;
    }

    public String getRack() {
      return this.rack;
    }

    @Override
    public String toString() {
      return "Endpoint : "
          + endpoint
          + " / "
          + "Status : "
          + status
          + " / "
          + "DC : "
          + dc
          + " / "
          + "Rack : "
          + rack
          + " / "
          + "Release version : "
          + releaseVersion
          + " / "
          + "Load : "
          + load
          + " / "
          + "Severity : "
          + severity
          + " / "
          + "Host Id : "
          + hostId
          + " / "
          + "Tokens : "
          + tokens
          + " / "
          + "Type : "
          + type;
    }
  }
}
