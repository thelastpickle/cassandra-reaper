/*
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import jersey.repackaged.com.google.common.collect.Lists;
import jersey.repackaged.com.google.common.collect.Maps;

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

  private static final Pattern ENDPOINT_NAME_PATTERN
      = Pattern.compile("^([0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3})", Pattern.MULTILINE | Pattern.DOTALL);
  private static final Pattern ENDPOINT_STATUS_22_PATTERN = Pattern.compile("(STATUS):([0-9]*):(\\w+)");
  private static final Pattern ENDPOINT_DC_22_PATTERN = Pattern.compile("(DC):([0-9]*):([0-9a-zA-Z-\\.]+)");
  private static final Pattern ENDPOINT_RACK_22_PATTERN = Pattern.compile("(RACK):([0-9]*):([0-9a-zA-Z-\\.]+)");
  private static final Pattern ENDPOINT_LOAD_22_PATTERN = Pattern.compile("(LOAD):([0-9]*):([0-9eE.]+)");
  private static final Pattern ENDPOINT_RELEASE_22_PATTERN = Pattern.compile("(RELEASE_VERSION):([0-9]*):([0-9.]+)");
  private static final Pattern ENDPOINT_SEVERITY_22_PATTERN = Pattern.compile("(SEVERITY):([0-9]*):([0-9.]+)");
  private static final Pattern ENDPOINT_HOSTID_22_PATTERN = Pattern.compile("(HOST_ID):([0-9]*):([0-9a-z-]+)");
  private static final Pattern ENDPOINT_TOKENS_22_PATTERN = Pattern.compile("(TOKENS):([0-9]*)");
  private static final Pattern ENDPOINT_STATUS_21_PATTERN = Pattern.compile("(STATUS)(:)(\\w+)");
  private static final Pattern ENDPOINT_DC_21_PATTERN = Pattern.compile("(DC)(:)([0-9a-zA-Z-\\.]+)");
  private static final Pattern ENDPOINT_RACK_21_PATTERN = Pattern.compile("(RACK)(:)([0-9a-zA-Z-\\.]+)");
  private static final Pattern ENDPOINT_LOAD_21_PATTERN = Pattern.compile("(LOAD)(:)([0-9eE.]+)");
  private static final Pattern ENDPOINT_RELEASE_21_PATTERN = Pattern.compile("(RELEASE_VERSION)(:)([0-9.]+)");
  private static final Pattern ENDPOINT_SEVERITY_21_PATTERN = Pattern.compile("(SEVERITY)(:)([0-9.]+)");
  private static final Pattern ENDPOINT_HOSTID_21_PATTERN = Pattern.compile("(HOST_ID)(:)([0-9a-z-]+)");

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

  private GossipInfo parseEndpointStatesString(
      String sourceNode,
      String allEndpointStates,
      Map<String, String> simpleStates) {

    List<EndpointState> endpointStates = Lists.newArrayList();
    Set<String> endpoints = Sets.newHashSet();
    Matcher matcher;

    String[] strEndpoints = allEndpointStates.split("(?<![0-9a-zA-Z ])/");
    Double totalLoad = 0.0;

    for (int i = 1; i < strEndpoints.length; i++) {
      String endpointString = strEndpoints[i];
      Optional<String> status = Optional.absent();
      Optional<String> endpoint = parseEndpointState(ENDPOINT_NAME_PATTERNS, endpointString, 1, String.class);

      for (Pattern endpointStatusPattern : ENDPOINT_STATUS_PATTERNS) {
        matcher = endpointStatusPattern.matcher(endpointString);
        if (matcher.find() && matcher.groupCount() >= 3) {
          status = Optional.of(matcher.group(3) + " - " + simpleStates.getOrDefault("/" + endpoint.or(""), "UNKNOWN"));
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
      totalLoad += load.or(0.0);

      EndpointState endpointState = new EndpointState(
          endpoint.or(NOT_AVAILABLE),
          hostId.or(NOT_AVAILABLE),
          dc.or(NOT_AVAILABLE),
          rack.or(NOT_AVAILABLE),
          status.or(NOT_AVAILABLE),
          severity.or(0.0),
          releaseVersion.or(NOT_AVAILABLE),
          tokens.or(NOT_AVAILABLE),
          load.or(0.0));

      endpoints.add(endpoint.or(NOT_AVAILABLE));
      endpointStates.add(endpointState);
    }

    Map<String, Map<String, List<EndpointState>>> endpointsByDcAndRack = Maps.newHashMap();
    Map<String, List<EndpointState>> endpointsByDc
        = endpointStates.stream().collect(Collectors.groupingBy(EndpointState::getDc, Collectors.toList()));

    for (String dc : endpointsByDc.keySet()) {
      Map<String, List<EndpointState>> endpointsByRack
          = endpointsByDc.get(dc).stream().collect(Collectors.groupingBy(EndpointState::getRack, Collectors.toList()));
      endpointsByDcAndRack.put(dc, endpointsByRack);
    }

    return new GossipInfo(sourceNode, endpointsByDcAndRack, totalLoad, endpoints);
  }

  private <T> Optional<T> parseEndpointState(List<Pattern> patterns, String endpointString, int group, Class<T> type) {
    Optional<T> result = Optional.absent();
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

  private static void initPatterns() {
    ENDPOINT_NAME_PATTERNS.add(ENDPOINT_NAME_PATTERN);
    ENDPOINT_STATUS_PATTERNS.addAll(Arrays.asList(ENDPOINT_STATUS_22_PATTERN, ENDPOINT_STATUS_21_PATTERN));
    ENDPOINT_DC_PATTERNS.addAll(Arrays.asList(ENDPOINT_DC_22_PATTERN, ENDPOINT_DC_21_PATTERN));
    ENDPOINT_RACK_PATTERNS.addAll(Arrays.asList(ENDPOINT_RACK_22_PATTERN, ENDPOINT_RACK_21_PATTERN));
    ENDPOINT_LOAD_PATTERNS.addAll(Arrays.asList(ENDPOINT_LOAD_22_PATTERN, ENDPOINT_LOAD_21_PATTERN));
    ENDPOINT_RELEASE_PATTERNS.addAll(Arrays.asList(ENDPOINT_RELEASE_22_PATTERN, ENDPOINT_RELEASE_21_PATTERN));
    ENDPOINT_SEVERITY_PATTERNS.addAll(Arrays.asList(ENDPOINT_SEVERITY_22_PATTERN, ENDPOINT_SEVERITY_21_PATTERN));
    ENDPOINT_HOSTID_PATTERNS.addAll(Arrays.asList(ENDPOINT_HOSTID_22_PATTERN, ENDPOINT_HOSTID_21_PATTERN));
    ENDPOINT_TOKENS_PATTERNS.add(ENDPOINT_TOKENS_22_PATTERN);
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

    public EndpointState(
        String endpoint,
        String hostId,
        String dc,
        String rack,
        String status,
        Double severity,
        String releaseVersion,
        String tokens,
        Double load) {

      this.endpoint = endpoint;
      this.hostId = hostId;
      this.dc = dc;
      this.rack = rack;
      this.status = status;
      this.severity = severity;
      this.releaseVersion = releaseVersion;
      this.tokens = tokens;
      this.load = load;
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
          + tokens;
    }
  }
}
