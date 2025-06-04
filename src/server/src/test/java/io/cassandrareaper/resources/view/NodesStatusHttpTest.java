/*
 * Copyright 2017-2017 Spotify AB
 * Copyright 2017-2018 The Last Pickle Ltd
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

import io.cassandrareaper.management.http.HttpCassandraManagementProxy;

import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.mgmtapi.client.model.EndpointStates;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

/**
 * Covers {@link NodesStatus#NodesStatus(String, EndpointStates)}, which takes input data retrieved
 * via the HTTP API.
 *
 * @see HttpCassandraManagementProxy#getNodesStatus()
 */
public class NodesStatusHttpTest {

  @Test
  public void testRegularValues() {
    testSingleNode(
        ImmutableMap.<String, String>builder()
            .put("DC", "dc1")
            .put("RACK", "rack1")
            .put("ENDPOINT_IP", "10.0.0.1")
            .put("IS_ALIVE", "true")
            .put("STATUS_WITH_PORT", "NORMAL,-1181245519517832414")
            .put("SEVERITY", "1.0")
            .put("RELEASE_VERSION", "4.0.0")
            .put("HOST_ID", "061cef57-4cfc-40d5-9671-5ff412f7b292")
            .put("TOKENS", "-1181245519517832414,-2148609762596367912")
            .put("LOAD", "2.0")
            .build(),
        "dc1",
        "rack1",
        endpointState -> {
          assertThat(endpointState.endpoint).isEqualTo("10.0.0.1");
          assertThat(endpointState.status).isEqualTo("NORMAL - UP");
          assertThat(endpointState.dc).isEqualTo("dc1");
          assertThat(endpointState.rack).isEqualTo("rack1");
          assertThat(endpointState.severity).isEqualTo(1.0);
          assertThat(endpointState.releaseVersion).isEqualTo("4.0.0");
          assertThat(endpointState.hostId).isEqualTo("061cef57-4cfc-40d5-9671-5ff412f7b292");
          assertThat(endpointState.tokens).isEqualTo("-1181245519517832414,-2148609762596367912");
          assertThat(endpointState.type).isEqualTo(NodesStatus.NodeType.CASSANDRA);
        });
  }

  @Test
  public void testMissingValues() {
    testSingleNode(
        Collections.emptyMap(),
        "Not available",
        "Not available",
        endpointState -> {
          assertThat(endpointState.endpoint).isEqualTo("Not available");
          assertThat(endpointState.status).isEqualTo("Not available");
          assertThat(endpointState.dc).isEqualTo("Not available");
          assertThat(endpointState.rack).isEqualTo("Not available");
          assertThat(endpointState.severity).isEqualTo(0.0);
          assertThat(endpointState.releaseVersion).isEqualTo("Not available");
          assertThat(endpointState.hostId).isEqualTo("Not available");
          assertThat(endpointState.tokens).isEqualTo("Not available");
          assertThat(endpointState.type).isEqualTo(NodesStatus.NodeType.CASSANDRA);
        });
  }

  @Test
  public void testSimpleStateUnknown() {
    testSingleNode(
        ImmutableMap.<String, String>builder()
            .put("DC", "dc1")
            .put("RACK", "rack1")
            // missing IS_ALIVE
            .put("STATUS_WITH_PORT", "NORMAL,-1181245519517832414")
            .build(),
        "dc1",
        "rack1",
        endpointState -> assertThat(endpointState.status).isEqualTo("NORMAL - UNKNOWN"));
  }

  @Test
  public void testFallbackToStatus() {
    testSingleNode(
        ImmutableMap.<String, String>builder()
            .put("DC", "dc1")
            .put("RACK", "rack1")
            .put("IS_ALIVE", "true")
            // missing STATUS_WITH_PORT, but STATUS present:
            .put("STATUS", "NORMAL,-1181245519517832414")
            .build(),
        "dc1",
        "rack1",
        endpointState -> assertThat(endpointState.status).isEqualTo("NORMAL - UP"));
  }

  @Test
  public void testMalformedDoubles() {
    testSingleNode(
        ImmutableMap.<String, String>builder()
            .put("DC", "dc1")
            .put("RACK", "rack1")
            .put("SEVERITY", "foo")
            .put("LOAD", "bar")
            .build(),
        "dc1",
        "rack1",
        endpointState -> {
          assertThat(endpointState.severity).isEqualTo(0.0);
          assertThat(endpointState.load).isEqualTo(0.0);
        });
  }

  @Test
  public void testStargateNode() {
    testSingleNode(
        ImmutableMap.<String, String>builder()
            .put("DC", "dc1")
            .put("RACK", "rack1")
            .put("X10", "stargate")
            .build(),
        "dc1",
        "rack1",
        endpointState -> assertThat(endpointState.type).isEqualTo(NodesStatus.NodeType.STARGATE));
  }

  @Test
  public void testX10NotStargateNode() {
    testSingleNode(
        ImmutableMap.<String, String>builder()
            .put("DC", "dc1")
            .put("RACK", "rack1")
            .put("X10", "something else")
            .build(),
        "dc1",
        "rack1",
        endpointState -> assertThat(endpointState.type).isEqualTo(NodesStatus.NodeType.CASSANDRA));
  }

  private void testSingleNode(
      Map<String, String> input,
      String expectedDc,
      String expectedRack,
      Consumer<NodesStatus.EndpointState> requirements) {
    EndpointStates endpointStates = new EndpointStates();
    endpointStates.addEntityItem(input);

    NodesStatus nodesStatus = new NodesStatus("mockSourceNode", endpointStates);

    assertThat(nodesStatus.endpointStates).hasSize(1);
    NodesStatus.GossipInfo gossipInfo = nodesStatus.endpointStates.get(0);
    assertThat(gossipInfo.sourceNode).isEqualTo("mockSourceNode");
    assertThat(gossipInfo.endpoints).containsOnlyKeys(expectedDc);
    assertThat(gossipInfo.endpoints.get(expectedDc)).containsOnlyKeys(expectedRack);

    assertThat(gossipInfo.endpoints.get(expectedDc).get(expectedRack).get(0))
        .satisfies(requirements);
  }

  @Test
  public void testRemovedNodes() {
    EndpointStates endpointStates = new EndpointStates();
    endpointStates.addEntityItem(ImmutableMap.of("STATUS_WITH_PORT", "LEFT"));
    endpointStates.addEntityItem(ImmutableMap.of("STATUS_WITH_PORT", "REMOVED"));

    NodesStatus nodesStatus = new NodesStatus("mockSourceNode", endpointStates);

    assertThat(nodesStatus.endpointStates).hasSize(1);
    NodesStatus.GossipInfo gossipInfo = nodesStatus.endpointStates.get(0);
    assertThat(gossipInfo.endpoints).isEmpty();
  }

  @Test
  public void testMultiNodes() {
    EndpointStates endpointStates = new EndpointStates();
    endpointStates.addEntityItem(
        ImmutableMap.of(
            "DC", "dc1",
            "RACK", "rack1",
            "ENDPOINT_IP", "10.0.0.1",
            "LOAD", "1.0"));
    endpointStates.addEntityItem(
        ImmutableMap.of(
            "DC", "dc1",
            "RACK", "rack1",
            "ENDPOINT_IP", "10.0.0.2",
            "LOAD", "2.0"));
    endpointStates.addEntityItem(
        ImmutableMap.of(
            "DC", "dc1",
            "RACK", "rack2",
            "ENDPOINT_IP", "10.0.0.3",
            "LOAD", "3.0"));
    endpointStates.addEntityItem(
        ImmutableMap.of(
            "DC", "dc2",
            "RACK", "rack1",
            "ENDPOINT_IP", "10.0.0.4",
            "LOAD", "4.0"));

    NodesStatus nodesStatus = new NodesStatus("mockSourceNode", endpointStates);

    assertThat(nodesStatus.endpointStates).hasSize(1);
    NodesStatus.GossipInfo gossipInfo = nodesStatus.endpointStates.get(0);

    assertThat(gossipInfo.sourceNode).isEqualTo("mockSourceNode");
    assertThat(gossipInfo.totalLoad).isEqualTo(10.0);
    assertThat(gossipInfo.endpointNames)
        .containsOnly("10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.4");

    assertThat(gossipInfo.endpoints).containsOnlyKeys("dc1", "dc2");
    assertThat(gossipInfo.endpoints.get("dc1")).containsOnlyKeys("rack1", "rack2");
    assertThat(gossipInfo.endpoints.get("dc1").get("rack1"))
        .extracting(s -> s.endpoint)
        .containsOnly("10.0.0.1", "10.0.0.2");
    assertThat(gossipInfo.endpoints.get("dc1").get("rack2"))
        .extracting(s -> s.endpoint)
        .containsOnly("10.0.0.3");
    assertThat(gossipInfo.endpoints.get("dc2")).containsOnlyKeys("rack1");
    assertThat(gossipInfo.endpoints.get("dc2").get("rack1"))
        .extracting(s -> s.endpoint)
        .containsOnly("10.0.0.4");
  }
}
