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

import io.cassandrareaper.resources.view.NodesStatus.EndpointState;

import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public final class NodesStatusTest {

  @Test
  public void testParseIPv4Endpoint22StatusString() {
    testParseEndpoint22StatusString(
        "127.0.0.1",
        "/127.0.0.1",
        "/127.0.0.2",
        "/127.0.0.3");
  }

  @Test
  public void testParseIPv4Endpoint22WithHostnameStatusString() {
    testParseEndpoint22StatusString(
        "127.0.0.1",
        "localhost/127.0.0.1",
        "/127.0.0.2",
        "localhost3/127.0.0.3");
  }

  @Test
  public void testParseIPv6Endpoint22StatusString() {
    testParseEndpoint22StatusString(
        "/::1",
        "/2a02:6b8f:b010:61aa:34b3:7805:1d3d:3b20",
        "/[2a02:6b8f:b010:61aa:34b3:7805:1d3d:3b21]",
        "/2a:6:b0:61aa:34b3:7805:1d3d:32");
  }

  @Test
  public void testParseIPv6Endpoint22WithHostnameStatusString() {
    testParseEndpoint22StatusString(
        "/::1",
        "a.example.com/2a02:6b8f:b010:61aa:34b3:7805:1d3d:3b20",
        "b.example.com/[2a02:6b8f:b010:61aa:34b3:7805:1d3d:3b21]",
        "c.example.com/2a:6:b0:61aa:34b3:7805:1d3d:32");
  }

  @Test
  public void testParseIPv4Endpoint21StatusString() {
    testParseEndpoint21StatusString(
        "127.0.0.1",
        "/127.0.0.1",
        "/127.0.0.2",
        "/127.0.0.3");
  }

  @Test
  public void testParseIPv4Endpoint21WithHostnameStatusString() {
    testParseEndpoint21StatusString(
        "127.0.0.1",
        "localhost1/127.0.0.1",
        "localhost2/127.0.0.2",
        "localhost3/127.0.0.3");
  }

  @Test
  public void testParseIPv6Endpoint21StatusString() {
    testParseEndpoint21StatusString(
        "::1",
        "/::1",
        "/[2a02:6b8f:b010:61aa:34b3:7805:1d3d:3b20]",
        "/2a:6:b0:61aa:34b3:7805:1d3d:32");
  }

  @Test
  public void testParseIPv6Endpoint21WithHostnameStatusString() {
    testParseEndpoint21StatusString(
        "::1",
        "localhost/::1",
        "a.example.com/[2a02:6b8f:b010:61aa:34b3:7805:1d3d:3b20]",
        "b.example.com/2a:6:b0:61aa:34b3:7805:1d3d:32");
  }

  @Test
  public void testParseIPv4EndpointCassandraStatusString() {
    testParseEndpointCassandraStatusString(
        "10.0.0.1",
        "/10.0.0.1",
        "/10.0.0.2",
        "/10.0.0.3");
  }

  @Test
  public void testParseIPv4EndpointCassandraWithHostnameStatusString() {
    testParseEndpointCassandraStatusString(
        "10.0.0.1",
        "one/10.0.0.1",
        "two/10.0.0.2",
        "three/10.0.0.3");
  }

  @Test
  public void testParseIPv6EndpointCassandraStatusString() {
    testParseEndpointCassandraStatusString(
        "/::1",
        "/2a02:6b8f:b010:61aa:34b3:7805:1d3d:3b20",
        "/[2a02:6b8f:b010:61aa:34b3:7805:1d3d:3b21]",
        "/2a:6:b0:61aa:34b3:7805:1d3d:32");
  }

  @Test
  public void testParseIPv6EndpointCassandraWithHostnameStatusString() {
    testParseEndpointCassandraStatusString(
        "/::1",
        "a.example.com/2a02:6b8f:b010:61aa:34b3:7805:1d3d:3b20",
        "b.example.com/[2a02:6b8f:b010:61aa:34b3:7805:1d3d:3b21]",
        "c.example.com/2a:6:b0:61aa:34b3:7805:1d3d:32");
  }

  private void testParseEndpoint22StatusString(String sourceNode,
                                               String endpoint1, String endpoint2, String endpoint3) {
    Map<String, String> simpleStates = Maps.newHashMap();

    String endpointsStatusString =
        endpoint1 + "\n"
            + "  generation:1496849190\n"
            + "  heartbeat:1231900\n"
            + "  STATUS:14:NORMAL,-9223372036854775808\n"
            + "  LOAD:1231851:4043215.0\n"
            + "  SCHEMA:10:2de0af6a-bf86-38e0-b62b-474ff6aefb51\n"
            + "  DC:6:datacenter1\n"
            + "  RACK:8:rack1\n"
            + "  RELEASE_VERSION:4:3.0.8\n"
            + "  RPC_ADDRESS:3:127.0.0.1\n"
            + "  SEVERITY:1231899:0.0\n"
            + "  NET_VERSION:1:10\n"
            + "  HOST_ID:2:f091f82b-ce2c-40ee-b30c-6e761e94e821\n"
            + "  RPC_READY:16:true\n"
            + "  TOKENS:13:<hidden>\n"
            + endpoint2 + "\n"
            + "  generation:1497347537\n"
            + "  heartbeat:27077\n"
            + "  STATUS:14:NORMAL,-3074457345618258603\n"
            + "  LOAD:1231851:3988763.0\n"
            + "  SCHEMA:10:2de0af6a-bf86-38e0-b62b-474ff6aefb51\n"
            + "  DC:6:datacenter2\n"
            + "  RACK:8:rack2\n"
            + "  RELEASE_VERSION:4:3.0.8\n"
            + "  RPC_ADDRESS:3:127.0.0.2\n"
            + "  SEVERITY:1231899:0.0\n"
            + "  NET_VERSION:1:10\n"
            + "  HOST_ID:2:08f819b5-d96f-444e-9d4d-ec4136e1b716\n"
            + "  RPC_READY:16:true\n"
            + "  TOKENS:13:<hidden>\n"
            + endpoint3 + "\n"
            + "  generation:1496849191\n"
            + "  heartbeat:1230183\n"
            + "  STATUS:16:NORMAL,3074457345618258602\n"
            + "  LOAD:1230134:3.974144E6\n"
            + "  SCHEMA:10:2de0af6a-bf86-38e0-b62b-474ff6aefb51\n"
            + "  DC:6:us-west-1\n"
            + "  RACK:8:rack3\n"
            + "  RELEASE_VERSION:4:3.0.8\n"
            + "  RPC_ADDRESS:3:127.0.0.3\n"
            + "  SEVERITY:1230182:0.0\n"
            + "  NET_VERSION:1:10\n"
            + "  HOST_ID:2:20769fed-7916-4b7a-a729-8b99bcdc9b95\n"
            + "  RPC_READY:44:true\n"
            + "  TOKENS:15:<hidden>\n";

    simpleStates.put(endpoint1, "DOWN");
    simpleStates.put(endpoint3, "UP");

    NodesStatus nodesStatus = new NodesStatus(sourceNode, endpointsStatusString, simpleStates);

    assertEquals(1, nodesStatus.endpointStates.size());
    assertEquals(sourceNode, nodesStatus.endpointStates.get(0).sourceNode);

    Map<String, Map<String, List<EndpointState>>> endpoints = nodesStatus.endpointStates.get(0).endpoints;
    assertEquals(1, endpoints.get("datacenter1").keySet().size());
    assertEquals(1, endpoints.get("datacenter1").get("rack1").size());
    assertEquals("NORMAL - DOWN", endpoints.get("datacenter1").get("rack1").get(0).status);
    assertEquals("NORMAL - UNKNOWN", endpoints.get("datacenter2").get("rack2").get(0).status);
    assertEquals("NORMAL - UP", endpoints.get("us-west-1").get("rack3").get(0).status);
    assertEquals(afterSlash(endpoint1), endpoints.get("datacenter1").get("rack1").get(0).endpoint);
    assertEquals("f091f82b-ce2c-40ee-b30c-6e761e94e821", endpoints.get("datacenter1").get("rack1").get(0).hostId);
    assertEquals("13", endpoints.get("datacenter1").get("rack1").get(0).tokens);
    assertTrue(endpoints.get("datacenter1").get("rack1").get(0).severity.equals(0.0));
    assertEquals("3.0.8", endpoints.get("datacenter1").get("rack1").get(0).releaseVersion);
    assertEquals("datacenter2", endpoints.get("datacenter2").get("rack2").get(0).dc);
    assertEquals("rack2", endpoints.get("datacenter2").get("rack2").get(0).rack);
    assertEquals("us-west-1", endpoints.get("us-west-1").get("rack3").get(0).dc);
    assertEquals("rack3", endpoints.get("us-west-1").get("rack3").get(0).rack);
    assertEquals(afterSlash(endpoint3), endpoints.get("us-west-1").get("rack3").get(0).endpoint);
    assertTrue(endpoints.get("us-west-1").get("rack3").get(0).load.equals(3974144.0));

    assertTrue(nodesStatus.endpointStates.get(0).endpointNames.contains(afterSlash(endpoint1)));
    assertTrue(nodesStatus.endpointStates.get(0).endpointNames.contains(afterSlash(endpoint2)));
    assertTrue(nodesStatus.endpointStates.get(0).endpointNames.contains(afterSlash(endpoint3)));
  }

  private void testParseEndpoint21StatusString(String sourceNode,
                                               String endpoint1, String endpoint2, String endpoint3) {
    Map<String, String> simpleStates = Maps.newHashMap();

    String endpointsStatusString =
        endpoint1 + "\n"
            + "  generation:1496849190\n"
            + "  heartbeat:1231900\n"
            + "  STATUS:NORMAL,-9223372036854775808\n"
            + "  LOAD:4043215.0\n"
            + "  SCHEMA:2de0af6a-bf86-38e0-b62b-474ff6aefb51\n"
            + "  DC:datacenter1\n"
            + "  RACK:rack1\n"
            + "  RELEASE_VERSION:3.0.8\n"
            + "  RPC_ADDRESS:127.0.0.1\n"
            + "  SEVERITY:0.0\n"
            + "  NET_VERSION:10\n"
            + "  HOST_ID:f091f82b-ce2c-40ee-b30c-6e761e94e821\n"
            + "  RPC_READY:true\n"
            + "  TOKENS:<hidden>\n"
            + endpoint2 + "\n"
            + "  generation:1497347537\n"
            + "  heartbeat:27077\n"
            + "  STATUS:NORMAL,-3074457345618258603\n"
            + "  LOAD:3988763.0\n"
            + "  SCHEMA:2de0af6a-bf86-38e0-b62b-474ff6aefb51\n"
            + "  DC:datacenter2\n"
            + "  RACK:rack2\n"
            + "  RELEASE_VERSION:3.0.8\n"
            + "  RPC_ADDRESS:127.0.0.2\n"
            + "  SEVERITY:0.0\n"
            + "  NET_VERSION:10\n"
            + "  HOST_ID:08f819b5-d96f-444e-9d4d-ec4136e1b716\n"
            + "  RPC_READY:true\n"
            + endpoint3 + "\n"
            + "  generation:1496849191\n"
            + "  heartbeat:1230183\n"
            + "  STATUS:NORMAL,3074457345618258602\n"
            + "  LOAD:3.974144E6\n"
            + "  SCHEMA:2de0af6a-bf86-38e0-b62b-474ff6aefb51\n"
            + "  DC:us-west-1\n"
            + "  RACK:rack3\n"
            + "  RELEASE_VERSION:3.0.8\n"
            + "  RPC_ADDRESS:127.0.0.3\n"
            + "  SEVERITY:0.0\n"
            + "  NET_VERSION:10\n"
            + "  HOST_ID:20769fed-7916-4b7a-a729-8b99bcdc9b95\n"
            + "  RPC_READY:true\n"
            + "  TOKENS:<hidden>\n";

    simpleStates.put(endpoint1, "DOWN");
    simpleStates.put(endpoint3, "UP");

    NodesStatus nodesStatus = new NodesStatus(sourceNode, endpointsStatusString, simpleStates);

    assertEquals(1, nodesStatus.endpointStates.size());
    assertEquals(sourceNode, nodesStatus.endpointStates.get(0).sourceNode);

    Map<String, Map<String, List<EndpointState>>> endpoints = nodesStatus.endpointStates.get(0).endpoints;
    assertEquals(1, endpoints.get("datacenter1").keySet().size());
    assertEquals(1, endpoints.get("datacenter1").get("rack1").size());
    assertEquals("NORMAL - DOWN", endpoints.get("datacenter1").get("rack1").get(0).status);
    assertEquals("NORMAL - UNKNOWN", endpoints.get("datacenter2").get("rack2").get(0).status);
    assertEquals("NORMAL - UP", endpoints.get("us-west-1").get("rack3").get(0).status);
    assertEquals(afterSlash(endpoint1), endpoints.get("datacenter1").get("rack1").get(0).endpoint);
    assertEquals("f091f82b-ce2c-40ee-b30c-6e761e94e821", endpoints.get("datacenter1").get("rack1").get(0).hostId);
    assertTrue(endpoints.get("datacenter1").get("rack1").get(0).severity.equals(0.0));
    assertEquals("3.0.8", endpoints.get("datacenter1").get("rack1").get(0).releaseVersion);
    assertEquals("datacenter2", endpoints.get("datacenter2").get("rack2").get(0).dc);
    assertEquals("rack2", endpoints.get("datacenter2").get("rack2").get(0).rack);
    assertEquals("us-west-1", endpoints.get("us-west-1").get("rack3").get(0).dc);
    assertEquals("rack3", endpoints.get("us-west-1").get("rack3").get(0).rack);
    assertEquals(afterSlash(endpoint3), endpoints.get("us-west-1").get("rack3").get(0).endpoint);
    assertTrue(endpoints.get("us-west-1").get("rack3").get(0).load.equals(3974144.0));

    assertTrue(nodesStatus.endpointStates.get(0).endpointNames.contains(afterSlash(endpoint1)));
    assertTrue(nodesStatus.endpointStates.get(0).endpointNames.contains(afterSlash(endpoint2)));
    assertTrue(nodesStatus.endpointStates.get(0).endpointNames.contains(afterSlash(endpoint3)));
  }

  private void testParseEndpointCassandraStatusString(String sourceNode,
                                                      String endpoint1, String endpoint2, String endpoint3) {
    Map<String, String> simpleStates = Maps.newHashMap();

    String endpointsStatusString =
        endpoint1 + "\n"
            + "  generation:1506371953\n"
            + "  heartbeat:360312\n"
            + "  STATUS:17:NORMAL,-1046276550383960957\n"
            + "  LOAD:360290:2.07524381E8\n"
            + "  SCHEMA:160565:c09883c3-ac39-3bd5-8982-6e35a425d7a3\n"
            + "  DC:8:us-west-2\n"
            + "  RACK:10:a\n"
            + "  RELEASE_VERSION:4:3.11.0\n"
            + "  INTERNAL_IP:6:172.0.0.1\n"
            + "  RPC_ADDRESS:3:xx.xx.xx.xx\n"
            + "  NET_VERSION:1:11\n"
            + "  HOST_ID:2:a05fd32d-4bd8-44a4-9265-2a47f8ef7130\n"
            + "  RPC_READY:29:true\n"
            + "  X1:37:{\"siq_test1\":3,\"siq_test3\":3}\n"
            + "  X2:73550:9d0e8942-20dd-4bb8-878d-f20b4e847d8f/15\n"
            + "  TOKENS:16:<hidden>\n"
            + endpoint2 + "\n"
            + "  generation:1506368075\n"
            + "  heartbeat:364340\n"
            + "  STATUS:17:NORMAL,-1278741951029486876\n"
            + "  LOAD:364339:1.9967795187E10\n"
            + "  SCHEMA:164592:c09883c3-ac39-3bd5-8982-6e35a425d7a3\n"
            + "  DC:8:us-west-2\n"
            + "  RACK:10:a\n"
            + "  RELEASE_VERSION:4:3.11.0\n"
            + "  INTERNAL_IP:6:172.0.0.2\n"
            + "  RPC_ADDRESS:3:xx.xx.xx.xx\n"
            + "  NET_VERSION:1:11\n"
            + "  HOST_ID:2:9d0e8942-20dd-4bb8-878d-f20b4e847d8f\n"
            + "  RPC_READY:30:true\n"
            + "  X1:36:{\"siq_test1\":3,\"siq_test3\":3}\n"
            + "  X2:641:9d0e8942-20dd-4bb8-878d-f20b4e847d8f/15\n"
            + "  TOKENS:16:<hidden>\n"
            + endpoint3 + "\n"
            + "  generation:1506368359\n"
            + "  heartbeat:364021\n"
            + "  STATUS:17:NORMAL,-1065832441861161765\n"
            + "  LOAD:364005:2.7071009691E10\n"
            + "  SCHEMA:164276:c09883c3-ac39-3bd5-8982-6e35a425d7a3\n"
            + "  DC:8:us-west-2\n"
            + "  RACK:10:a\n"
            + "  RELEASE_VERSION:4:3.11.0\n"
            + "  INTERNAL_IP:6:172.0.0.3\n"
            + "  RPC_ADDRESS:3:xx.xx.xx.xx\n"
            + "  NET_VERSION:1:11\n"
            + "  HOST_ID:2:aa9a2c53-b7d6-4c48-b094-4233e97e8e84\n"
            + "  RPC_READY:30:true\n"
            + "  X1:36:{\"siq_test1\":3,\"siq_test3\":3}\n"
            + "  X2:324:9d0e8942-20dd-4bb8-878d-f20b4e847d8f/15\n"
            + "  TOKENS:16:<hidden>\n";

    simpleStates.put(endpoint1, "DOWN");
    simpleStates.put(endpoint3, "UP");

    NodesStatus nodesStatus = new NodesStatus(sourceNode, endpointsStatusString, simpleStates);

    assertEquals(1, nodesStatus.endpointStates.size());
    assertEquals(sourceNode, nodesStatus.endpointStates.get(0).sourceNode);

    Map<String, Map<String, List<EndpointState>>> endpoints =
        nodesStatus.endpointStates.get(0).endpoints;
    assertEquals(1, endpoints.get("us-west-2").keySet().size());
    assertEquals(3, endpoints.get("us-west-2").get("a").size());
    assertEquals("NORMAL - DOWN", endpoints.get("us-west-2").get("a").get(0).status);
    assertEquals("NORMAL - UNKNOWN", endpoints.get("us-west-2").get("a").get(1).status);
    assertEquals("NORMAL - UP", endpoints.get("us-west-2").get("a").get(2).status);
    assertEquals(afterSlash(endpoint1), endpoints.get("us-west-2").get("a").get(0).endpoint);
    assertEquals(
        "a05fd32d-4bd8-44a4-9265-2a47f8ef7130", endpoints.get("us-west-2").get("a").get(0).hostId);
    assertTrue(endpoints.get("us-west-2").get("a").get(0).severity.equals(0.0));
    assertEquals("3.11.0", endpoints.get("us-west-2").get("a").get(0).releaseVersion);
    assertEquals("us-west-2", endpoints.get("us-west-2").get("a").get(0).dc);
    assertEquals("a", endpoints.get("us-west-2").get("a").get(1).rack);
    assertEquals("us-west-2", endpoints.get("us-west-2").get("a").get(0).dc);
    assertEquals("a", endpoints.get("us-west-2").get("a").get(2).rack);
    assertTrue(endpoints.get("us-west-2").get("a").get(2).load.equals(27071009691.0));

    assertTrue(nodesStatus.endpointStates.get(0).endpointNames.contains(afterSlash(endpoint1)));
    assertTrue(nodesStatus.endpointStates.get(0).endpointNames.contains(afterSlash(endpoint2)));
    assertTrue(nodesStatus.endpointStates.get(0).endpointNames.contains(afterSlash(endpoint3)));
  }

  private static String afterSlash(String value) {
    Preconditions.checkNotNull(value);
    String[] split = value.split("/");
    if (split.length == 1) {
      return value;
    }
    if (split.length == 2) {
      return split[1];
    }
    throw new IllegalArgumentException("Too many slashes in " + value);
  }

}
