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

import io.cassandrareaper.resources.view.NodesStatus.EndpointState;

import java.util.List;
import java.util.Map;

import jersey.repackaged.com.google.common.collect.Maps;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public final class NodesStatusTest {

  @Test
  public void testParseEndpoint22StatusString() {
    Map<String, String> simpleStates = Maps.newHashMap();

    String endpointsStatusString =
        "/127.0.0.1"
        + "  generation:1496849190 "
        + "  heartbeat:1231900 "
        + "  STATUS:14:NORMAL,-9223372036854775808 "
        + "  LOAD:1231851:4043215.0 "
        + "  SCHEMA:10:2de0af6a-bf86-38e0-b62b-474ff6aefb51 "
        + "  DC:6:datacenter1 "
        + "  RACK:8:rack1 "
        + "  RELEASE_VERSION:4:3.0.8 "
        + "  RPC_ADDRESS:3:127.0.0.1 "
        + "  SEVERITY:1231899:0.0 "
        + "  NET_VERSION:1:10 "
        + "  HOST_ID:2:f091f82b-ce2c-40ee-b30c-6e761e94e821 "
        + "  RPC_READY:16:true "
        + "  TOKENS:13:<hidden> \r"
        + "/127.0.0.2"
        + "  generation:1497347537 "
        + "  heartbeat:27077 "
        + "  STATUS:14:NORMAL,-3074457345618258603 "
        + "  LOAD:1231851:3988763.0 "
        + "  SCHEMA:10:2de0af6a-bf86-38e0-b62b-474ff6aefb51 "
        + "  DC:6:datacenter2 "
        + "  RACK:8:rack2 "
        + "  RELEASE_VERSION:4:3.0.8 "
        + "  RPC_ADDRESS:3:127.0.0.2 "
        + "  SEVERITY:1231899:0.0 "
        + "  NET_VERSION:1:10 "
        + "  HOST_ID:2:08f819b5-d96f-444e-9d4d-ec4136e1b716 "
        + "  RPC_READY:16:true"
        + "  TOKENS:13:<hidden> \r"
        + "/127.0.0.3"
        + "  generation:1496849191 "
        + "  heartbeat:1230183 "
        + "  STATUS:16:NORMAL,3074457345618258602 "
        + "  LOAD:1230134:3.974144E6"
        + "  SCHEMA:10:2de0af6a-bf86-38e0-b62b-474ff6aefb51 "
        + "  DC:6:us-west-1"
        + "  RACK:8:rack3 "
        + "  RELEASE_VERSION:4:3.0.8 "
        + "  RPC_ADDRESS:3:127.0.0.3 "
        + "  SEVERITY:1230182:0.0 "
        + "  NET_VERSION:1:10 "
        + "  HOST_ID:2:20769fed-7916-4b7a-a729-8b99bcdc9b95 "
        + "  RPC_READY:44:true "
        + "  TOKENS:15:<hidden> ";

    simpleStates.put("/127.0.0.3", "UP");
    simpleStates.put("/127.0.0.1", "DOWN");

    NodesStatus nodesStatus = new NodesStatus("127.0.0.1", endpointsStatusString, simpleStates);

    assertEquals(nodesStatus.endpointStates.size(), 1);
    assertEquals(nodesStatus.endpointStates.get(0).sourceNode, "127.0.0.1");

    Map<String,Map<String,List<EndpointState>>> endpoints = nodesStatus.endpointStates.get(0).endpoints;
    assertEquals(endpoints.get("datacenter1").keySet().size(), 1);
    assertEquals(endpoints.get("datacenter1").get("rack1").size(), 1);
    assertEquals(endpoints.get("datacenter1").get("rack1").get(0).status, "NORMAL - DOWN");
    assertEquals(endpoints.get("datacenter2").get("rack2").get(0).status, "NORMAL - UNKNOWN");
    assertEquals(endpoints.get("us-west-1").get("rack3").get(0).status, "NORMAL - UP");
    assertEquals(endpoints.get("datacenter1").get("rack1").get(0).endpoint, "127.0.0.1");
    assertEquals(endpoints.get("datacenter1").get("rack1").get(0).hostId, "f091f82b-ce2c-40ee-b30c-6e761e94e821");
    assertEquals(endpoints.get("datacenter1").get("rack1").get(0).tokens, "13");
    assertTrue(endpoints.get("datacenter1").get("rack1").get(0).severity.equals(0.0));
    assertEquals(endpoints.get("datacenter1").get("rack1").get(0).releaseVersion, "3.0.8");
    assertEquals(endpoints.get("datacenter2").get("rack2").get(0).dc, "datacenter2");
    assertEquals(endpoints.get("datacenter2").get("rack2").get(0).rack, "rack2");
    assertEquals(endpoints.get("us-west-1").get("rack3").get(0).dc, "us-west-1");
    assertEquals(endpoints.get("us-west-1").get("rack3").get(0).rack, "rack3");
    assertTrue(endpoints.get("us-west-1").get("rack3").get(0).load.equals(3974144.0));
  }

  @Test
  public void testParseEndpoint21StatusString() {
    Map<String, String> simpleStates = Maps.newHashMap();

    String endpointsStatusString =
        "/127.0.0.1"
        + "  generation:1496849190 "
        + "  heartbeat:1231900 "
        + "  STATUS:NORMAL,-9223372036854775808 "
        + "  LOAD:4043215.0 "
        + "  SCHEMA:2de0af6a-bf86-38e0-b62b-474ff6aefb51 "
        + "  DC:datacenter1 "
        + "  RACK:rack1 "
        + "  RELEASE_VERSION:3.0.8 "
        + "  RPC_ADDRESS:127.0.0.1 "
        + "  SEVERITY:0.0 "
        + "  NET_VERSION:10 "
        + "  HOST_ID:f091f82b-ce2c-40ee-b30c-6e761e94e821 "
        + "  RPC_READY:true "
        + "  TOKENS:<hidden> \r"
        + "/127.0.0.2"
        + "  generation:1497347537 "
        + "  heartbeat:27077 "
        + "  STATUS:NORMAL,-3074457345618258603 "
        + "  LOAD:3988763.0 "
        + "  SCHEMA:2de0af6a-bf86-38e0-b62b-474ff6aefb51 "
        + "  DC:datacenter2 "
        + "  RACK:rack2 "
        + "  RELEASE_VERSION:3.0.8 "
        + "  RPC_ADDRESS:127.0.0.2 "
        + "  SEVERITY:0.0 "
        + "  NET_VERSION:10 "
        + "  HOST_ID:08f819b5-d96f-444e-9d4d-ec4136e1b716 "
        + "  RPC_READY:true \r"
        + "/127.0.0.3"
        + "  generation:1496849191 "
        + "  heartbeat:1230183 "
        + "  STATUS:NORMAL,3074457345618258602 "
        + "  LOAD:3.974144E6"
        + "  SCHEMA:2de0af6a-bf86-38e0-b62b-474ff6aefb51 "
        + "  DC:us-west-1"
        + "  RACK:rack3 "
        + "  RELEASE_VERSION:3.0.8 "
        + "  RPC_ADDRESS:127.0.0.3 "
        + "  SEVERITY:0.0 "
        + "  NET_VERSION:10 "
        + "  HOST_ID:20769fed-7916-4b7a-a729-8b99bcdc9b95 "
        + "  RPC_READY:true "
        + "  TOKENS:<hidden> ";

    simpleStates.put("/127.0.0.3", "UP");
    simpleStates.put("/127.0.0.1", "DOWN");

    NodesStatus nodesStatus = new NodesStatus("127.0.0.1", endpointsStatusString, simpleStates);

    assertEquals(nodesStatus.endpointStates.size(), 1);
    assertEquals(nodesStatus.endpointStates.get(0).sourceNode, "127.0.0.1");

    Map<String,Map<String,List<EndpointState>>> endpoints = nodesStatus.endpointStates.get(0).endpoints;
    assertEquals(endpoints.get("datacenter1").keySet().size(), 1);
    assertEquals(endpoints.get("datacenter1").get("rack1").size(), 1);
    assertEquals(endpoints.get("datacenter1").get("rack1").get(0).status, "NORMAL - DOWN");
    assertEquals(endpoints.get("datacenter2").get("rack2").get(0).status, "NORMAL - UNKNOWN");
    assertEquals(endpoints.get("us-west-1").get("rack3").get(0).status, "NORMAL - UP");
    assertEquals(endpoints.get("datacenter1").get("rack1").get(0).endpoint, "127.0.0.1");
    assertEquals(endpoints.get("datacenter1").get("rack1").get(0).hostId, "f091f82b-ce2c-40ee-b30c-6e761e94e821");
    assertTrue(endpoints.get("datacenter1").get("rack1").get(0).severity.equals(0.0));
    assertEquals(endpoints.get("datacenter1").get("rack1").get(0).releaseVersion, "3.0.8");
    assertEquals(endpoints.get("datacenter2").get("rack2").get(0).dc, "datacenter2");
    assertEquals(endpoints.get("datacenter2").get("rack2").get(0).rack, "rack2");
    assertEquals(endpoints.get("us-west-1").get("rack3").get(0).dc, "us-west-1");
    assertEquals(endpoints.get("us-west-1").get("rack3").get(0).rack, "rack3");
    assertTrue(endpoints.get("us-west-1").get("rack3").get(0).load.equals(3974144.0));

    assertTrue(nodesStatus.endpointStates.get(0).endpointNames.contains("127.0.0.1"));
    assertTrue(nodesStatus.endpointStates.get(0).endpointNames.contains("127.0.0.2"));
    assertTrue(nodesStatus.endpointStates.get(0).endpointNames.contains("127.0.0.3"));
  }

}
