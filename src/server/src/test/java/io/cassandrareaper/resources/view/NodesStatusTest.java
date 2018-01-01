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

import com.google.common.collect.Maps;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public final class NodesStatusTest {

  @Test
  public void testParseEndpoint22StatusString() {
    Map<String, String> simpleStates = Maps.newHashMap();

    String endpointsStatusString =
        "/127.0.0.1\n"
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
        + "/127.0.0.2\n"
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
        + "/127.0.0.3\n"
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
  public void testParseEndpoint22WithHostnameStatusString() {
    Map<String, String> simpleStates = Maps.newHashMap();

    String endpointsStatusString =
        "localhost/127.0.0.1\n"
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
        + "/127.0.0.2\n"
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
        + "localhost3/127.0.0.3\n"
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

    simpleStates.put("localhost3/127.0.0.3", "UP");
    simpleStates.put("localhost/127.0.0.1", "DOWN");

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
        "/127.0.0.1\n"
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
        + "/127.0.0.2\n"
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
        + "/127.0.0.3\n"
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

  @Test
  public void testParseEndpointElassandraStatusString() {
    Map<String, String> simpleStates = Maps.newHashMap();

    String endpointsStatusString =
        "/10.0.0.1\n"
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
            + "/10.0.0.2\n"
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
            + "/10.0.0.3\n"
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

    simpleStates.put("/10.0.0.3", "UP");
    simpleStates.put("/10.0.0.1", "DOWN");

    NodesStatus nodesStatus = new NodesStatus("10.0.0.1", endpointsStatusString, simpleStates);

    assertEquals(nodesStatus.endpointStates.size(), 1);
    assertEquals(nodesStatus.endpointStates.get(0).sourceNode, "10.0.0.1");

    Map<String, Map<String, List<EndpointState>>> endpoints =
        nodesStatus.endpointStates.get(0).endpoints;
    assertEquals(1, endpoints.get("us-west-2").keySet().size());
    assertEquals(3, endpoints.get("us-west-2").get("a").size());
    assertEquals("NORMAL - DOWN", endpoints.get("us-west-2").get("a").get(0).status);
    assertEquals("NORMAL - UNKNOWN", endpoints.get("us-west-2").get("a").get(1).status);
    assertEquals("NORMAL - UP", endpoints.get("us-west-2").get("a").get(2).status);
    assertEquals("10.0.0.1", endpoints.get("us-west-2").get("a").get(0).endpoint);
    assertEquals(
        "a05fd32d-4bd8-44a4-9265-2a47f8ef7130", endpoints.get("us-west-2").get("a").get(0).hostId);
    assertTrue(endpoints.get("us-west-2").get("a").get(0).severity.equals(0.0));
    assertEquals("3.11.0", endpoints.get("us-west-2").get("a").get(0).releaseVersion);
    assertEquals("us-west-2", endpoints.get("us-west-2").get("a").get(0).dc);
    assertEquals("a", endpoints.get("us-west-2").get("a").get(1).rack);
    assertEquals("us-west-2", endpoints.get("us-west-2").get("a").get(0).dc);
    assertEquals("a", endpoints.get("us-west-2").get("a").get(2).rack);
    assertTrue(endpoints.get("us-west-2").get("a").get(2).load.equals(27071009691.0));

    assertTrue(nodesStatus.endpointStates.get(0).endpointNames.contains("10.0.0.1"));
    assertTrue(nodesStatus.endpointStates.get(0).endpointNames.contains("10.0.0.2"));
    assertTrue(nodesStatus.endpointStates.get(0).endpointNames.contains("10.0.0.3"));
  }

}
