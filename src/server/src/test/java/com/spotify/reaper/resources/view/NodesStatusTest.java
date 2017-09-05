package com.spotify.reaper.resources.view;

import org.junit.Test;

import com.spotify.reaper.resources.view.NodesStatus.EndpointState;

import jersey.repackaged.com.google.common.collect.Maps;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class NodesStatusTest {


  @Test
  public void testParseEndpoint22StatusString() {
    Map<String, String> simpleStates = Maps.newHashMap();

    StringBuilder endpointsStatusString = new StringBuilder().append("/127.0.0.1")
        .append("  generation:1496849190 ")
        .append("  heartbeat:1231900 ")
        .append("  STATUS:14:NORMAL,-9223372036854775808 ")
        .append("  LOAD:1231851:4043215.0 ")
        .append("  SCHEMA:10:2de0af6a-bf86-38e0-b62b-474ff6aefb51 ")
        .append("  DC:6:datacenter1 ")
        .append("  RACK:8:rack1 ")
        .append("  RELEASE_VERSION:4:3.0.8 ")
        .append("  RPC_ADDRESS:3:127.0.0.1 ")
        .append("  SEVERITY:1231899:0.0 ")
        .append("  NET_VERSION:1:10 ")
        .append("  HOST_ID:2:f091f82b-ce2c-40ee-b30c-6e761e94e821 ")
        .append("  RPC_READY:16:true ")
        .append("  TOKENS:13:<hidden> \r")
        .append("/127.0.0.2")
        .append("  generation:1497347537 ")
        .append("  heartbeat:27077 ")
        .append("  STATUS:14:NORMAL,-3074457345618258603 ")
        .append("  LOAD:1231851:3988763.0 ")
        .append("  SCHEMA:10:2de0af6a-bf86-38e0-b62b-474ff6aefb51 ")
        .append("  DC:6:datacenter2 ")
        .append("  RACK:8:rack2 ")
        .append("  RELEASE_VERSION:4:3.0.8 ")
        .append("  RPC_ADDRESS:3:127.0.0.2 ")
        .append("  SEVERITY:1231899:0.0 ")
        .append("  NET_VERSION:1:10 ")
        .append("  HOST_ID:2:08f819b5-d96f-444e-9d4d-ec4136e1b716 ")
        .append("  RPC_READY:16:true")
        .append("  TOKENS:13:<hidden> \r")
        .append("/127.0.0.3")
        .append("  generation:1496849191 ")
        .append("  heartbeat:1230183 ")
        .append("  STATUS:16:NORMAL,3074457345618258602 ")
        .append("  LOAD:1230134:3.974144E6")
        .append("  SCHEMA:10:2de0af6a-bf86-38e0-b62b-474ff6aefb51 ")
        .append("  DC:6:us-west-1")
        .append("  RACK:8:rack3 ")
        .append("  RELEASE_VERSION:4:3.0.8 ")
        .append("  RPC_ADDRESS:3:127.0.0.3 ")
        .append("  SEVERITY:1230182:0.0 ")
        .append("  NET_VERSION:1:10 ")
        .append("  HOST_ID:2:20769fed-7916-4b7a-a729-8b99bcdc9b95 ")
        .append("  RPC_READY:44:true ")
        .append("  TOKENS:15:<hidden> ");

    simpleStates.put("/127.0.0.3", "UP");
    simpleStates.put("/127.0.0.1", "DOWN");

    NodesStatus nodesStatus = new NodesStatus("127.0.0.1", endpointsStatusString.toString(), simpleStates);

    assertEquals(nodesStatus.endpointStates.size(), 1);
    assertEquals(nodesStatus.endpointStates.get(0).sourceNode, "127.0.0.1");

    assertEquals(nodesStatus.endpointStates.get(0).endpoints.get("datacenter1").keySet().size(), 1);
    assertEquals(nodesStatus.endpointStates.get(0).endpoints.get("datacenter1").get("rack1").size(), 1);
    assertEquals(nodesStatus.endpointStates.get(0).endpoints.get("datacenter1").get("rack1").get(0).status, "NORMAL - DOWN");
    assertEquals(nodesStatus.endpointStates.get(0).endpoints.get("datacenter2").get("rack2").get(0).status, "NORMAL - UNKNOWN");
    assertEquals(nodesStatus.endpointStates.get(0).endpoints.get("us-west-1").get("rack3").get(0).status, "NORMAL - UP");
    assertEquals(nodesStatus.endpointStates.get(0).endpoints.get("datacenter1").get("rack1").get(0).endpoint, "127.0.0.1");
    assertEquals(nodesStatus.endpointStates.get(0).endpoints.get("datacenter1").get("rack1").get(0).hostId, "f091f82b-ce2c-40ee-b30c-6e761e94e821");
    assertEquals(nodesStatus.endpointStates.get(0).endpoints.get("datacenter1").get("rack1").get(0).tokens, "13");
    assertTrue(nodesStatus.endpointStates.get(0).endpoints.get("datacenter1").get("rack1").get(0).severity.equals(0.0));
    assertEquals(nodesStatus.endpointStates.get(0).endpoints.get("datacenter1").get("rack1").get(0).releaseVersion, "3.0.8");
    assertEquals(nodesStatus.endpointStates.get(0).endpoints.get("datacenter2").get("rack2").get(0).dc, "datacenter2");
    assertEquals(nodesStatus.endpointStates.get(0).endpoints.get("datacenter2").get("rack2").get(0).rack, "rack2");
    assertEquals(nodesStatus.endpointStates.get(0).endpoints.get("us-west-1").get("rack3").get(0).dc, "us-west-1");
    assertEquals(nodesStatus.endpointStates.get(0).endpoints.get("us-west-1").get("rack3").get(0).rack, "rack3");
    assertTrue(nodesStatus.endpointStates.get(0).endpoints.get("us-west-1").get("rack3").get(0).load.equals(3974144.0));
  }

  @Test
  public void testParseEndpoint21StatusString() {
    Map<String, String> simpleStates = Maps.newHashMap();

    StringBuilder endpointsStatusString = new StringBuilder().append("/127.0.0.1")
        .append("  generation:1496849190 ")
        .append("  heartbeat:1231900 ")
        .append("  STATUS:NORMAL,-9223372036854775808 ")
        .append("  LOAD:4043215.0 ")
        .append("  SCHEMA:2de0af6a-bf86-38e0-b62b-474ff6aefb51 ")
        .append("  DC:datacenter1 ")
        .append("  RACK:rack1 ")
        .append("  RELEASE_VERSION:3.0.8 ")
        .append("  RPC_ADDRESS:127.0.0.1 ")
        .append("  SEVERITY:0.0 ")
        .append("  NET_VERSION:10 ")
        .append("  HOST_ID:f091f82b-ce2c-40ee-b30c-6e761e94e821 ")
        .append("  RPC_READY:true ")
        .append("  TOKENS:<hidden> \r")
        .append("/127.0.0.2")
        .append("  generation:1497347537 ")
        .append("  heartbeat:27077 ")
        .append("  STATUS:NORMAL,-3074457345618258603 ")
        .append("  LOAD:3988763.0 ")
        .append("  SCHEMA:2de0af6a-bf86-38e0-b62b-474ff6aefb51 ")
        .append("  DC:datacenter2 ")
        .append("  RACK:rack2 ")
        .append("  RELEASE_VERSION:3.0.8 ")
        .append("  RPC_ADDRESS:127.0.0.2 ")
        .append("  SEVERITY:0.0 ")
        .append("  NET_VERSION:10 ")
        .append("  HOST_ID:08f819b5-d96f-444e-9d4d-ec4136e1b716 ")
        .append("  RPC_READY:true \r")
        .append("/127.0.0.3")
        .append("  generation:1496849191 ")
        .append("  heartbeat:1230183 ")
        .append("  STATUS:NORMAL,3074457345618258602 ")
        .append("  LOAD:3.974144E6")
        .append("  SCHEMA:2de0af6a-bf86-38e0-b62b-474ff6aefb51 ")
        .append("  DC:us-west-1")
        .append("  RACK:rack3 ")
        .append("  RELEASE_VERSION:3.0.8 ")
        .append("  RPC_ADDRESS:127.0.0.3 ")
        .append("  SEVERITY:0.0 ")
        .append("  NET_VERSION:10 ")
        .append("  HOST_ID:20769fed-7916-4b7a-a729-8b99bcdc9b95 ")
        .append("  RPC_READY:true ")
        .append("  TOKENS:<hidden> ");

    simpleStates.put("/127.0.0.3","UP");
    simpleStates.put("/127.0.0.1","DOWN");


    NodesStatus nodesStatus = new NodesStatus("127.0.0.1", endpointsStatusString.toString(), simpleStates);

    assertEquals(nodesStatus.endpointStates.size(), 1);
    assertEquals(nodesStatus.endpointStates.get(0).sourceNode, "127.0.0.1");



    assertEquals(nodesStatus.endpointStates.get(0).endpoints.get("datacenter1").keySet().size(), 1);
    assertEquals(nodesStatus.endpointStates.get(0).endpoints.get("datacenter1").get("rack1").size(), 1);
    assertEquals(nodesStatus.endpointStates.get(0).endpoints.get("datacenter1").get("rack1").get(0).status, "NORMAL - DOWN");
    assertEquals(nodesStatus.endpointStates.get(0).endpoints.get("datacenter2").get("rack2").get(0).status, "NORMAL - UNKNOWN");
    assertEquals(nodesStatus.endpointStates.get(0).endpoints.get("us-west-1").get("rack3").get(0).status, "NORMAL - UP");
    assertEquals(nodesStatus.endpointStates.get(0).endpoints.get("datacenter1").get("rack1").get(0).endpoint, "127.0.0.1");
    assertEquals(nodesStatus.endpointStates.get(0).endpoints.get("datacenter1").get("rack1").get(0).hostId, "f091f82b-ce2c-40ee-b30c-6e761e94e821");
    assertTrue(nodesStatus.endpointStates.get(0).endpoints.get("datacenter1").get("rack1").get(0).severity.equals(0.0));
    assertEquals(nodesStatus.endpointStates.get(0).endpoints.get("datacenter1").get("rack1").get(0).releaseVersion, "3.0.8");
    assertEquals(nodesStatus.endpointStates.get(0).endpoints.get("datacenter2").get("rack2").get(0).dc, "datacenter2");
    assertEquals(nodesStatus.endpointStates.get(0).endpoints.get("datacenter2").get("rack2").get(0).rack, "rack2");
    assertEquals(nodesStatus.endpointStates.get(0).endpoints.get("us-west-1").get("rack3").get(0).dc, "us-west-1");
    assertEquals(nodesStatus.endpointStates.get(0).endpoints.get("us-west-1").get("rack3").get(0).rack, "rack3");
    assertTrue(nodesStatus.endpointStates.get(0).endpoints.get("us-west-1").get("rack3").get(0).load.equals(3974144.0));
  }

}
