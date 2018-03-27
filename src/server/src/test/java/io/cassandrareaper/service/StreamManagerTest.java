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

package io.cassandrareaper.service;

import io.cassandrareaper.AppContext;
import io.cassandrareaper.ReaperApplicationConfiguration;
import io.cassandrareaper.core.Node;
import io.cassandrareaper.core.Stream;

import java.net.InetAddress;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.management.openmbean.CompositeData;

import com.google.common.collect.Lists;
import org.apache.cassandra.streaming.ProgressInfo;
import org.apache.cassandra.streaming.SessionInfo;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamSummary;
import org.apache.cassandra.streaming.management.ProgressInfoCompositeData;
import org.apache.cassandra.streaming.management.SessionInfoCompositeData;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


public class StreamManagerTest {

  private StreamManager streamManager;

  @Before
  public void setup() {
    AppContext context = new AppContext();
    context.config = new ReaperApplicationConfiguration();
    streamManager = StreamManager.create(context);
  }

  @Test
  public void testInitStream() throws Exception {

    String cluster = "test-cluster";
    String host = "127.0.0.1";
    long timestamp = 0;
    String sessionId = UUID.fromString("00000000-0000-0000-0000-000000000000").toString();

    // Make StreamManager handle a message that is sent when a stream is established
    CompositeData sessionInfo = sessionInfo(host, "127.0.0.2", sessionId, 0);
    streamManager.handleNotification(cluster, host, sessionInfo, timestamp).get();

    Map<String, io.cassandrareaper.core.StreamSession> streamSessions = streamManager.listStreams(cluster);

    assertEquals("Only one session should exist",
                 1,
                 streamSessions.size());
    assertEquals("The session should have the correct ID",
                 sessionId,
                 streamSessions.keySet().iterator().next());

    io.cassandrareaper.core.StreamSession streamSession = streamSessions.get(sessionId);

    assertEquals("There should be one stream session",
                 1,
                 streamSession.getStreams().size());

    Map<String, Stream> streamMap = streamSession.getStreams();

    assertEquals("There should be one stream in the session",
                 1,
                 streamMap.size());
    assertEquals("The stream has the expected ID",
                 "127.0.0.1->127.0.0.2",
                 streamMap.keySet().iterator().next());

    Stream stream = streamMap.values().iterator().next();

    assertEquals("The source host is correct",
                 "127.0.0.1",
                 stream.getHost());
    assertEquals("The target host is correct",
                 "127.0.0.2",
                 stream.getPeer());
    assertEquals("No data has been sent",
                 0,
                 stream.getSizeSent());
    assertEquals("No data has been received",
                 0,
                 stream.getSizeReceived());
  }

  @Test
  public void testUpdateStream() throws Exception {
    final String cluster = "test-cluster";
    final String host = "127.0.0.1";
    final String peer = "127.0.0.2";
    final long timestamp1 = 0;
    final long timestamp2 = 1;
    final String sessionId = UUID.fromString("00000000-0000-0000-0000-000000000000").toString();
    final String fileName = "/tmp/foo";

    // Make the StreamManager handle a message when a stream is established
    CompositeData sessionInfo = sessionInfo(host, peer, sessionId, 0);
    streamManager.handleNotification(cluster, host, sessionInfo, timestamp1).get();

    // Get streams to verify the stream was created
    Map<String, io.cassandrareaper.core.StreamSession> streams = streamManager.listStreams(cluster);

    assertEquals("There is one stream in the cache",
                 1,
                 streams.size());

    Stream stream = streams.get(sessionId).getStream(String.format("%s->%s", host, peer));

    assertEquals("No data was sent within that stream",
                 0,
                 stream.getSizeSent());
    assertEquals("The stream was last updated with the initial message",
                 timestamp1,
                 stream.getLastUpdated());
    assertNull("The file is not tracked yet",
               stream.getProgressSent().get(fileName));

    // Send a second message indicating some activity on the stream
    CompositeData progressInfo = progressInfo(sessionId, peer, fileName, "OUT", 5, 5);
    streamManager.handleNotification(cluster, host, progressInfo, timestamp2).get();

    streams = streamManager.listStreams(cluster);

    assertEquals("There is still one stream",
                 1,
                 streams.size());

    stream = streams.get(sessionId).getStream(String.format("%s->%s", host, peer));

    assertEquals("The number of sent bytes was updated",
                 5,
                 stream.getSizeSent());
    assertEquals("The updated happened with the second message",
                 timestamp2,
                 stream.getLastUpdated());
    assertNotNull("The streamed file is tracked",
                  stream.getProgressSent().get(fileName));
    assertEquals("The streamed file has correct progress set",
                 5,
                 stream.getProgressSent().get(fileName).totalBytes);
  }

  @Test
  public void testUpdateStreamOfTwoFiles() throws Exception {
    final String cluster = "test-cluster";
    final String host = "127.0.0.1";
    final String peer = "127.0.0.2";
    final long timestamp1 = 0;
    final long timestamp2 = 1;
    final long timestamp3 = 2;
    final String sessionId = UUID.fromString("00000000-0000-0000-0000-000000000000").toString();
    final String streamId = String.format("%s->%s", host, peer);
    final String fileName1 = "/tmp/foo";
    final String fileName2 = "/tmp/bar";
    final long totalSizeToSend = 50;

    // report the stream
    CompositeData sessionInfo = sessionInfo(host, peer, sessionId, totalSizeToSend);
    streamManager.handleNotification(cluster, host, sessionInfo, timestamp1).get();

    // make progress on one file
    CompositeData progressInfo = progressInfo(sessionId, peer, fileName1, "OUT", 5, 5);
    streamManager.handleNotification(cluster, host, progressInfo, timestamp2).get();

    // make progress on the second file
    progressInfo = progressInfo(sessionId, peer, fileName2, "OUT", 7, 7);
    streamManager.handleNotification(cluster, host, progressInfo, timestamp3).get();

    // assert that the files have progress correctly set
    Map<String, Stream.FileProgress> streamSession = streamManager
        .listStreams(cluster)
        .get(sessionId)
        .getStream(streamId)
        .getProgressSent();

    assertEquals("Stream tracks progress of two files",
                 2,
                 streamSession.size());
    assertEquals("Progress of first file is correct",
                 5,
                 streamSession.get(fileName1).totalBytes);
    assertEquals("Progress of the second file is correct",
                 7,
                 streamSession.get(fileName2).totalBytes);

    // assert that the stream is correct overall
    Stream stream = streamManager.listStreams(cluster).get(sessionId).getStream(streamId);

    assertEquals("The stream was updated with the last message" ,
                 timestamp3,
                 stream.getLastUpdated());
    assertEquals("The stream has correct sizeSent",
                 12,
                 stream.getSizeSent());
    assertEquals("The stream has correct total size to send",
                 totalSizeToSend,
                 stream.getSizeToSend());
    assertFalse("The stream is not considered completed",
                stream.getCompleted());
  }

  @Test
  public void testGetStreamsForNode() throws Exception {
    final String cluster = "test-cluster";
    final String host1 = "127.0.0.1";
    final String host2 = "127.0.0.2";
    final String host3 = "127.0.0.3";
    final long timestamp1 = 0;
    final long timestamp2 = 1;
    final long timestamp3 = 2;
    final String sessionId = UUID.fromString("00000000-0000-0000-0000-000000000000").toString();
    final long totalSizeToSend = 50;

    // init stream node1 -> node2 by node1
    CompositeData sessionInfo = sessionInfo(host1, host2, sessionId, totalSizeToSend);
    streamManager.handleNotification(cluster, host1, sessionInfo, timestamp1).get();

    // init stream node2 -> node1 by node2
    sessionInfo = sessionInfo(host2, host1, sessionId, totalSizeToSend);
    streamManager.handleNotification(cluster, host2, sessionInfo, timestamp2).get();

    // init stream node3 -> node2 by node3
    sessionInfo = sessionInfo(host3, host2, sessionId, totalSizeToSend);
    streamManager.handleNotification(cluster, host3, sessionInfo, timestamp3).get();

    // get streams for node1
    Node node1 = Node.builder().withClusterName(cluster).withHostname(host1).build();
    List<Stream> node1Streams = streamManager.listStreams(node1);
    assertEquals("There are 2 streams featuring node1",
                 2,
                 node1Streams.size());

    // get streams for node2
    Node node2 = Node.builder().withClusterName(cluster).withHostname(host2).build();
    List<Stream> node2Streams = streamManager.listStreams(node2);
    assertEquals("There are three streams featuring node2",
                 3,
                 node2Streams.size());

    // get streams for node3
    Node node3 = Node.builder().withClusterName(cluster).withHostname(host3).build();
    List<Stream> node3Streams = streamManager.listStreams(node3);
    assertEquals("There is one stream featuring node3",
                 1,
                 node3Streams.size());

  }

  @Test
  public void testGettingStreamUpdateWithoutInitializingTheStream() throws Exception {

    final String cluster = "test-cluster";
    final String host = "127.0.0.1";
    final String peer = "127.0.0.2";
    final String fileName = "/tmp/foo";
    final long timestamp1 = 0;
    final String sessionId = UUID.fromString("00000000-0000-0000-0000-000000000000").toString();

    // make progress on one file
    CompositeData progressInfo = progressInfo(sessionId, peer, fileName, "OUT", 5, 5);
    streamManager.handleNotification(cluster, host, progressInfo, timestamp1).get();

    // the session gets created but there are no streams in it
    io.cassandrareaper.core.StreamSession session = streamManager.listStreams(cluster).get(sessionId);
    assertNotNull("The session is present",
                  session);
    assertTrue("There are no streams",
               session.getStreams().isEmpty());
  }

  private CompositeData sessionInfo(String host, String peer, String planId, long totalSizeToSend) throws Exception {
    UUID planUuid = UUID.fromString(planId);
    InetAddress hostAddr = InetAddress.getByName(host);
    int sessionIndex = 0;
    InetAddress peerAddr = InetAddress.getByName(peer);
    Collection<StreamSummary> receiving = Lists.newArrayList(new StreamSummary(planUuid, 0, 0));
    Collection<StreamSummary> sending = Lists.newArrayList(new StreamSummary(planUuid, 0, totalSizeToSend));
    StreamSession.State state = StreamSession.State.PREPARING;
    SessionInfo sessionInfo = new SessionInfo(hostAddr, sessionIndex, peerAddr, receiving, sending, state);
    return SessionInfoCompositeData.toCompositeData(planUuid, sessionInfo);
  }

  private CompositeData progressInfo(String planId,
                                     String peer,
                                     String fileName,
                                     String direction,
                                     long currentB,
                                     long totalB
  ) throws Exception {
    InetAddress peerAddr = InetAddress.getByName(peer);
    UUID planUuid = UUID.fromString(planId);
    int sessionIndex = 0;
    ProgressInfo.Direction dir = ProgressInfo.Direction.valueOf(direction);

    ProgressInfo progressInfo = new ProgressInfo(peerAddr, sessionIndex, fileName, dir, currentB, totalB);
    return ProgressInfoCompositeData.toCompositeData(planUuid, progressInfo);
  }

}
