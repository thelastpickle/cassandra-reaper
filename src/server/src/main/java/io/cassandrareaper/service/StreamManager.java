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
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.Node;
import io.cassandrareaper.core.Stream;
import io.cassandrareaper.core.StreamSession;
import io.cassandrareaper.jmx.JmxProxy;
import io.cassandrareaper.jmx.StreamStatusHandler;
import io.cassandrareaper.storage.StreamCache;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import javax.management.openmbean.CompositeData;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import org.apache.cassandra.streaming.ProgressInfo;
import org.apache.cassandra.streaming.SessionInfo;
import org.apache.cassandra.streaming.management.ProgressInfoCompositeData;
import org.apache.cassandra.streaming.management.SessionInfoCompositeData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements the functionality needed for receiving,
 * handling and serving streaming-related notifications.
 */
public final class StreamManager implements StreamStatusHandler {

  private static final Logger LOG = LoggerFactory.getLogger(StreamManager.class);
  private final AppContext context;
  private final ExecutorService executor = Executors.newFixedThreadPool(5);
  private final StreamCache streamSessions = new StreamCache();

  private StreamManager(AppContext context) {
    this.context = context;
  }

  public static StreamManager create(AppContext context) {
    return new StreamManager(context);
  }

  /**
   * Connects a JmxProxy to each node in the given cluster
   * and register notification handlers for streaming events.
   */
  public void addListeners(Cluster cluster) throws ReaperException {
    final int jmxTimeout = context.config.getJmxConnectionTimeoutInSeconds();
    try {
      JmxProxy seedJmxProxy = context.jmxConnectionFactory.connectAny(cluster, jmxTimeout);

      List<Callable<JmxProxy>> connectionTasks = seedJmxProxy.getLiveNodes()
          .stream()
          .map(hostName -> Node.builder().withCluster(cluster).withHostname(hostName).build())
          .map(node -> connectJmxTask(node))
          .collect(Collectors.toList());

      List<JmxProxy> nodeProxies = Lists.newArrayList();
      List<Future<JmxProxy>> futures = executor.invokeAll(connectionTasks);
      for (Future<JmxProxy> future : futures) {
        nodeProxies.add(future.get());
      }

      nodeProxies.forEach(nodeJmxProxy -> nodeJmxProxy.addStreamStatusHandler(this));

    } catch (InterruptedException | ExecutionException e) {
      LOG.error("Failed adding stream state listeners for cluster {}", cluster.getName(), e);
      throw new ReaperException(e);
    }
  }

  private Callable<JmxProxy> connectJmxTask(Node node) {
    return () -> context.jmxConnectionFactory.connect(node, context.config.getJmxConnectionTimeoutInSeconds());
  }

  @Override
  public Future<?> handleNotification(String cluster, String host, CompositeData payload, long timestamp) {
    String planId = payload.get("planId").toString();
    String streamEventType = payload.getCompositeType().getDescription();

    switch (streamEventType) {
      case "SessionInfo":
        return executor.submit(() -> handleSessionInfo(cluster, host, planId, payload, timestamp));
      case "ProgressInfo":
        return executor.submit(() -> handleProgressInfo(cluster, host, planId, payload, timestamp));
      case "SessionCompleteEvent":
        return executor.submit(() -> handleSessionCompleted(cluster, host, planId, payload, timestamp));
      case "StreamState":
        // Didn't find a reason to actually process this one
        return Futures.immediateFuture(null);
      default:
        LOG.error("Unknown stream event type : {} ", streamEventType);
    }
    return Futures.immediateFailedFuture(new ReaperException("Unknown stream event type " + streamEventType));
  }

  /**
   * Handle the beginning of a streaming session.
   */
  private void handleSessionInfo(String cluster, String host, String planId, CompositeData payload, long timestamp) {
    SessionInfo sessionInfo = SessionInfoCompositeData.fromCompositeData(payload);

    LOG.debug(String.format(
        "SessionInfo | planId=%s | index=%s | name=%s | %s -> %s | toSend=%d toReceive=%d | sent=%d received=%d ",
        planId,
        sessionInfo.sessionIndex,
        sessionInfo.state.name(),
        host,
        sessionInfo.peer.toString(),
        sessionInfo.getTotalSizeToSend(),
        sessionInfo.getTotalSizeToReceive(),
        sessionInfo.getTotalSizeSent(),
        sessionInfo.getTotalSizeReceived()
    ));

    try {
      streamSessions.initSession(cluster, planId, host, sessionInfo, timestamp);
    } catch (ReaperException e) {
      LOG.error("Handling SessionInfo failed");
    }
  }

  /**
   * handle an update to a streaming session.
   */
  private void handleProgressInfo(String cluster, String host, String planId, CompositeData payload, long timeStamp) {
    ProgressInfo progressInfo = ProgressInfoCompositeData.fromCompositeData(payload);

    LOG.debug(String.format(
        "ProgressInfo | planId=%s | %s -> %s | sessionIndex=%d | direction=%s | currentBytes=%d | totalBytes = %d",
        planId,
        host,
        progressInfo.peer,
        progressInfo.sessionIndex,
        progressInfo.direction,
        progressInfo.currentBytes,
        progressInfo.totalBytes
    ));

    try {
      streamSessions.updateSession(cluster, planId, host, progressInfo, timeStamp);
    } catch (ReaperException e) {
      LOG.error("Handling ProgressInfo failed");
    }
  }

  /**
   * Handle concluding a streaming session.
   */
  private void handleSessionCompleted(String cluster,
                                      String host,
                                      String planId,
                                      CompositeData payload,
                                      long timeStamp
  ) {
    String peer = payload.get("peer").toString();
    Boolean success = (Boolean) payload.get("success");

    LOG.debug(String.format(
        "SessionCompleteEvent | planId=%s | %s -> %s | success=%b",
        planId,
        host,
        peer,
        success
    ));

    try {
      streamSessions.finalizeSession(cluster, planId, host, peer, success, timeStamp);
    } catch (ReaperException e) {
      LOG.error("Handling SessionCompleted failed");
    }
  }

  public Map<String, StreamSession> listStreams(String clusterName) throws ReaperException {
    return streamSessions.get(clusterName);
  }

  public List<Stream> listStreams(Node node) throws ReaperException {
    return streamSessions.get(node.getCluster().getName(), node.getHostname());
  }

}
