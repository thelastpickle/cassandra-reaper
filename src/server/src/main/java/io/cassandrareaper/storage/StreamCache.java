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

package io.cassandrareaper.storage;

import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Stream;
import io.cassandrareaper.core.StreamSession;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;
import org.apache.cassandra.streaming.ProgressInfo;
import org.apache.cassandra.streaming.SessionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamCache {

  private static final Logger LOG = LoggerFactory.getLogger(StreamCache.class);

  /**
   * Cluster -> PlanId -> StreamSession
   */
  private LoadingCache<String, ConcurrentMap<String, StreamSession>> plansByCluster = CacheBuilder.newBuilder()
      .maximumSize(1000L)
      .expireAfterWrite(24L, TimeUnit.HOURS)
      .build(new CacheLoader<String, ConcurrentMap<String, StreamSession>>() {
        public ConcurrentMap<String, StreamSession> load(String cluster) {
          return emptySessionsForCluster(cluster);
        }
      });

  private static ConcurrentMap<String, StreamSession> emptySessionsForCluster(String cluster) {
    return Maps.newConcurrentMap();
  }

  public void initSession(String clusterName, String planId, String host, SessionInfo sessionInfo,long timestamp)
      throws ReaperException {
    try {
      ConcurrentMap<String, StreamSession> oldSessions = plansByCluster.get(clusterName);
      oldSessions.computeIfAbsent(planId, StreamSession::empty);
      oldSessions.computeIfPresent(
          planId,
          (id, oldSession) -> oldSession.addStream(host, sessionInfo, timestamp)
      );
    } catch (ExecutionException e) {
      LOG.error("Failed handling SessionInfo: {}", e.getMessage());
      throw new ReaperException(e);
    }
  }

  public void updateSession(String clusterName, String planId, String host, ProgressInfo progressInfo, long timeStamp)
      throws ReaperException {
    try {
      ConcurrentMap<String, StreamSession> oldSessions = plansByCluster.get(clusterName);
      oldSessions.computeIfAbsent(planId, StreamSession::empty);
      oldSessions.computeIfPresent(
          planId,
          (id, oldSession) -> oldSession.updateStream(host, progressInfo, timeStamp)
      );

    } catch (ExecutionException e) {
      LOG.error("Failed handling ProgressInfo: {}", e.getMessage());
      throw new ReaperException(e);
    }
  }

  public void finalizeSession(String clusterName,
                              String planId,
                              String host,
                              String peer,
                              Boolean success,
                              long timeStamp
  ) throws ReaperException {
    try {
      ConcurrentMap<String, StreamSession> oldSessions = plansByCluster.get(clusterName);
      oldSessions.computeIfPresent(
          planId,
          (id, oldSession) -> oldSession.finalizeStream(host, peer, success, timeStamp)
      );

    } catch (ExecutionException e) {
      LOG.error("Failed handling session completion: {}", e.getMessage());
      throw new ReaperException(e);
    }
  }

  public Map<String, StreamSession> get(String clusterName) throws ReaperException {
    try {
      return plansByCluster.get(clusterName, Maps::newConcurrentMap);
    } catch (ExecutionException e) {
      LOG.error("Failed getting sessions for cluster {}: {}", clusterName, e.getMessage());
      throw new ReaperException(e);
    }
  }

  public List<Stream> get(String cluster, String node) throws ReaperException {
    try {
      ConcurrentMap<String, StreamSession> clusterStreams = plansByCluster.get(cluster, Maps::newConcurrentMap);
      return filterStreamsFromNode(clusterStreams, node);
    } catch (ExecutionException e) {
      LOG.error("Failed getting sessions for cluster {} and node {}: {}", cluster, node, e.getMessage());
      throw new ReaperException(e);
    }
  }

  private List<Stream> filterStreamsFromNode(ConcurrentMap<String, StreamSession> clusterStreams, String node) {
    List<Stream> streamsFromNode =
        // from all known StreamSessions for this cluster
        clusterStreams.values().stream()
        // take the Streams
        .map(streamSession -> streamSession.getStreams().entrySet())
        // concatenate Streams from all the sessions
        .flatMap(Collection::stream)
        // streams are a map of streamId -> Stream, we need just the Stream
        .map(Map.Entry::getValue)
        // finally, we want only the streams featuring the given node
        .filter(stream -> stream.getHost().equals(node) || stream.getPeer().equals(node))
        // and we want them as a List
        .collect(Collectors.toList());

    return streamsFromNode;
  }

}
