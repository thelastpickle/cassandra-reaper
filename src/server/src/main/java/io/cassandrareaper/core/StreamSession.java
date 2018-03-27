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

package io.cassandrareaper.core;

import java.util.Map;

import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.collect.Maps;
import org.apache.cassandra.streaming.ProgressInfo;
import org.apache.cassandra.streaming.SessionInfo;


/**
 * This class represents a Cassandra streaming session.
 *
 * <p>Streaming session is identified by an UUID. A streaming session includes a collection of bi-directional data
 * exchanges between two nodes.
 */
public class StreamSession {

  /**
   * The UUID identifying the stream session.
   */
  private String planId;

  /**
   * The streams belonging to this session.
   *
   * <p>Stored in a map keyed by {@link Stream#streamId()} to allow fast lookup of streams needed
   * to handle stream updates.
   */
  private Map<String, Stream> streams;

  public StreamSession(Builder builder) {
    this.planId = builder.planId;
    this.streams = builder.streams;
  }

  public String getPlanId() {
    return planId;
  }

  public Map<String, Stream> getStreams() {
    return streams;
  }

  public Stream getStream(String streamId) {
    return streams.get(streamId);
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format("StreamSession(planId=%s, streams=[", planId));
    streams.forEach((key, value) -> sb.append(String.format("%s: %s, ", key, value.toString())));
    sb.append("]");
    return sb.toString();
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Initializes a new stream within the session.
   *
   * <p>This method is called to handle a notification about new stream being established.
   *
   * @param host originating the notification
   * @param sessionInfo the payload received from Cassandra with the JMX notification
   * @param timestamp of the JMX notification
   * @return itself with the new stream added
   */
  public StreamSession addStream(String host, SessionInfo sessionInfo, long timestamp) {
    Stream newStream = Stream.newStream(host, sessionInfo, timestamp);
    streams.putIfAbsent(newStream.streamId(), newStream);
    return this;
  }

  /**
   * Updates a stream within the session based on the update received from Cassandra.
   *
   * <p>The method assumes the stream has been previously initialized when {@link #addStream(String, SessionInfo, long)}
   * was called. If it wasn't, the method does nothing.
   *
   * <p>If the notification comes with old timestamp, then no update happens.
   *
   * <p>This method is called every time Cassandra sends a stream update, which happens quite a lot.
   *
   * @param host originating the notification
   * @param progressInfo the payload received from Cassandra with the JMX notification
   * @param timeStamp of the JMX notification
   * @return itself with the stream updated
   */
  public StreamSession updateStream(String host, ProgressInfo progressInfo, long timeStamp) {
    String streamId = Stream.streamId(host, progressInfo.peer.toString());

    streams.computeIfPresent(streamId, (id, oldStream) -> {
      // process the payload only if the timestamp is newer
      if (oldStream.getLastUpdated() < timeStamp) {
        if (progressInfo.direction.equals(ProgressInfo.Direction.OUT)) {
          oldStream.setSizeSent(progressInfo);
        }
        if (progressInfo.direction.equals(ProgressInfo.Direction.IN)) {
          oldStream.setSizeReceived(progressInfo);
        }
        oldStream.setLastUpdated(timeStamp);
      }
      return oldStream;
    });
    return this;
  }

  /**
   * Updates the stream to mark it as complete after Cassandra sends the related message.
   *
   * <p>The method assumes the stream has been initialized with {@link #addStream(String, SessionInfo, long)}.
   * If it wasn't, then the method does nothing.
   *
   * <p>If the notification comes with old timestamps, then no update happens.
   *
   * @param host originating the notification
   * @param peer the second node participating in the stream
   * @param success of the streaming session between the two nodes
   * @param timeStamp of the JMX notification
   * @return itself with stream finalized
   */
  public StreamSession finalizeStream(String host, String peer, Boolean success, long timeStamp) {
    String streamId = Stream.streamId(host, peer);
    streams.computeIfPresent(streamId, (id, oldStream) -> {
      if (oldStream.getLastUpdated() < timeStamp) {
        oldStream.setCompleted(true);
        oldStream.setSuccess(success);
      }
      oldStream.setLastUpdated(timeStamp);
      return oldStream;
    });
    return this;
  }

  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "with")
  public static final class Builder {

    private String planId;
    private Map<String, Stream> streams;

    private Builder() {
    }

    public Builder withPlanId(String planId) {
      this.planId = planId;
      return this;
    }

    public Builder withNoStreams() {
      this.streams = Maps.newConcurrentMap();
      return this;
    }

    public StreamSession build() {
      return new StreamSession(this);
    }

  }

  public static StreamSession empty(String planId) {
    return StreamSession.builder()
        .withPlanId(planId)
        .withNoStreams()
        .build();
  }

}
