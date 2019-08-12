/*
 * Copyright 2018-2018 The Last Pickle Ltd
 *
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

package io.cassandrareaper.core;

import java.util.Map;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.collect.ImmutableMap;


/**
 * This class represents a Cassandra streaming session.
 *
 * <p>Streaming session is identified by an UUID. A streaming session includes a collection of bi-directional data
 * exchanges between two nodes.
 */
@JsonDeserialize(builder = StreamSession.Builder.class)
public final class StreamSession {

  /**
   * The UUID identifying the stream session.
   */
  private final String planId;

  /**
   * The streams belonging to this session.
   *
   * <p>Stored in a map keyed by {@link Stream#id()} to allow fast lookup of streams needed
   * to handle stream updates.
   */
  private final ImmutableMap<String, Stream> streams;

  private StreamSession(Builder builder) {
    this.planId = builder.planId;
    this.streams = ImmutableMap.copyOf(builder.streams);
  }

  public String getPlanId() {
    return planId;
  }

  public ImmutableMap<String, Stream> getStreams() {
    return streams;
  }

  public Stream getStream(String streamId) {
    return streams.get(streamId);
  }

  public Stream getStream(String streamId, Stream defaultValue) {
    return streams.getOrDefault(streamId, defaultValue);
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

  public static Builder builder(StreamSession oldSession) {
    return new Builder(oldSession);
  }

  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "with")
  public static final class Builder {

    private String planId;
    private Map<String, Stream> streams;

    private Builder() {
    }

    private Builder(StreamSession oldSession) {
      this.planId = oldSession.getPlanId();
      this.streams = oldSession.getStreams();
    }

    public Builder withPlanId(String planId) {
      this.planId = planId;
      return this;
    }

    public Builder withStreams(Map<String, Stream> streams) {
      this.streams = streams;
      return this;
    }

    public StreamSession build() {
      return new StreamSession(this);
    }

  }

}
