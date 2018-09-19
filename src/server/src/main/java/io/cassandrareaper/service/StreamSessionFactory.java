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

package io.cassandrareaper.service;

import io.cassandrareaper.core.Stream;
import io.cassandrareaper.core.StreamSession;

import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.cassandra.streaming.StreamState;


public final class StreamSessionFactory {

  private StreamSessionFactory() {}

  /**
   * This is called when stream info is explicitly pulled from a node.
   *
   * @param host hostname of the node the streams are pulled from.
   * @param streamState parsed payload received from Cassandra.
   * @return stream session.
   */
  public static StreamSession fromStreamState(String host, StreamState streamState) {

    UUID planId = streamState.planId;

    Map<String, Stream> streams = streamState.sessions.stream()
        .map(sessionInfo -> StreamFactory.newStream(host, sessionInfo, System.currentTimeMillis()))
        .collect(Collectors.toMap(Stream::getId, stream -> stream));

    return StreamSession.builder()
        .withPlanId(planId.toString())
        .withStreams(streams)
        .build();
  }
}
