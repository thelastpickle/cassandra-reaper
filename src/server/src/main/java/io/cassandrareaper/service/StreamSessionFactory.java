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


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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

  public static StreamSession testSession() {

    ImmutableMap<String, Stream> streams = ImmutableMap.of(
        // receiving, not complete
        "127.0.0.3-IN-127.0.0.1",
        StreamFactory.testStream("127.0.0.3-IN-127.0.0.1",
                                 "127.0.0.3", "127.0.0.1", "IN",
                                 1024L, 512L, ImmutableList.of(new Stream.TableProgress("ks.t", 512L, 1024L)),
                                 0L, 0L, ImmutableList.of(),
                                 false, true),

        // sending, not complete
        "127.0.0.3-OUT-127.0.0.4",
        StreamFactory.testStream("127.0.0.3-OUT-127.0.0.4",
                                 "127.0.0.3", "127.0.0.4", "OUT",
                                 0L, 0L, ImmutableList.of(),
                                 1024L, 128L, ImmutableList.of(new Stream.TableProgress("ks.t", 128L, 1024L)),
                                 false, true),

        // receiving, complete
        "127.0.0.3-IN-127.0.0.6",
        StreamFactory.testStream("127.0.0.3-IN-127.0.0.6",
                                 "127.0.0.3", "127.0.0.6", "IN",
                                 1024L, 1024L, ImmutableList.of(new Stream.TableProgress("ks.t", 1024L, 1024L)),
                                 0L, 0L, ImmutableList.of(),
                                 true, true),

        // both sending and receiving, not complete
        "127.0.0.3-BOTH-127.0.0.5",
        StreamFactory.testStream("127.0.0.3-BOTH-127.0.0.5",
                                 "127.0.0.3", "127.0.0.5", "BOTH",
                                 2048L, 512L, ImmutableList.of(new Stream.TableProgress("ks.t", 512L, 2048L)),
                                 1024L, 128L, ImmutableList.of(new Stream.TableProgress("ks.t", 128L, 1024L)),
                                 false, true),

        // sending, error
        "127.0.0.3-IN-127.0.0.7",
        StreamFactory.testStream("127.0.0.3-IN-127.0.0.7",
                                 "127.0.0.3", "127.0.0.6", "IN",
                                 1024L, 512L, ImmutableList.of(new Stream.TableProgress("ks.t", 512L, 1024L)),
                                 0L, 0L, ImmutableList.of(),
                                 false, false)
    );

    return StreamSession.builder()
        .withPlanId("41d6f5b0-7609-11e8-885c-a1f7917fbcdd")
        .withStreams(streams)
        .build();
  }

  public static StreamSession testSession2() {
    ImmutableMap<String, Stream> streams = ImmutableMap.of(
        // sending, not complete
        "127.0.0.3-OUT-192.168.0.1",
        StreamFactory
            .testStream("127.0.0.3-OUT-192.168.0.1",
                        "127.0.0.3", "192.168.0.1", "OUT",
                        0L, 0L, ImmutableList.of(),
                        57484657L, 13234740L, ImmutableList.of(new Stream.TableProgress("ks.t", 13234740L, 57484657L)),
                        false, true),

        // receiving, not complete
        "127.0.0.3-IN-192.168.0.2",
        StreamFactory.testStream("127.0.0.3-IN-192.168.0.2",
                                 "127.0.0.3", "192.168.0.2", "IN",
                                 1024L, 512L, ImmutableList.of(new Stream.TableProgress("ks.t", 512L, 1024L)),
                                 0L, 0L, ImmutableList.of(),
                                 false, true),

        // multi-table stream
        "127.0.0.3-IN-127.0.0.8",
        StreamFactory.testStream("127.0.0.3-IN-127.0.0.8",
                                 "127.0.0.3", "127.0.0.8", "IN",
                                 2048L, 650L, ImmutableList.of(new Stream.TableProgress("ks.t1", 512L, 1024L),
                                                               new Stream.TableProgress("ks.t2", 128L, 1024L)),
                                 0L, 0L, ImmutableList.of(),
                                 false, false)
    );

    return StreamSession.builder()
        .withPlanId("710aa4bb-3041-4214-a8cd-a9d34682a595")
        .withStreams(streams)
        .build();
  }

}
