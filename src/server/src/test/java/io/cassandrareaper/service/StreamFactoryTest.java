/*
 * Copyright 2018-2019 The Last Pickle Ltd
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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.UUID;

import com.google.common.collect.ImmutableSet;
import org.apache.cassandra.streaming.ProgressInfo;
import org.apache.cassandra.streaming.SessionInfo;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.streaming.StreamSummary;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public final class StreamFactoryTest {

  @Test
  public void testCountProgressPerTableWithTable() throws UnknownHostException {
    UUID cfid = UUID.randomUUID();
    int files = 3;
    int totalSize = 2048;
    StreamSummary streamSummary = new StreamSummary(cfid, files, totalSize);

    InetAddress peer = InetAddress.getByName("127.0.0.1");
    int index = 0;
    InetAddress connecting = InetAddress.getByName("127.0.0.2");
    ImmutableSet<StreamSummary> receivingSummaries = ImmutableSet.of(streamSummary);
    ImmutableSet<StreamSummary> sendingSummaries = ImmutableSet.of();

    SessionInfo sessionInfo
        = new SessionInfo(peer, index, connecting, receivingSummaries, sendingSummaries, StreamSession.State.STREAMING);

    String file1 = "/cass/data/keyspace1/standard1-af5311f0633a11e89d71710c22f847e7/lb-4-big-Data.db";
    String file2 = "/cass/data/keyspace1/standard1-af5311f0633a11e89d71710c22f847e7/lb-5-big-Data.db";
    sessionInfo.updateProgress(new ProgressInfo(peer, index, file1, ProgressInfo.Direction.OUT, 512, 1024));
    sessionInfo.updateProgress(new ProgressInfo(peer, index, file2, ProgressInfo.Direction.OUT, 512, 1024));

    UUID planId = UUID.randomUUID();
    ImmutableSet<SessionInfo> sessionInfos = ImmutableSet.of(sessionInfo);
    StreamState streamState = new StreamState(planId, "descr", sessionInfos);

    io.cassandrareaper.core.StreamSession streamSession
        = StreamSessionFactory.fromStreamState(peer.toString(), streamState);

    assertEquals(1, streamSession.getStreams().size());

    List<Stream.TableProgress> progressSent = streamSession.getStreams().values().asList().get(0).getProgressSent();
    assertEquals(1, progressSent.size());

    Stream.TableProgress tableProgress = progressSent.get(0);
    assertEquals("keyspace1.standard1", tableProgress.getTable());
    assertEquals(Long.valueOf(1024), tableProgress.getCurrent());
    assertEquals(Long.valueOf(2048), tableProgress.getTotal());
  }

  @Test
  public void testCountProgressPerTableWhenReceiving() throws UnknownHostException {
    UUID cfid = UUID.randomUUID();
    int files = 1;
    int totalSize = 1024;
    StreamSummary streamSummary = new StreamSummary(cfid, files, totalSize);

    InetAddress peer = InetAddress.getByName("127.0.0.1");
    int index = 0;
    InetAddress connecting = InetAddress.getByName("127.0.0.2");
    ImmutableSet<StreamSummary> receivingSummaries = ImmutableSet.of(streamSummary);
    ImmutableSet<StreamSummary> sendingSummaries = ImmutableSet.of();

    SessionInfo sessionInfo
        = new SessionInfo(peer, index, connecting, receivingSummaries, sendingSummaries, StreamSession.State.STREAMING);

    // this is the important part - when receiving, the absolute path is not known
    String file1 = "keyspace1/standard1";
    sessionInfo.updateProgress(new ProgressInfo(peer, index, file1, ProgressInfo.Direction.IN, 512, 1024));

    UUID planId = UUID.randomUUID();
    ImmutableSet<SessionInfo> sessionInfos = ImmutableSet.of(sessionInfo);
    StreamState streamState = new StreamState(planId, "descr", sessionInfos);

    io.cassandrareaper.core.StreamSession streamSession
        = StreamSessionFactory.fromStreamState(peer.toString(), streamState);

    assertEquals(1, streamSession.getStreams().size());

    List<Stream.TableProgress> progressSent = streamSession.getStreams().values().asList().get(0).getProgressReceived();
    assertEquals(1, progressSent.size());

    Stream.TableProgress tableProgress = progressSent.get(0);
    assertEquals("keyspace1.standard1", tableProgress.getTable());
    assertEquals(Long.valueOf(512), tableProgress.getCurrent());
    assertEquals(Long.valueOf(1024), tableProgress.getTotal());
  }

  @Test
  public void testCountProgressPerTableWithMultipleTables() throws UnknownHostException {
    UUID cfid = UUID.randomUUID();
    int files = 3;
    int totalSize = 2048;
    StreamSummary streamSummary = new StreamSummary(cfid, files, totalSize);

    InetAddress peer = InetAddress.getByName("127.0.0.1");
    int index = 0;
    InetAddress connecting = InetAddress.getByName("127.0.0.2");
    ImmutableSet<StreamSummary> receivingSummaries = ImmutableSet.of(streamSummary);
    ImmutableSet<StreamSummary> sendingSummaries = ImmutableSet.of();

    SessionInfo sessionInfo
        = new SessionInfo(peer, index, connecting, receivingSummaries, sendingSummaries, StreamSession.State.STREAMING);

    String file1 = "/cass/data/keyspace1/standard1-af5311f0633a11e89d71710c22f847e7/lb-4-big-Data.db";
    sessionInfo.updateProgress(new ProgressInfo(peer, index, file1, ProgressInfo.Direction.OUT, 32, 1024));
    String file2 = "/cass/data/keyspace1/standard1-af5311f0633a11e89d71710c22f847e7/lb-5-big-Data.db";
    sessionInfo.updateProgress(new ProgressInfo(peer, index, file2, ProgressInfo.Direction.OUT, 64, 1024));
    String file3 = "/cass/data/keyspace1/counter1-af5311f0633a11e89d71710c22f847e7/lb-4-big-Data.db";
    sessionInfo.updateProgress(new ProgressInfo(peer, index, file3, ProgressInfo.Direction.OUT, 512, 512));
    String file4 = "/cass/data/keyspace1/counter1-af5311f0633a11e89d71710c22f847e7/lb-5-big-Data.db";
    sessionInfo.updateProgress(new ProgressInfo(peer, index, file4, ProgressInfo.Direction.OUT, 128, 512));

    UUID planId = UUID.randomUUID();
    ImmutableSet<SessionInfo> sessionInfos = ImmutableSet.of(sessionInfo);
    StreamState streamState = new StreamState(planId, "descr", sessionInfos);

    io.cassandrareaper.core.StreamSession streamSession
        = StreamSessionFactory.fromStreamState(peer.toString(), streamState);

    assertEquals(1, streamSession.getStreams().size());

    List<Stream.TableProgress> progressSent = streamSession.getStreams().values().asList().get(0).getProgressSent();
    assertEquals(2, progressSent.size());

    Stream.TableProgress standardTableProgess = progressSent.stream()
        .filter(ps -> ps.getTable().equals("keyspace1.standard1"))
        .findFirst()
        .get();
    assertEquals(Long.valueOf(96), standardTableProgess.getCurrent());
    assertEquals(Long.valueOf(2048), standardTableProgess.getTotal());

    Stream.TableProgress counterTableProgress = progressSent.stream()
        .filter(ps -> ps.getTable().equals("keyspace1.counter1"))
        .findFirst()
        .get();
    assertEquals(Long.valueOf(640), counterTableProgress.getCurrent());
    assertEquals(Long.valueOf(1024), counterTableProgress.getTotal());
  }

}
