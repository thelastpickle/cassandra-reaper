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

import io.cassandrareaper.core.Stream;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.streaming.ProgressInfo;
import org.apache.cassandra.streaming.SessionInfo;
import org.apache.cassandra.streaming.StreamSession;


public final class StreamFactory {

  private StreamFactory() {}

  public static Stream newStream(String host, SessionInfo sessionInfo, long timeStamp) {

    final String peer = sessionInfo.peer.toString().replaceAll("/", "");

    final long sizeSent = sessionInfo.getTotalSizeSent();
    final long sizeReceived = sessionInfo.getTotalSizeReceived();

    Stream.Direction direction = Stream.Direction.BOTH;
    if (sizeReceived == 0 && sizeSent != 0) {
      direction = Stream.Direction.IN;
    }
    if (sizeReceived != 0 && sizeSent == 0) {
      direction = Stream.Direction.OUT;
    }

    final String streamId = String.format("%s-%s-%s", host, direction, peer);

    final List<Stream.TableProgress> progressReceived = countProgressPerTable(sessionInfo.getReceivingFiles());
    final List<Stream.TableProgress> progressSent = countProgressPerTable(sessionInfo.getSendingFiles());

    return Stream.builder()
        .withId(streamId)
        .withHost(host)
        .withPeer(peer)
        .withDirection(direction)
        .withSizeToReceive(sessionInfo.getTotalSizeToReceive())
        .withSizeToSend(sessionInfo.getTotalSizeToSend())
        .withProgressSent(progressSent)
        .withProgressReceived(progressReceived)
        .withSizeSent(sizeSent)
        .withSizeReceived(sizeReceived)
        .withLastUpdated(timeStamp)
        .withCompleted(sessionInfo.state == StreamSession.State.COMPLETE)
        .withSuccess(!sessionInfo.isFailed())
        .build();
  }

  private static List<Stream.TableProgress> countProgressPerTable(Collection<ProgressInfo> progressPerFile) {
    return progressPerFile.stream()
        .map(StreamFactory::getTableProgressFromFile)
        // group by table
        .collect(Collectors.groupingBy(Stream.TableProgress::getTable))
        .entrySet()
        .stream()
        .map(group -> sumTableProgress(group.getKey(), group.getValue()))
        .collect(Collectors.toList());
  }

  private static Stream.TableProgress getTableProgressFromFile(ProgressInfo progressInfo) {
    Descriptor descriptor = Descriptor.fromFilename(progressInfo.fileName);
    String ksTable = String.format("%s.%s", descriptor.ksname, descriptor.cfname);
    long currentBytes = progressInfo.currentBytes;
    long totalBytes = progressInfo.totalBytes;
    return new Stream.TableProgress(ksTable, currentBytes, totalBytes);
  }

  private static Stream.TableProgress sumTableProgress(String table,
                                                       List<Stream.TableProgress> fileProgresses
  ) {
    long sumCurrent = fileProgresses.stream()
        .map(Stream.TableProgress::getCurrent)
        .mapToLong(Long::longValue)
        .sum();
    long sumTotal = fileProgresses.stream()
        .map(Stream.TableProgress::getTotal)
        .mapToLong(Long::longValue)
        .sum();
    return new Stream.TableProgress(table, sumCurrent, sumTotal);
  }

  public static Stream testStream(String id,
                                  String host,
                                  String peer,
                                  String direction,
                                  Long sizeToReceive,
                                  Long sizeReceived,
                                  List<Stream.TableProgress> progressReceived,
                                  Long sizeToSend,
                                  Long sizeSent,
                                  List<Stream.TableProgress> progressSent,
                                  boolean completed,
                                  boolean success
  ) {
    return Stream.builder()
        .withId(id)
        .withHost(host)
        .withPeer(peer)
        .withDirection(Stream.Direction.valueOf(direction))
        .withSizeToReceive(sizeToReceive)
        .withSizeReceived(sizeReceived)
        .withSizeToSend(sizeToSend)
        .withSizeSent(sizeSent)
        .withProgressReceived(progressReceived)
        .withProgressSent(progressSent)
        .withLastUpdated(System.currentTimeMillis())
        .withCompleted(completed)
        .withSuccess(success)
        .build();
  }

}
