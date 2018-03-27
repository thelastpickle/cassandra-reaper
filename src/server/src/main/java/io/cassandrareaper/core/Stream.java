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
 * This class describes the state of a file exchange between two Cassandra nodes.
 */
public final class Stream {

  /**
   * Node on the _local_ side of the stream. The JMX notification came from this node.
   */
  private String host;

  /**
   * Node on the _remote_ side of the stream.
   */
  private String peer;

  /**
   * Total number of bytes to transfer from host to peer.
   */
  private long sizeToSend;

  /**
   * Total number of bytes to transfer from peer to host.
   */
  private long sizeToReceive;

  /**
   * Per-file progress from host to peer.
   */
  private Map<String, FileProgress> progressSent;

  /**
   * Per-file progress from peer to host.
   */
  private Map<String, FileProgress> progressReceived;

  /**
   * Timestamp of the last JMX notification used to update this stream.
   */
  private long lastUpdated;

  /**
   * Indicates if the stream has completed.
   */
  private boolean completed;

  /**
   * Indicates if the stream has completed successfully.
   */
  private boolean success;

  /**
   * Number of bytes transferred from host to peer. Needed only to make the JSON automagic see this field.
   */
  private long sizeSent;

  /**
   * Number of bytes transferred from peer to host. Needed only to make the JSON automagic see this field.
   */
  private long sizeReceived;

  private Stream(Builder builder) {
    this.host = builder.host;
    this.peer = builder.peer;
    this.sizeToSend = builder.sizeToSend;
    this.sizeToReceive = builder.sizeToReceive;
    this.progressSent = builder.progressSent;
    this.progressReceived = builder.progressReceived;
    this.lastUpdated = builder.lastUpdated;
    this.completed = builder.completed;
    this.success = builder.success;
  }

  public String getHost() {
    return host;
  }

  public String getPeer() {
    return peer;
  }

  public long getSizeToSend() {
    return sizeToSend;
  }

  public long getSizeToReceive() {
    return sizeToReceive;
  }

  /**
   * Computes number of bytes currently sent from host to peer.
   * @return sum of bytes sent across all files in this stream.
   */
  public long getSizeSent() {
    return this.progressSent.values().stream()
        .map(fileProgress -> fileProgress.currentBytes)
        .mapToLong(Long::longValue).sum();
  }

  /**
   * Computes the number of bytes currently received by host from peer.
   * @return sum of bytes received across all files in this stream.
   */
  public long getSizeReceived() {
    return this.progressReceived.values().stream()
        .map(fileProgress -> fileProgress.currentBytes)
        .mapToLong(Long::longValue).sum();
  }

  public Map<String, FileProgress> getProgressReceived() {
    return progressReceived;
  }

  public Map<String, FileProgress> getProgressSent() {
    return progressSent;
  }

  public long getLastUpdated() {
    return lastUpdated;
  }

  public boolean getCompleted() {
    return completed;
  }

  public boolean getSuccess() {
    return success;
  }

  public void setSizeSent(ProgressInfo progressInfo) {
    progressSent.compute(progressInfo.fileName, (fileName, oldProgress) -> toFileProgress(progressInfo));
  }

  public void setSizeReceived(ProgressInfo progressInfo) {
    progressReceived.compute(progressInfo.fileName, (fileName, oldProgress) -> toFileProgress(progressInfo));
  }

  public void setLastUpdated(long lastUpdated) {
    this.lastUpdated = lastUpdated;
  }

  public void setCompleted(boolean completed) {
    this.completed = completed;
  }

  public void setSuccess(boolean success) {
    this.success = success;
  }

  public String streamId() {
    return streamId(this.host, this.peer);
  }

  public static String streamId(String host, String peer) {
    return String.format("%s->%s", host, peer.replaceAll("/", ""));
  }

  public String toString() {
    return String.format(
        "Stream(host=%s, peer=%s, toSend=%d, sent=%d, toReceive=%d, received=%d, updated=%d, completed=%b",
        host, peer, sizeToSend, getSizeSent(), sizeToReceive, getSizeReceived(), lastUpdated, completed);
  }

  public static Stream.Builder builder() {
    return new Stream.Builder();
  }

  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "with")
  public static final class Builder {
    private String host;
    private String peer;
    private long sizeToSend;
    private long sizeToReceive;
    private Map<String, FileProgress> progressSent;
    private Map<String, FileProgress> progressReceived;
    private long lastUpdated;
    private boolean completed;
    private boolean success;

    private Builder() {}

    public Builder withHost(String host) {
      this.host = host;
      return this;
    }

    public Stream.Builder withPeer(String peer) {
      this.peer = peer;
      return this;
    }

    public Stream.Builder withProgressReceived(Map<String, FileProgress> progressReceived) {
      this.progressReceived = progressReceived;
      return this;
    }

    public Stream.Builder withSizeToReceive(long sizeToReceive) {
      this.sizeToReceive = sizeToReceive;
      return this;
    }

    public Stream.Builder withProgressSent(Map<String, FileProgress> progressSent) {
      this.progressSent = progressSent;
      return this;
    }

    public Stream.Builder withSizeToSend(long sizeToSend) {
      this.sizeToSend = sizeToSend;
      return this;
    }

    public Stream.Builder withLastUpdated(long lastUpdated) {
      this.lastUpdated = lastUpdated;
      return this;
    }

    public Stream.Builder withCompleted(boolean completed) {
      this.completed = completed;
      return this;
    }

    public Stream.Builder withSuccess(boolean success) {
      this.success = success;
      return this;
    }

    public Stream build() {
      return new Stream(this);
    }

  }

  public static class FileProgress {
    public long totalBytes;
    public long currentBytes;

    public FileProgress(long totalBytes, long currentBytes) {
      this.totalBytes = totalBytes;
      this.currentBytes = currentBytes;
    }
  }

  private static FileProgress toFileProgress(ProgressInfo pi) {
    return new FileProgress(pi.totalBytes, pi.currentBytes);
  }

  public static Stream newStream(String host, SessionInfo sessionInfo, long timeStamp) {

    // it'd be cool to pre-populate this with file names as SessionInfo should have them, but they always come empty.
    Map<String, FileProgress> progressSent = Maps.newConcurrentMap();
    Map<String, FileProgress> progressReceived = Maps.newConcurrentMap();

    return Stream.builder()
        .withHost(host)
        .withPeer(sessionInfo.connecting.toString().replaceAll("/", ""))
        .withSizeToReceive(sessionInfo.getTotalSizeToReceive())
        .withSizeToSend(sessionInfo.getTotalSizeToSend())
        .withProgressReceived(progressReceived)
        .withProgressSent(progressSent)
        .withLastUpdated(timeStamp)
        .withCompleted(false)
        .withSuccess(false)
        .build();
  }

}
