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

import java.util.List;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.collect.ImmutableList;


/**
 * This class describes the state of a file exchange between two Cassandra nodes.
 */
@JsonDeserialize(builder = Stream.Builder.class)
public final class Stream {

  /**
   * String identifier of this stream.
   *
   * <p>Needs to be unique within a StreamSession.
   */
  private final String id;

  /**
   * Node on the _local_ side of the stream. The JMX notification came from this node.
   */
  private final String host;

  /**
   * Node on the _remote_ side of the stream.
   */
  private final String peer;

  /**
   * The direction of data transfer relative from host.
   */
  private final Direction direction;

  /**
   * Total number of bytes to transfer from host to peer.
   */
  private final long sizeToSend;

  /**
   * Total number of bytes to transfer from peer to host.
   */
  private final long sizeToReceive;

  /**
   * Number of bytes transferred from host to peer. Needed only to make the JSON automagic see this field.
   */
  private final long sizeSent;

  /**
   * Number of bytes transferred from peer to host. Needed only to make the JSON automagic see this field.
   */
  private final long sizeReceived;

  /**
   * Per keyspace.table total number of bytes sent.
   */
  private final List<TableProgress> progressSent ;

  /**
   * Per keyspace.table total number of bytes received.
   */
  private final List<TableProgress> progressReceived;

  /**
   * Timestamp of the last update of this stream.
   */
  private final long lastUpdated;

  /**
   * Indicates if the stream has completed.
   */
  private final boolean completed;

  /**
   * Indicates if the stream has completed successfully.
   */
  private final boolean success;

  public enum Direction {
    IN,
    OUT,
    BOTH
  }

  @JsonDeserialize(builder = TableProgress.Builder.class)
  public static final class TableProgress {
    private final String table;
    private final Long current;
    private final Long total;

    private TableProgress(TableProgress.Builder builder) {
      this.table = builder.table;
      this.current = builder.current;
      this.total = builder.total;
    }

    public String getTable() {
      return table;
    }

    public Long getCurrent() {
      return current;
    }

    public Long getTotal() {
      return total;
    }

    @Override
    public String toString() {
      return String.format("TableProgress(table=%s,current=%d,total=%d", table, current, total);
    }

    public static TableProgress.Builder builder() {
      return new TableProgress.Builder();
    }

    public static final class Builder {
      private String table;
      private Long current;
      private Long total;

      public TableProgress.Builder withTable(String table) {
        this.table = table;
        return this;
      }

      public TableProgress.Builder withCurrent(Long current) {
        this.current = current;
        return this;
      }

      public TableProgress.Builder withTotal(Long total) {
        this.total = total;
        return this;
      }

      public TableProgress build() {
        return new TableProgress(this);
      }
    }

  }

  private Stream(Builder builder) {
    this.id = builder.id;
    this.host = builder.host;
    this.peer = builder.peer;
    this.direction = builder.direction;
    this.sizeToSend = builder.sizeToSend;
    this.sizeToReceive = builder.sizeToReceive;
    this.sizeSent = builder.sizeSent;
    this.sizeReceived = builder.sizeReceived;
    this.progressSent = ImmutableList.copyOf(builder.progressSent);
    this.progressReceived = ImmutableList.copyOf(builder.progressReceived);
    this.lastUpdated = builder.lastUpdated;
    this.completed = builder.completed;
    this.success = builder.success;
  }

  public String getId() {
    return id;
  }

  public String getHost() {
    return host;
  }

  public String getPeer() {
    return peer;
  }

  public Direction getDirection() {
    return direction;
  }

  public long getSizeToSend() {
    return sizeToSend;
  }

  public long getSizeToReceive() {
    return sizeToReceive;
  }

  public long getSizeSent() {
    return sizeSent;
  }

  public long getSizeReceived() {
    return sizeReceived;
  }

  public List<TableProgress> getProgressReceived() {
    return progressReceived;
  }

  public List<TableProgress> getProgressSent() {
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

  public String toString() {
    return String.format(
        "Stream(host=%s, peer=%s, direction=%s, toSend=%d, sent=%d, toReceive=%d, received=%d, progressSent=%s, "
            + "progressReceived=%s, updated=%d, completed=%b",
        host, peer, direction.toString(), sizeToSend, sizeSent, sizeToReceive, sizeReceived, progressSent,
        progressReceived, lastUpdated, completed);
  }

  public static Stream.Builder builder() {
    return new Stream.Builder();
  }

  public static Stream.Builder builder(Stream oldStream) {
    return new Stream.Builder(oldStream);
  }

  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "with")
  public static final class Builder {
    private String id;
    private String host;
    private String peer;
    private Direction direction;
    private long sizeToSend;
    private long sizeToReceive;
    private long sizeSent;
    private long sizeReceived;
    private List<TableProgress> progressSent;
    private List<TableProgress> progressReceived;
    private long lastUpdated;
    private boolean completed;
    private boolean success;

    private Builder() {}

    public Builder(Stream oldStream) {
      this.id = oldStream.getId();
      this.host = oldStream.getHost();
      this.peer = oldStream.getPeer();
      this.direction = oldStream.getDirection();
      this.sizeToSend = oldStream.getSizeToSend();
      this.sizeToReceive = oldStream.getSizeToReceive();
      this.progressSent = oldStream.getProgressSent();
      this.progressReceived = oldStream.getProgressReceived();
      this.lastUpdated = oldStream.getLastUpdated();
      this.completed = oldStream.getCompleted();
      this.success = oldStream.getSuccess();
    }

    public Builder withId(String id) {
      this.id = id;
      return this;
    }

    public Builder withHost(String host) {
      this.host = host;
      return this;
    }

    public Stream.Builder withPeer(String peer) {
      this.peer = peer;
      return this;
    }

    public Stream.Builder withDirection(Direction direction) {
      this.direction = direction;
      return this;
    }

    public Stream.Builder withProgressReceived(List<TableProgress> progressReceived) {
      this.progressReceived = progressReceived;
      return this;
    }

    public Stream.Builder withSizeToReceive(long sizeToReceive) {
      this.sizeToReceive = sizeToReceive;
      return this;
    }

    public Stream.Builder withProgressSent(List<TableProgress> progressSent) {
      this.progressSent = progressSent;
      return this;
    }

    public Stream.Builder withSizeToSend(long sizeToSend) {
      this.sizeToSend = sizeToSend;
      return this;
    }

    public Stream.Builder withSizeSent(long sizeSent) {
      this.sizeSent = sizeSent;
      return this;
    }

    public Stream.Builder withSizeReceived(long sizeReceived) {
      this.sizeReceived =  sizeReceived;
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

}
