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

import java.util.Optional;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import org.joda.time.DateTime;

@JsonDeserialize(builder = Snapshot.Builder.class)
public final class Snapshot {
  private String host;
  private String name;
  private String keyspace;
  private String table;
  private Double trueSize;
  private Double sizeOnDisk;
  private Optional<String> owner;
  private Optional<String> cause;
  private Optional<DateTime> creationDate;
  private Optional<String> clusterName;

  private Snapshot(Builder builder) {
    this.name = builder.name;
    this.host = builder.host;
    this.keyspace = builder.keyspace;
    this.table = builder.table;
    this.trueSize = builder.trueSize;
    this.sizeOnDisk = builder.sizeOnDisk;
    this.owner = Optional.ofNullable(builder.owner);
    this.cause = Optional.ofNullable(builder.cause);
    this.creationDate = Optional.ofNullable(builder.creationDate);
    this.clusterName = Optional.ofNullable(builder.clusterName);
  }

  public String getName() {
    return name;
  }

  public String getHost() {
    return host;
  }

  public String getKeyspace() {
    return keyspace;
  }

  public String getTable() {
    return table;
  }

  public Double getTrueSize() {
    return trueSize;
  }

  public Double getSizeOnDisk() {
    return sizeOnDisk;
  }

  public Optional<String> getOwner() {
    return owner;
  }

  public Optional<String> getCause() {
    return cause;
  }

  public Optional<DateTime> getCreationDate() {
    return creationDate;
  }

  public String getClusterName() {
    return clusterName.orElse("");
  }

  public String toString() {
    return "{name="
        + name
        + ", host="
        + host
        + ", keyspace="
        + keyspace
        + ", table="
        + table
        + ", true size="
        + trueSize
        + ", size on disk="
        + sizeOnDisk
        + "}";
  }

  /**
   * Creates builder to build {@link Snapshot}.
   *
   * @return created builder
   */
  public static Builder builder() {
    return new Builder();
  }

  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "with")
  public static final class Builder {
    private String name;
    private String host;
    private String keyspace;
    private String table;
    private Double trueSize;
    private Double sizeOnDisk;
    private String owner;
    private String cause;
    private DateTime creationDate;
    private String clusterName;

    private Builder() {}

    public Builder withName(String name) {
      this.name = name;
      return this;
    }

    public Builder withHost(String host) {
      this.host = host;
      return this;
    }

    public Builder withKeyspace(String keyspace) {
      this.keyspace = keyspace;
      return this;
    }

    public Builder withTable(String table) {
      this.table = table;
      return this;
    }

    public Builder withTrueSize(Double trueSize) {
      this.trueSize = trueSize;
      return this;
    }

    public Builder withSizeOnDisk(Double sizeOnDisk) {
      this.sizeOnDisk = sizeOnDisk;
      return this;
    }

    public Builder withCause(String cause) {
      this.cause = cause;
      return this;
    }

    public Builder withOwner(String owner) {
      this.owner = owner;
      return this;
    }

    public Builder withClusterName(String clusterName) {
      this.clusterName = clusterName;
      return this;
    }

    public Builder withCreationDate(DateTime creationDate) {
      this.creationDate = creationDate;
      return this;
    }

    public Snapshot build() {
      return new Snapshot(this);
    }
  }
}
