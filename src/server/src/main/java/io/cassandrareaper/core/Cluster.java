/*
 * Copyright 2014-2017 Spotify AB
 * Copyright 2016-2018 The Last Pickle Ltd
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

import java.time.LocalDate;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import com.google.common.base.Preconditions;

public final class Cluster implements Comparable<Cluster> {

  public enum State {
    UNKNOWN,
    ACTIVE,
    UNREACHABLE
  }

  public static final int DEFAULT_JMX_PORT = 7199;

  private final String name;
  private final Optional<String> partitioner; // Full name of the partitioner class
  private final Set<String> seedHosts;
  private final State state;
  private final LocalDate lastContact;
  private final ClusterProperties properties;

  private Cluster(
      String name,
      Optional<String> partitioner,
      Set<String> seedHosts,
      State state,
      LocalDate lastContact,
      ClusterProperties properties) {

    this.name = toSymbolicName(name);
    this.partitioner = partitioner;
    this.seedHosts = seedHosts;
    this.state = state;
    this.lastContact = lastContact;
    this.properties = properties;
  }

  public static Builder builder() {
    return new Builder();
  }

  public Builder with() {
    Builder builder = new Builder()
        .withName(name)
        .withSeedHosts(seedHosts)
        .withState(state)
        .withLastContact(lastContact)
        .withJmxPort(getJmxPort());

    if (partitioner.isPresent()) {
      builder = builder.withPartitioner(partitioner.get());
    }
    return builder;
  }

  public static String toSymbolicName(String name) {
    Preconditions.checkNotNull(name, "cannot turn null into symbolic name");
    return name.toLowerCase().replaceAll("[^a-z0-9_\\-\\.]", "");
  }

  public String getName() {
    return name;
  }

  public Optional<String> getPartitioner() {
    return partitioner;
  }

  public Set<String> getSeedHosts() {
    return seedHosts;
  }

  public int getJmxPort() {
    return properties.getJmxPort();
  }

  public State getState() {
    return state;
  }

  public LocalDate getLastContact() {
    return lastContact;
  }

  public ClusterProperties getProperties() {
    return properties;
  }

  @Override
  public int compareTo(Cluster other) {
    return this.getState() == other.getState()
        ? this.getName().compareTo(other.getName())
        : this.getState().compareTo(other.getState());
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    Cluster cluster = (Cluster) obj;
    return Objects.equals(name, cluster.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }

  public static final class Builder {

    private String name;
    private String partitioner;
    private Set<String> seedHosts;
    private State state = State.UNKNOWN;
    private LocalDate lastContact = LocalDate.MIN;
    private final ClusterProperties.Builder properties = ClusterProperties.builder().withJmxPort(DEFAULT_JMX_PORT);

    private Builder() {
    }

    public Builder withName(String name) {
      Preconditions.checkState(null == this.name);
      this.name = name;
      return this;
    }

    public Builder withPartitioner(String partitioner) {
      Preconditions.checkState(null == this.partitioner);
      this.partitioner = partitioner;
      return this;
    }

    public Builder withSeedHosts(Set<String> seedHosts) {
      Preconditions.checkArgument(!seedHosts.isEmpty());
      this.seedHosts = seedHosts;
      return this;
    }

    public Builder withState(State state) {
      Preconditions.checkNotNull(state);
      this.state = state;
      return this;
    }

    public Builder withLastContact(LocalDate lastContact) {
      Preconditions.checkNotNull(lastContact);
      this.lastContact = lastContact;
      return this;
    }

    public Builder withJmxPort(int jmxPort) {
      this.properties.withJmxPort(jmxPort);
      return this;
    }

    public Cluster build() {
      Preconditions.checkNotNull(name);
      Preconditions.checkNotNull(seedHosts);

      return new Cluster(name, Optional.ofNullable(partitioner), seedHosts, state, lastContact, properties.build());
    }
  }
}
