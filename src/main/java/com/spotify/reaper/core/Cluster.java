package com.spotify.reaper.core;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Set;

public class Cluster {

  @JsonProperty
  private final String name;
  private final String partitioner; // Name of the partitioner class
  private final Set<String> seedHosts;

  public String getName() {
    return name;
  }

  public String getPartitioner() {
    return partitioner;
  }

  public Set<String> getSeedHosts() {
    return seedHosts;
  }

  private Cluster(Builder builder) {
    this.name = builder.name;
    this.partitioner = builder.partitioner;
    this.seedHosts = builder.seedHosts;
  }

  public static class Builder {

    public final String name;
    public final String partitioner;
    private final Set<String> seedHosts;

    public Builder(String name, String partitioner, Set<String> seedHosts) {
      this.name = name;
      this.partitioner = partitioner;
      this.seedHosts = seedHosts;
    }

    public Builder addSeedHost(String seedHost) {
      seedHosts.add(seedHost);
      return this;
    }

    public Cluster build() {
      return new Cluster(this);
    }
  }
}
