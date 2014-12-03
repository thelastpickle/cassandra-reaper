package com.spotify.reaper.core;

import java.util.Set;

public class Cluster {

  private final String name;
  private final String partitioner; // Name of the partitioner class
  private final Set<String> seedHosts;

  public String getPartitioner() {
    return partitioner;
  }

  public String getName() {
    return name;
  }

  public Set<String> getSeedHosts() {
    return seedHosts;
  }

  private Cluster(Builder builder) {
    this.partitioner = builder.partitioner;
    this.name = builder.name;
    this.seedHosts = builder.seedHosts;
  }

  public static class Builder {

    private String partitioner;
    private String name;
    private Set<String> seedHosts;

    public Builder partitioner(String partitioner) {
      this.partitioner = partitioner;
      return this;
    }

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    public Builder seedHosts(Set<String> seedHosts) {
      this.seedHosts = seedHosts;
      return this;
    }

    public Cluster build() {
      return new Cluster(this);
    }
  }
}
