package com.spotify.reaper.core;

import com.spotify.reaper.service.IRepairStrategy;

public class ColumnFamily {
  private Long id;
  private final Cluster cluster;
  private final String keyspaceName;
  private final String name;
  private final IRepairStrategy strategy;
  private final int segmentCount; // int/long/BigInteger?
  private final boolean snapshotRepair;

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public Cluster getCluster() {
    return cluster;
  }

  public String getKeyspaceName() {
    return keyspaceName;
  }

  public String getName() {
    return name;
  }

  public IRepairStrategy getStrategy() {
    return strategy;
  }

  public int getSegmentCount() {
    return segmentCount;
  }

  public boolean isSnapshotRepair() {
    return snapshotRepair;
  }

  private ColumnFamily(ColumnFamilyBuilder builder)
  {
    this.id = builder.id;
    this.cluster = builder.cluster;
    this.keyspaceName = builder.keyspaceName;
    this.name = builder.name;
    this.strategy = builder.strategy;
    this.segmentCount = builder.segmentCount;
    this.snapshotRepair = builder.snapshotRepair;
  }


  public static class ColumnFamilyBuilder {
    private Long id;
    private Cluster cluster;
    private String keyspaceName;
    private String name;
    private IRepairStrategy strategy;
    private int segmentCount;
    private boolean snapshotRepair;

    public ColumnFamilyBuilder id(long id) {
      this.id = id;
      return this;
    }

    public ColumnFamilyBuilder cluster(Cluster cluster) {
      this.cluster = cluster;
      return this;
    }

    public ColumnFamilyBuilder keyspaceName(String keyspaceName) {
      this.keyspaceName = keyspaceName;
      return this;
    }

    public ColumnFamilyBuilder name(String name) {
      this.name = name;
      return this;
    }

    public ColumnFamilyBuilder strategy(IRepairStrategy strategy) {
      this.strategy = strategy;
      return this;
    }

    public ColumnFamilyBuilder segmentCount(int segmentCount) {
      this.segmentCount = segmentCount;
      return this;
    }

    public ColumnFamilyBuilder snapshotRepair(boolean snapshotRepair) {
      this.snapshotRepair = snapshotRepair;
      return this;
    }


    public ColumnFamily build() {
      return new ColumnFamily(this);
    }
  }
}
