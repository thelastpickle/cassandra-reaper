package com.spotify.reaper.core;

public class ColumnFamily {

  private Long id;
  private final Cluster cluster;
  private final String keyspaceName;
  private final String name;
  private final int segmentCount; // int/long/BigInteger?
  private final boolean snapshotRepair;

  public Long getId() {
    return id;
  }

  public void setId(long id) {
    assert this.id == null : "cannot reset id after once set";
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

  public int getSegmentCount() {
    return segmentCount;
  }

  public boolean isSnapshotRepair() {
    return snapshotRepair;
  }

  private ColumnFamily(Builder builder) {
    this.id = builder.id;
    this.cluster = builder.cluster;
    this.keyspaceName = builder.keyspaceName;
    this.name = builder.name;
    this.segmentCount = builder.segmentCount;
    this.snapshotRepair = builder.snapshotRepair;
  }


  public static class Builder {

    private Long id;
    private Cluster cluster;
    private String keyspaceName;
    private String name;
    private int segmentCount;
    private boolean snapshotRepair;

    public Builder id(long id) {
      this.id = id;
      return this;
    }

    public Builder cluster(Cluster cluster) {
      this.cluster = cluster;
      return this;
    }

    public Builder keyspaceName(String keyspaceName) {
      this.keyspaceName = keyspaceName;
      return this;
    }

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    public Builder segmentCount(int segmentCount) {
      this.segmentCount = segmentCount;
      return this;
    }

    public Builder snapshotRepair(boolean snapshotRepair) {
      this.snapshotRepair = snapshotRepair;
      return this;
    }


    public ColumnFamily build() {
      return new ColumnFamily(this);
    }
  }
}
