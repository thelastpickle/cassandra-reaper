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
package com.spotify.reaper.core;

public class ColumnFamily {

  private final long id;
  private final String clusterName;
  private final String keyspaceName;
  private final String name;
  private final int segmentCount;
  private final boolean snapshotRepair;

  public long getId() {
    return id;
  }

  public String getClusterName() {
    return clusterName;
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

  private ColumnFamily(Builder builder, long id) {
    this.id = id;
    this.clusterName = builder.clusterName;
    this.keyspaceName = builder.keyspaceName;
    this.name = builder.name;
    this.segmentCount = builder.segmentCount;
    this.snapshotRepair = builder.snapshotRepair;
  }

  public Builder with() {
    return new Builder(this);
  }

  public static class Builder {

    public final String clusterName;
    public final String keyspaceName;
    public final String name;
    private int segmentCount;
    private boolean snapshotRepair;

    public Builder(String clusterName, String keyspaceName, String name, int segmentCount,
                   boolean snapshotRepair) {
      this.clusterName = clusterName;
      this.keyspaceName = keyspaceName;
      this.name = name;
      this.segmentCount = segmentCount;
      this.snapshotRepair = snapshotRepair;
    }

    public Builder(ColumnFamily original) {
      clusterName = original.clusterName;
      keyspaceName = original.keyspaceName;
      name = original.name;
      segmentCount = original.segmentCount;
      snapshotRepair = original.snapshotRepair;
    }

    public Builder segmentCount(int segmentCount) {
      this.segmentCount = segmentCount;
      return this;
    }

    public Builder snapshotRepair(boolean snapshotRepair) {
      this.snapshotRepair = snapshotRepair;
      return this;
    }

    public ColumnFamily build(long id) {
      return new ColumnFamily(this, id);
    }
  }
}
