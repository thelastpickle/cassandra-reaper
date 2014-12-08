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
  private final Cluster cluster;
  private final String keyspaceName;
  private final String name;
  private final int segmentCount; // int/long/BigInteger?
  private final boolean snapshotRepair;

  public long getId() {
    return id;
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

  private ColumnFamily(Builder builder, long id) {
    this.id = id;
    this.cluster = builder.cluster;
    this.keyspaceName = builder.keyspaceName;
    this.name = builder.name;
    this.segmentCount = builder.segmentCount;
    this.snapshotRepair = builder.snapshotRepair;
  }


  public static class Builder {

    public final Cluster cluster;
    public final String keyspaceName;
    public final String name;
    public final int segmentCount;
    public final boolean snapshotRepair;

    public Builder(Cluster cluster, String keyspaceName, String name, int segmentCount,
                   boolean snapshotRepair) {
      this.cluster = cluster;
      this.keyspaceName = keyspaceName;
      this.name = name;
      this.segmentCount = segmentCount;
      this.snapshotRepair = snapshotRepair;
    }

    public ColumnFamily build(long id) {
      return new ColumnFamily(this, id);
    }
  }
}
