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
package com.spotify.reaper.resources.view;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.reaper.core.ColumnFamily;

/**
 * Contains the data to be shown when replying with column family (table) data.
 */
public class ColumnFamilyStatus {

  @JsonProperty("cluster_name")
  private final String clusterName;

  @JsonProperty("keyspace_name")
  private final String keyspaceName;

  @JsonProperty("table_name")
  private final String tableName;

  @JsonProperty("segment_count")
  private final int segmentCount;

  @JsonProperty("snapshot_repair")
  private final boolean snapshotRepair;

  public ColumnFamilyStatus(ColumnFamily columnFamily) {
    this.clusterName = columnFamily.getClusterName();
    this.keyspaceName = columnFamily.getKeyspaceName();
    this.tableName = columnFamily.getName();
    this.segmentCount = columnFamily.getSegmentCount();
    this.snapshotRepair = columnFamily.isSnapshotRepair();
  }

}
