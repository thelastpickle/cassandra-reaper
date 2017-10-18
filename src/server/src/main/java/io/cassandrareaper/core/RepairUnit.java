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

package io.cassandrareaper.core;

import java.util.Set;
import java.util.UUID;

public final class RepairUnit {

  private final UUID id;
  private final String clusterName;
  private final String keyspaceName;
  private final Set<String> columnFamilies;
  private final Boolean incrementalRepair;
  private final Set<String> nodes;
  private final Set<String> datacenters;
  private final Set<String> blacklistedTables;

  private RepairUnit(Builder builder, UUID id) {
    this.id = id;
    this.clusterName = builder.clusterName;
    this.keyspaceName = builder.keyspaceName;
    this.columnFamilies = builder.columnFamilies;
    this.incrementalRepair = builder.incrementalRepair;
    this.nodes = builder.nodes;
    this.datacenters = builder.datacenters;
    this.blacklistedTables = builder.blacklistedTables;
  }

  public UUID getId() {
    return id;
  }

  public String getClusterName() {
    return clusterName;
  }

  public String getKeyspaceName() {
    return keyspaceName;
  }

  public Set<String> getColumnFamilies() {
    return columnFamilies;
  }

  public Boolean getIncrementalRepair() {
    return incrementalRepair;
  }

  public Set<String> getNodes() {
    return nodes;
  }

  public Set<String> getDatacenters() {
    return datacenters;
  }

  public Set<String> getBlacklistedTables() {
    return blacklistedTables;
  }

  public Builder with() {
    return new Builder(this);
  }

  public static class Builder {

    public final String clusterName;
    public final String keyspaceName;
    public final Set<String> columnFamilies;
    public final boolean incrementalRepair;
    public final Set<String> nodes;
    public final Set<String> datacenters;
    public final Set<String> blacklistedTables;

    public Builder(
        String clusterName,
        String keyspaceName,
        Set<String> columnFamilies,
        Boolean incrementalRepair,
        Set<String> nodes,
        Set<String> datacenters,
        Set<String> blacklistedTables) {
      this.clusterName = clusterName;
      this.keyspaceName = keyspaceName;
      this.columnFamilies = columnFamilies;
      this.incrementalRepair = incrementalRepair;
      this.nodes = nodes;
      this.datacenters = datacenters;
      this.blacklistedTables = blacklistedTables;
    }

    private Builder(RepairUnit original) {
      clusterName = original.clusterName;
      keyspaceName = original.keyspaceName;
      columnFamilies = original.columnFamilies;
      incrementalRepair = original.incrementalRepair;
      nodes = original.nodes;
      datacenters = original.datacenters;
      blacklistedTables = original.blacklistedTables;
    }

    public RepairUnit build(UUID id) {
      return new RepairUnit(this, id);
    }
  }
}
