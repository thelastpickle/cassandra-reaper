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

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import com.google.common.base.Preconditions;

public final class RepairUnit {

  private final UUID id;
  private final String clusterName;
  private final String keyspaceName;
  private final Set<String> columnFamilies;
  private final boolean incrementalRepair;
  private final Set<String> nodes;
  private final Set<String> datacenters;
  private final Set<String> blacklistedTables;
  private final int repairThreadCount;
  private final int timeout;

  private RepairUnit(Builder builder, UUID id) {
    this.id = id;
    this.clusterName = builder.clusterName;
    this.keyspaceName = builder.keyspaceName;
    this.columnFamilies = builder.columnFamilies;
    this.incrementalRepair = builder.incrementalRepair;
    this.nodes = builder.nodes;
    this.datacenters = builder.datacenters;
    this.blacklistedTables = builder.blacklistedTables;
    this.repairThreadCount = builder.repairThreadCount;
    this.timeout = builder.timeout;
  }

  public static Builder builder() {
    return new Builder();
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

  public boolean getIncrementalRepair() {
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

  public int getRepairThreadCount() {
    return repairThreadCount;
  }

  public int getTimeout() {
    return timeout;
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append("ID: ").append(this.id)
        .append(", Cluster name: ").append(this.clusterName)
        .append(", Keyspace name: ").append(this.keyspaceName);
    return buf.toString();
  }

  public Builder with() {
    return new Builder(this);
  }

  public static final class Builder {

    public String clusterName;
    public String keyspaceName;
    public Set<String> columnFamilies = Collections.emptySet();
    public Boolean incrementalRepair;
    public Set<String> nodes = Collections.emptySet();
    public Set<String> datacenters = Collections.emptySet();
    public Set<String> blacklistedTables = Collections.emptySet();
    public Integer repairThreadCount;
    public Integer timeout;

    private Builder() {}

    private Builder(RepairUnit original) {
      clusterName = original.clusterName;
      keyspaceName = original.keyspaceName;
      columnFamilies = original.columnFamilies;
      incrementalRepair = original.incrementalRepair;
      nodes = original.nodes;
      datacenters = original.datacenters;
      blacklistedTables = original.blacklistedTables;
      repairThreadCount = original.repairThreadCount;
      timeout = original.timeout;
    }

    public Builder clusterName(String clusterName) {
      this.clusterName = clusterName;
      return this;
    }

    public Builder keyspaceName(String keyspaceName) {
      this.keyspaceName = keyspaceName;
      return this;
    }

    public Builder columnFamilies(Set<String> columnFamilies) {
      this.columnFamilies = Collections.unmodifiableSet(columnFamilies);
      return this;
    }

    public Builder incrementalRepair(boolean incrementalRepair) {
      this.incrementalRepair = incrementalRepair;
      return this;
    }

    public Builder nodes(Set<String> nodes) {
      this.nodes = Collections.unmodifiableSet(nodes);
      return this;
    }

    public Builder datacenters(Set<String> datacenters) {
      this.datacenters = Collections.unmodifiableSet(datacenters);
      return this;
    }

    public Builder blacklistedTables(Set<String> blacklistedTables) {
      this.blacklistedTables = Collections.unmodifiableSet(blacklistedTables);
      return this;
    }

    public Builder repairThreadCount(int repairThreadCount) {
      this.repairThreadCount = repairThreadCount;
      return this;
    }

    public Builder timeout(int timeout) {
      this.timeout = timeout;
      return this;
    }

    public RepairUnit build(UUID id) {
      Preconditions.checkState(null != clusterName, "clusterName(..) must be called before build(..)");
      Preconditions.checkState(null != keyspaceName, "keyspaceName(..) must be called before build(..)");
      Preconditions.checkState(null != incrementalRepair, "incrementalRepair(..) must be called before build(..)");
      Preconditions.checkState(null != repairThreadCount, "repairThreadCount(..) must be called before build(..)");
      Preconditions.checkState(null != timeout, "timeout(..) must be called before build(..)");
      return new RepairUnit(this, id);
    }

    @Override
    public int hashCode() {
      // primes 7 & 59 chosen as optimal by netbeans – https://stackoverflow.com/a/21914964
      int hash = 7 * 59;
      hash +=  Objects.hashCode(this.clusterName);
      hash *= 59;
      hash +=  Objects.hashCode(this.keyspaceName);
      hash *= 59;
      hash +=  Objects.hashCode(this.columnFamilies);
      hash *= 59;
      hash +=  (this.incrementalRepair ? 2 : 1);
      hash *= 59;
      hash +=  Objects.hashCode(this.nodes);
      hash *= 59;
      hash +=  Objects.hashCode(this.datacenters);
      hash *= 59;
      hash +=  Objects.hashCode(this.blacklistedTables);
      hash *= 59;
      hash +=  Objects.hashCode(this.repairThreadCount);
      hash *= 59;
      hash +=  Objects.hashCode(this.timeout);
      return hash;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (null == obj || getClass() != obj.getClass()) {
        return false;
      }

      return Objects.equals(this.incrementalRepair, ((Builder) obj).incrementalRepair)
          && Objects.equals(this.clusterName, ((Builder) obj).clusterName)
          && Objects.equals(this.keyspaceName, ((Builder) obj).keyspaceName)
          && Objects.equals(this.columnFamilies, ((Builder) obj).columnFamilies)
          && Objects.equals(this.nodes, ((Builder) obj).nodes)
          && Objects.equals(this.datacenters, ((Builder) obj).datacenters)
          && Objects.equals(this.blacklistedTables, ((Builder) obj).blacklistedTables)
          && Objects.equals(this.repairThreadCount, ((Builder) obj).repairThreadCount)
          && Objects.equals(this.timeout, ((Builder) obj).timeout);
    }
  }
}
