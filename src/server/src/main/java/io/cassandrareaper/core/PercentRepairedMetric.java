/*
 * Copyright 2021-2021 DataStax, Inc.
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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import java.util.UUID;

@JsonDeserialize(builder = PercentRepairedMetric.Builder.class)
public final class PercentRepairedMetric {
  private String cluster;
  private String node;
  private UUID repairScheduleId;
  private String keyspaceName;
  private String tableName;
  private int percentRepaired;

  private PercentRepairedMetric(Builder builder) {
    this.cluster = builder.cluster;
    this.node = builder.node;
    this.repairScheduleId = builder.repairScheduleId;
    this.keyspaceName = builder.keyspaceName;
    this.tableName = builder.tableName;
    this.percentRepaired = builder.percentRepaired;
  }

  public String getCluster() {
    return cluster;
  }

  public String getNode() {
    return node;
  }

  public UUID getRepairScheduleId() {
    return repairScheduleId;
  }

  public String getKeyspaceName() {
    return keyspaceName;
  }

  public String getTableName() {
    return tableName;
  }

  public int getPercentRepaired() {
    return percentRepaired;
  }

  public static Builder builder() {
    return new Builder();
  }

  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "with")
  public static final class Builder {
    private String cluster;
    private String node;
    private UUID repairScheduleId;
    private String keyspaceName;
    private String tableName;
    private int percentRepaired;

    private Builder() {}

    public Builder withCluster(String cluster) {
      this.cluster = cluster;
      return this;
    }

    public Builder withNode(String node) {
      this.node = node;
      return this;
    }

    public Builder withKeyspaceName(String keyspaceName) {
      this.keyspaceName = keyspaceName;
      return this;
    }

    public Builder withTableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public Builder withRepairScheduleId(UUID repairScheduleId) {
      this.repairScheduleId = repairScheduleId;
      return this;
    }

    public Builder withPercentRepaired(int percentRepaired) {
      this.percentRepaired = percentRepaired;
      return this;
    }

    public PercentRepairedMetric build() {
      return new PercentRepairedMetric(this);
    }
  }
}
