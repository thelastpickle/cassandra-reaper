/*
 * Copyright 2018-2018 The Last Pickle Ltd
 *
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

@JsonDeserialize(builder = ThreadPoolStat.Builder.class)
public final class ThreadPoolStat {
  private final String name;
  private final Integer activeTasks;
  private final Integer pendingTasks;
  private final Integer completedTasks;
  private final Integer currentlyBlockedTasks;
  private final Integer totalBlockedTasks;
  private final Integer maxPoolSize;

  private ThreadPoolStat(Builder builder) {
    this.name = builder.name;
    this.activeTasks = builder.activeTasks;
    this.pendingTasks = builder.pendingTasks;
    this.currentlyBlockedTasks = builder.currentlyBlockedTasks;
    this.completedTasks = builder.completedTasks;
    this.totalBlockedTasks = builder.totalBlockedTasks;
    this.maxPoolSize = builder.maxPoolSize;
  }

  public String getName() {
    return name;
  }

  public Integer getActiveTasks() {
    return activeTasks;
  }

  public Integer getPendingTasks() {
    return pendingTasks;
  }

  public Integer getCurrentlyBlockedTasks() {
    return currentlyBlockedTasks;
  }

  public Integer getCompletedTasks() {
    return completedTasks;
  }

  public Integer getTotalBlockedTasks() {
    return totalBlockedTasks;
  }

  public Integer getMaxPoolSize() {
    return maxPoolSize;
  }

  public static Builder builder() {
    return new Builder();
  }

  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "with")
  public static final class Builder {
    private String name;
    private Integer activeTasks;
    private Integer pendingTasks;
    private Integer currentlyBlockedTasks;
    private Integer completedTasks;
    private Integer totalBlockedTasks;
    private Integer maxPoolSize;

    private Builder() {}

    public Builder withName(String name) {
      this.name = name;
      return this;
    }

    public Builder withActiveTasks(Integer activeTasks) {
      this.activeTasks = activeTasks;
      return this;
    }

    public Builder withPendingTasks(Integer pendingTasks) {
      this.pendingTasks = pendingTasks;
      return this;
    }

    public Builder withCurrentlyBlockedTasks(Integer currentlyBlockedTasks) {
      this.currentlyBlockedTasks = currentlyBlockedTasks;
      return this;
    }

    public Builder withCompletedTasks(Integer completedTasks) {
      this.completedTasks = completedTasks;
      return this;
    }

    public Builder withTotalBlockedTasks(Integer totalBlockedTasks) {
      this.totalBlockedTasks = totalBlockedTasks;
      return this;
    }

    public Builder withMaxPoolSize(Integer maxPoolSize) {
      this.maxPoolSize = maxPoolSize;
      return this;
    }

    public ThreadPoolStat build() {
      return new ThreadPoolStat(this);
    }
  }
}
