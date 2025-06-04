/*
 * Copyright 2018-2018 The Last Pickle Ltd
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

import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

@JsonDeserialize(builder = CompactionStats.Builder.class)
public final class CompactionStats {
  private Optional<Integer> pendingCompactions;
  private List<Compaction> activeCompactions;

  private CompactionStats(Builder builder) {
    this.pendingCompactions = builder.pendingCompactions;
    this.activeCompactions = builder.activeCompactions;
  }

  public Optional<Integer> getPendingCompactions() {
    return pendingCompactions;
  }

  public List<Compaction> getActiveCompactions() {
    return activeCompactions;
  }

  public static Builder builder() {
    return new Builder();
  }

  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "with")
  public static final class Builder {
    private Optional<Integer> pendingCompactions;
    private List<Compaction> activeCompactions;

    private Builder() {}

    public Builder withPendingCompactions(Optional<Integer> pendingCompactions) {
      this.pendingCompactions = pendingCompactions;
      return this;
    }

    public Builder withActiveCompactions(List<Compaction> activeCompactions) {
      this.activeCompactions = activeCompactions;
      return this;
    }

    public CompactionStats build() {
      return new CompactionStats(this);
    }
  }
}
