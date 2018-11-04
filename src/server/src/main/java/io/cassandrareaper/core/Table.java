/*
 * Copyright 2018-2019 The Last Pickle Ltd
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

import com.google.common.base.Preconditions;

public final class Table {

  private final String name;
  private final String compactionStrategy;

  private Table(Builder builder) {
    this.name = builder.name;
    this.compactionStrategy = builder.compactionStrategy;
  }

  public String getName() {
    return name;
  }

  public String getCompactionStrategy() {
    return compactionStrategy;
  }

  public static Builder builder() {
    return new Builder();
  }

  public String toString() {
    return String.format("{name=%s, compactionStrategy=%s}", name, compactionStrategy);
  }

  public static final class Builder {

    private String name;
    private String compactionStrategy;

    private Builder() {
    }

    public Table.Builder withName(String name) {
      Preconditions.checkState(null == this.name, "`.withName(..)` can only be called once");
      this.name = name;
      return this;
    }

    public Table.Builder withCompactionStrategy(String compactionStrategy) {
      Preconditions
          .checkState(null == this.compactionStrategy, "`.withCompactionStrategy(..)` can only be called once");

      this.compactionStrategy = compactionStrategy;
      return this;
    }

    public Table build() {
      Preconditions.checkNotNull(name, "`.withName(..)` must be called before `.build()`");
      Preconditions.checkNotNull(compactionStrategy, "`.withCompactionStrategy(..)` must be called before `.build()`");
      return new Table(this);
    }
  }

}
