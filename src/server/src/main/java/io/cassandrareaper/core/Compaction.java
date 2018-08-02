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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

@JsonDeserialize(builder = Compaction.Builder.class)
public final class Compaction {
  private String id;
  private String type;
  private String keyspace;
  private String table;
  private Long progress;
  private Long total;
  private String unit;

  private Compaction(Builder builder) {
    this.id = builder.id;
    this.type = builder.type;
    this.keyspace = builder.keyspace;
    this.table = builder.table;
    this.progress = builder.progress;
    this.total = builder.total;
    this.unit = builder.unit;
  }

  public String getId() {
    return id;
  }

  public String getType() {
    return type;
  }

  public String getKeyspace() {
    return keyspace;
  }

  public String getTable() {
    return table;
  }

  public Long getProgress() {
    return progress;
  }

  public Long getTotal() {
    return total;
  }

  public String getUnit() {
    return unit;
  }

  public static Builder builder() {
    return new Builder();
  }

  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "with")
  public static final class Builder {
    private String id;
    private String type;
    private String keyspace;
    private String table;
    private Long progress;
    private Long total;
    private String unit;

    private Builder() {}

    public Builder withId(String id) {
      this.id = id;
      return this;
    }

    public Builder withType(String type) {
      this.type = type;
      return this;
    }

    public Builder withKeyspace(String keyspace) {
      this.keyspace = keyspace;
      return this;
    }

    public Builder withTable(String table) {
      this.table = table;
      return this;
    }

    public Builder withProgress(Long progress) {
      this.progress = progress;
      return this;
    }

    public Builder withTotal(Long total) {
      this.total = total;
      return this;
    }

    public Builder withUnit(String unit) {
      this.unit = unit;
      return this;
    }

    public Compaction build() {
      return new Compaction(this);
    }
  }
}
