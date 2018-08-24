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

@JsonDeserialize(builder = DroppedMessages.Builder.class)
public final class DroppedMessages {
  private final String name;
  private final Integer count;
  private final Double oneMinuteRate;
  private final Double fiveMinuteRate;
  private final Double fifteenMinuteRate;
  private final Double meanRate;

  private DroppedMessages(Builder builder) {
    this.name = builder.name;
    this.count = builder.count;
    this.oneMinuteRate = builder.oneMinuteRate;
    this.fiveMinuteRate = builder.fiveMinuteRate;
    this.fifteenMinuteRate = builder.fifteenMinuteRate;
    this.meanRate = builder.meanRate;
  }

  public String getName() {
    return name;
  }

  public Integer getCount() {
    return count;
  }

  public Double getOneMinuteRate() {
    return oneMinuteRate;
  }

  public Double getFiveMinuteRate() {
    return fiveMinuteRate;
  }

  public Double getFifteenMinuteRate() {
    return fifteenMinuteRate;
  }

  public Double getMeanRate() {
    return meanRate;
  }


  public static Builder builder() {
    return new Builder();
  }

  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "with")
  public static final class Builder {
    private String name;
    private Integer count;
    private Double oneMinuteRate;
    private Double fiveMinuteRate;
    private Double fifteenMinuteRate;
    private Double meanRate;

    private Builder() {}

    public Builder withName(String name) {
      this.name = name;
      return this;
    }

    public Builder withCount(Integer count) {
      this.count = count;
      return this;
    }

    public Builder withOneMinuteRate(Double oneMinuteRate) {
      this.oneMinuteRate = oneMinuteRate;
      return this;
    }

    public Builder withFiveMinuteRate(Double fiveMinuteRate) {
      this.fiveMinuteRate = fiveMinuteRate;
      return this;
    }

    public Builder withFifteenMinuteRate(Double fifteenMinuteRate) {
      this.fifteenMinuteRate = fifteenMinuteRate;
      return this;
    }

    public Builder withMeanRate(Double meanRate) {
      this.meanRate = meanRate;
      return this;
    }

    public DroppedMessages build() {
      return new DroppedMessages(this);
    }
  }
}
