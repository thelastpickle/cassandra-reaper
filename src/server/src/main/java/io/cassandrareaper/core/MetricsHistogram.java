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

@JsonDeserialize(builder = MetricsHistogram.Builder.class)
public final class MetricsHistogram {
  private final String name;
  private final String type;
  private final Double p50;
  private final Double p75;
  private final Double p95;
  private final Double p98;
  private final Double p99;
  private final Double p999;
  private final Double min;
  private final Double mean;
  private final Double max;
  private final Integer count;
  private final Double oneMinuteRate;
  private final Double fiveMinuteRate;
  private final Double fifteenMinuteRate;
  private final Double meanRate;
  private final Double stdDev;

  private MetricsHistogram(Builder builder) {
    this.name = builder.name;
    this.type = builder.type;
    this.p50 = builder.p50;
    this.p75 = builder.p75;
    this.p95 = builder.p95;
    this.p98 = builder.p98;
    this.p99 = builder.p99;
    this.p999 = builder.p999;
    this.min = builder.min;
    this.mean = builder.mean;
    this.max = builder.max;
    this.count = builder.count;
    this.oneMinuteRate = builder.oneMinuteRate;
    this.fiveMinuteRate = builder.fiveMinuteRate;
    this.fifteenMinuteRate = builder.fifteenMinuteRate;
    this.meanRate = builder.meanRate;
    this.stdDev = builder.stdDev;
  }

  public String getName() {
    return name;
  }

  public String getType() {
    return type;
  }

  public Double getP50() {
    return p50;
  }

  public Double getP75() {
    return p75;
  }

  public Double getP95() {
    return p95;
  }

  public Double getP98() {
    return p98;
  }

  public Double getP99() {
    return p99;
  }

  public Double getP999() {
    return p999;
  }

  public Double getMin() {
    return min;
  }

  public Double getMean() {
    return mean;
  }

  public Double getMax() {
    return max;
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

  public Double getStdDev() {
    return stdDev;
  }

  public static Builder builder() {
    return new Builder();
  }

  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "with")
  public static final class Builder {
    private String name;
    private String type;
    private Double p50;
    private Double p75;
    private Double p95;
    private Double p98;
    private Double p99;
    private Double p999;
    private Double min;
    private Double mean;
    private Double max;
    private Integer count;
    private Double oneMinuteRate;
    private Double fiveMinuteRate;
    private Double fifteenMinuteRate;
    private Double meanRate;
    private Double stdDev;

    private Builder() {}

    public Builder withName(String name) {
      this.name = name;
      return this;
    }

    public Builder withType(String type) {
      this.type = type;
      return this;
    }

    public Builder withP50(Double p50) {
      this.p50 = p50;
      return this;
    }

    public Builder withP75(Double p75) {
      this.p75 = p75;
      return this;
    }

    public Builder withP95(Double p95) {
      this.p95 = p95;
      return this;
    }

    public Builder withP98(Double p98) {
      this.p98 = p98;
      return this;
    }

    public Builder withP99(Double p99) {
      this.p99 = p99;
      return this;
    }

    public Builder withP999(Double p999) {
      this.p999 = p999;
      return this;
    }

    public Builder withMin(Double min) {
      this.min = min;
      return this;
    }

    public Builder withMean(Double mean) {
      this.mean = mean;
      return this;
    }

    public Builder withMax(Double max) {
      this.max = max;
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

    public Builder withStdDev(Double stdDev) {
      this.stdDev = stdDev;
      return this;
    }

    public MetricsHistogram build() {
      return new MetricsHistogram(this);
    }
  }

}
