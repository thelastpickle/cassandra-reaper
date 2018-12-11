/*
 * Copyright 2017-2017 Spotify AB
 * Copyright 2017-2018 The Last Pickle Ltd
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

import org.joda.time.DateTime;

public final class GenericMetric {

  private final String clusterName;
  private final String metric;
  private final String host;
  private final DateTime ts;
  private final double value;

  private GenericMetric(Builder builder) {
    this.clusterName = builder.clusterName;
    this.metric = builder.metric;
    this.host = builder.host;
    this.ts = builder.ts;
    this.value = builder.value;
  }

  public String getClusterName() {
    return clusterName;
  }

  public String getMetric() {
    return metric;
  }

  public String getHost() {
    return host;
  }

  public DateTime getTs() {
    return ts;
  }

  public double getValue() {
    return value;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private String clusterName;
    private String metric;
    private String host;
    private DateTime ts;
    private double value;

    private Builder() {}

    public Builder withClusterName(String clusterName) {
      this.clusterName = clusterName;
      return this;
    }

    public Builder withMetric(String metric) {
      this.metric = metric;
      return this;
    }

    public Builder withHost(String host) {
      this.host = host;
      return this;
    }

    public Builder withTs(DateTime ts) {
      this.ts = ts;
      return this;
    }

    public Builder withValue(double value) {
      this.value = value;
      return this;
    }

    public GenericMetric build() {
      return new GenericMetric(this);
    }
  }
}
