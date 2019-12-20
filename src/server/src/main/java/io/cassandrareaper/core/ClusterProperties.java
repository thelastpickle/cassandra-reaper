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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

@JsonDeserialize(builder = ClusterProperties.Builder.class)
public final class ClusterProperties {
  private final int jmxPort;
  private String jmxUsername;
  private String jmxPassword;

  private ClusterProperties(Builder builder) {
    this.jmxPort = builder.jmxPort;
    this.jmxUsername = builder.jmxUsername;
    this.jmxPassword = builder.jmxPassword;
  }

  public int getJmxPort() {
    return jmxPort;
  }

  public String getJmxUsername() {
    return jmxUsername;
  }

  public String getJmxPassword() {
    return jmxPassword;
  }

  public static Builder builder() {
    return new Builder();
  }

  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "with")
  public static final class Builder {
    private int jmxPort;
    private String jmxUsername;
    private String jmxPassword;

    private Builder() {}

    public Builder withJmxPort(int jmxPort) {
      this.jmxPort = jmxPort;
      return this;
    }

    public Builder withJmxUsername(String jmxUsername) {
      this.jmxUsername = jmxUsername;
      return this;
    }

    public Builder withJmxPassword(String jmxPassword) {
      this.jmxPassword = jmxPassword;
      return this;
    }

    public ClusterProperties build() {
      return new ClusterProperties(this);
    }
  }
}
