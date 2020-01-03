/*
 * Copyright 2014-2017 Spotify AB
 * Copyright 2016-2019 The Last Pickle Ltd
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
import com.google.common.base.Preconditions;

@JsonDeserialize(builder = JmxCredentials.Builder.class)
public final class JmxCredentials {

  private final String username;
  private final String password;

  private JmxCredentials(Builder builder) {
    this.username = builder.username;
    this.password = builder.password;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public static Builder builder() {
    return new Builder();
  }

  public String toString() {
    return username + ":" + (password != null ? "***" : null);
  }

  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "with")
  public static final class Builder {

    private String username;
    private String password;

    private Builder() {
    }

    public Builder withUsername(String username) {
      Preconditions.checkNotNull(username);
      this.username = username;
      return this;
    }

    public Builder withPassword(String password) {
      Preconditions.checkNotNull(password);
      this.password = password;
      return this;
    }

    public JmxCredentials build() {
      return new JmxCredentials(this);
    }

  }

}