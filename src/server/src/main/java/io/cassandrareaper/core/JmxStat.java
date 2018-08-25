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

@JsonDeserialize(builder = JmxStat.Builder.class)
public final class JmxStat {
  private final String mbeanName;
  private final String domain;
  private final String type;
  private final String scope;
  private final String name;
  private final String attribute;
  private final Double value;

  private JmxStat(Builder builder) {
    this.mbeanName = builder.mbeanName;
    this.domain = builder.domain;
    this.type = builder.type;
    this.scope = builder.scope;
    this.name = builder.name;
    this.attribute = builder.attribute;
    this.value = builder.value;
  }

  public String getMbeanName() {
    return mbeanName;
  }

  public String getDomain() {
    return domain;
  }

  public String getType() {
    return type;
  }

  public String getScope() {
    return scope;
  }

  public String getName() {
    return name;
  }

  public String getAttribute() {
    return attribute;
  }

  public Double getValue() {
    return value;
  }


  @Override
  public String toString() {
    return mbeanName + "/" + scope + "/" + name + "/" + attribute + " = " + value;
  }

  public static Builder builder() {
    return new Builder();
  }

  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "with")
  public static final class Builder {
    private String mbeanName;
    private String domain;
    private String type;
    private String scope;
    private String name;
    private String attribute;
    private Double value;

    private Builder() {}

    public Builder withMbeanName(String mbeanName) {
      this.mbeanName = mbeanName;
      return this;
    }

    public Builder withDomain(String domain) {
      this.domain = domain;
      return this;
    }

    public Builder withType(String type) {
      this.type = type;
      return this;
    }

    public Builder withScope(String scope) {
      this.scope = scope;
      return this;
    }

    public Builder withName(String name) {
      this.name = name;
      return this;
    }

    public Builder withAttribute(String attribute) {
      this.attribute = attribute;
      return this;
    }

    public Builder withValue(Double value) {
      this.value = value;
      return this;
    }

    public JmxStat build() {
      return new JmxStat(this);
    }
  }
}
