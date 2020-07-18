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

import io.cassandrareaper.service.RingRange;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

@JsonDeserialize(builder = Segment.Builder.class)
public final class Segment {

  public static final Comparator<Segment> START_COMPARATOR
      = (Segment o1, Segment o2) ->
          o1.getBaseRange().getStart().compareTo(o2.getBaseRange().getStart());

  RingRange baseRange;
  List<RingRange> tokenRanges;
  Map<String, String> replicas;

  private Segment(Builder builder) {
    this.tokenRanges = builder.tokenRanges;
    this.baseRange = builder.tokenRanges.get(0);
    if (builder.baseRange != null) {
      this.baseRange = builder.baseRange;
    }
    this.replicas = builder.replicas;
  }

  public RingRange getBaseRange() {
    return this.baseRange;
  }

  public List<RingRange> getTokenRanges() {
    return this.tokenRanges;
  }

  public BigInteger countTokens(BigInteger rangeSize) {
    BigInteger tokens = BigInteger.ZERO;
    for (RingRange tokenRange:tokenRanges) {
      tokens = tokens.add(tokenRange.span(rangeSize));
    }

    return tokens;
  }

  public Map<String, String> getReplicas() {
    return this.replicas;
  }

  public static Builder builder() {
    return new Builder();
  }

  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "with")
  public static final class Builder {
    private List<RingRange> tokenRanges;
    private RingRange baseRange;
    private Map<String, String> replicas;

    private Builder() {}

    public Builder withTokenRanges(List<RingRange> tokenRanges) {
      this.tokenRanges = tokenRanges;
      return this;
    }

    public Builder withTokenRange(RingRange tokenRange) {
      this.tokenRanges = Arrays.asList(tokenRange);
      return this;
    }

    public Builder withBaseRange(RingRange baseRange) {
      this.baseRange = baseRange;
      return this;
    }

    public Builder withReplicas(Map<String, String> replicas) {
      this.replicas = replicas;
      return this;
    }

    public Segment build() {
      return new Segment(this);
    }
  }
}
