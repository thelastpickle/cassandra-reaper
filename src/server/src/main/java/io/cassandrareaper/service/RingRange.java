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

package io.cassandrareaper.service;

import java.math.BigInteger;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.base.Preconditions;

// TODO: Check if this duplicates org.apache.cassandra.dht.Range.
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonDeserialize(builder = RingRange.Builder.class)
public final class RingRange {

  public static final Comparator<RingRange> START_COMPARATOR =
      (RingRange o1, RingRange o2) -> o1.start.compareTo(o2.start);

  private final BigInteger start;
  private final BigInteger end;

  public RingRange(BigInteger start, BigInteger end) {
    this.start = start;
    this.end = end;
  }

  public RingRange(String... range) {
    start = new BigInteger(range[0]);
    end = new BigInteger(range[1]);
  }

  public BigInteger getStart() {
    return start;
  }

  public BigInteger getEnd() {
    return end;
  }

  /**
   * Returns the size of this range
   *
   * @return size of the range, max - range, in case of wrap
   */
  public BigInteger span(BigInteger ringSize) {
    if (SegmentGenerator.greaterThanOrEqual(start, end)) {
      return end.subtract(start).add(ringSize);
    } else {
      return end.subtract(start);
    }
  }

  /**
   * @return true if other is enclosed in this range.
   */
  public boolean encloses(RingRange other) {
    if (!isWrapping()) {
      return !other.isWrapping()
          && SegmentGenerator.greaterThanOrEqual(other.start, start)
          && SegmentGenerator.lowerThanOrEqual(other.end, end);
    } else {
      return (!other.isWrapping()
              && (SegmentGenerator.greaterThanOrEqual(other.start, start)
                  || SegmentGenerator.lowerThanOrEqual(other.end, end)))
          || (SegmentGenerator.greaterThanOrEqual(other.start, start)
              && SegmentGenerator.lowerThanOrEqual(other.end, end));
    }
  }

  /**
   * @return true if 0 is inside of this range. Note that if start == end, then wrapping is true
   */
  @JsonIgnore
  public boolean isWrapping() {
    return SegmentGenerator.greaterThanOrEqual(start, end);
  }

  @Override
  public String toString() {
    return String.format("(%s,%s]", start.toString(), end.toString());
  }

  public static RingRange merge(List<RingRange> ranges) {
    // sor
    Collections.sort(ranges, START_COMPARATOR);

    // find gap
    int gap = 0;
    for (; gap < ranges.size() - 1; gap++) {
      RingRange left = ranges.get(gap);
      RingRange right = ranges.get(gap + 1);
      if (!left.end.equals(right.start)) {
        break;
      }
    }

    // return merged
    if (gap == ranges.size() - 1) {
      return new RingRange(ranges.get(0).start, ranges.get(gap).end);
    } else {
      return new RingRange(ranges.get(gap + 1).start, ranges.get(gap).end);
    }
  }

  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "with")
  public static final class Builder {

    private BigInteger start;
    private BigInteger end;

    public Builder() {}

    public Builder withStart(BigInteger start) {
      this.start = start;
      return this;
    }

    public Builder withStart(String start) {
      this.start = new BigInteger(start);
      return this;
    }

    public Builder withEnd(BigInteger end) {
      this.end = end;
      return this;
    }

    public Builder withEnd(String end) {
      this.end = new BigInteger(end);
      return this;
    }

    public RingRange build() {
      Preconditions.checkNotNull(start);
      Preconditions.checkNotNull(end);

      return new RingRange(this.start, this.end);
    }
  }
}
