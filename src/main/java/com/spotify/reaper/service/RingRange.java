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
package com.spotify.reaper.service;

import com.google.common.annotations.VisibleForTesting;

import java.math.BigInteger;

// TODO: Check if this duplicates org.apache.cassandra.dht.Range.
public class RingRange {
  private final BigInteger start;
  private final BigInteger end;

  public RingRange(BigInteger start, BigInteger end) {
    this.start = start;
    this.end = end;
  }

  public BigInteger getStart() {
    return start;
  }

  public BigInteger getEnd() {
    return end;
  }

  /**
   * Returns the size of this range
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
    if (!this.isWrapping() && !other.isWrapping()) {
      return SegmentGenerator.greaterThanOrEqual(other.start, start) &&
          SegmentGenerator.lowerThanOrEqual(other.end, end);
    }
    else if (!this.isWrapping() && other.isWrapping()) {
      return false;
    }
    else if (this.isWrapping() && !other.isWrapping()) {
      return SegmentGenerator.greaterThanOrEqual(other.start, start) ||
          SegmentGenerator.lowerThanOrEqual(other.end, end);
    }
    else { // if (this.isWrapping() && other.isWrapping())
      return SegmentGenerator.greaterThanOrEqual(other.start, start) &&
          SegmentGenerator.lowerThanOrEqual(other.end, end);
    }
  }

  /**
   * @return true if 0 is inside of this range. Note that if start == end, then wrapping is true
   */
  @VisibleForTesting
  protected boolean isWrapping() {
    return SegmentGenerator.greaterThanOrEqual(start, end);
  }

  @Override
  public String toString() {
    return String.format("(%s,%s]", start.toString(), end.toString());
  }
}
