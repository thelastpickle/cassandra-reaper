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
package com.spotify.reaper.repair.segment;

import com.google.common.annotations.VisibleForTesting;

import java.math.BigInteger;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

// TODO: Check if this duplicates org.apache.cassandra.dht.Range.
public class RingRange {

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
      return !other.isWrapping() &&
          SegmentGenerator.greaterThanOrEqual(other.start, start)
          && SegmentGenerator.lowerThanOrEqual(other.end, end);
    } else {
      return
          (!other.isWrapping() &&
              (
                  SegmentGenerator.greaterThanOrEqual(other.start, start) ||
                  SegmentGenerator.lowerThanOrEqual(other.end, end)
              )
          ) || (
              SegmentGenerator.greaterThanOrEqual(other.start, start) &&
              SegmentGenerator.lowerThanOrEqual(other.end, end)
          );
    }
  }

  /**
   * @return true if 0 is inside of this range. Note that if start == end, then wrapping is true
   */
  @VisibleForTesting
  public boolean isWrapping() {
    return SegmentGenerator.greaterThanOrEqual(start, end);
  }

  @Override
  public String toString() {
    return String.format("(%s,%s]", start.toString(), end.toString());
  }

  public static RingRange merge(List<RingRange> ranges) {

    // sort
    Collections.sort(ranges, startComparator);

    // find gap
    int gap = 0;
    for (;gap<ranges.size()-1;gap++) {
      RingRange left = ranges.get(gap);
      RingRange right = ranges.get(gap + 1);
      if (!left.end.equals(right.start)) {
        break;
      }
    }

    // return merged
    if (gap == ranges.size()-1) {
      return new RingRange(ranges.get(0).start, ranges.get(gap).end);
    } else {
      return new RingRange(ranges.get(gap+1).start, ranges.get(gap).end);
    }
  }

  public static final Comparator<RingRange> startComparator = new Comparator<RingRange>() {
    @Override
    public int compare(RingRange o1, RingRange o2) {
      return o1.start.compareTo(o2.start);
    }
  };
}
