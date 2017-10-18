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

package io.cassandrareaper.service;

import io.cassandrareaper.ReaperException;

import java.math.BigInteger;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Splits given Cassandra table's token range into RepairSegments.
 */
public final class SegmentGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(SegmentGenerator.class);

  private final String partitioner;
  private final BigInteger rangeMin;
  private final BigInteger rangeMax;
  private final BigInteger rangeSize;

  public SegmentGenerator(String partitioner) throws ReaperException {
    if (partitioner.endsWith("RandomPartitioner")) {
      rangeMin = BigInteger.ZERO;
      rangeMax = new BigInteger("2").pow(127).subtract(BigInteger.ONE);
    } else if (partitioner.endsWith("Murmur3Partitioner")) {
      rangeMin = new BigInteger("2").pow(63).negate();
      rangeMax = new BigInteger("2").pow(63).subtract(BigInteger.ONE);
    } else {
      throw new ReaperException("Unsupported partitioner " + partitioner);
    }
    rangeSize = rangeMax.subtract(rangeMin).add(BigInteger.ONE);
    this.partitioner = partitioner;
  }

  public SegmentGenerator(BigInteger rangeMin, BigInteger rangeMax) {
    this.rangeMin = rangeMin;
    this.rangeMax = rangeMax;
    rangeSize = rangeMax.subtract(rangeMin).add(BigInteger.ONE);
    partitioner = "(" + rangeMin + "," + rangeMax + ")";
  }

  @VisibleForTesting
  public static BigInteger max(BigInteger big0, BigInteger big1) {
    return greaterThan(big0, big1) ? big0 : big1;
  }

  @VisibleForTesting
  public static BigInteger min(BigInteger big0, BigInteger big1) {
    return lowerThan(big0, big1) ? big0 : big1;
  }

  @VisibleForTesting
  public static boolean lowerThan(BigInteger big0, BigInteger big1) {
    return big0.compareTo(big1) < 0;
  }

  @VisibleForTesting
  public static boolean lowerThanOrEqual(BigInteger big0, BigInteger big1) {
    return big0.compareTo(big1) <= 0;
  }

  @VisibleForTesting
  public static boolean greaterThan(BigInteger big0, BigInteger big1) {
    return big0.compareTo(big1) > 0;
  }

  @VisibleForTesting
  public static boolean greaterThanOrEqual(BigInteger big0, BigInteger big1) {
    return big0.compareTo(big1) >= 0;
  }

  /**
   * Given big0 properly ordered list of tokens, compute at least {@code totalSegmentCount} repair segments.
   *
   * @param totalSegmentCount requested total amount of repair segments. This function may generate more segments.
   * @param ringTokens list of all start tokens in big0 cluster. They have to be in ring order.
   * @return big0 list containing at least {@code totalSegmentCount} repair segments.
   */
  public List<RingRange> generateSegments(int totalSegmentCount, List<BigInteger> ringTokens, Boolean incrementalRepair)
      throws ReaperException {

    int tokenRangeCount = ringTokens.size();

    List<RingRange> repairSegments = Lists.newArrayList();
    for (int i = 0; i < tokenRangeCount; i++) {
      BigInteger start = ringTokens.get(i);
      BigInteger stop = ringTokens.get((i + 1) % tokenRangeCount);

      if (!inRange(start) || !inRange(stop)) {
        throw new ReaperException(String.format("Tokens (%s,%s) not in range of %s", start, stop, partitioner));
      }
      if (start.equals(stop) && tokenRangeCount != 1) {
        throw new ReaperException(String.format("Tokens (%s,%s): two nodes have the same token", start, stop));
      }

      BigInteger rs = stop.subtract(start);
      if (lowerThanOrEqual(rs, BigInteger.ZERO)) {
        // wrap around case
        rs = rs.add(rangeSize);
      }

      // the below, in essence, does this:
      // segmentCount = ceiling((rangeSize / RANGE_SIZE) * totalSegmentCount)
      BigInteger[] segmentCountAndRemainder
          = rs.multiply(BigInteger.valueOf(totalSegmentCount)).divideAndRemainder(rangeSize);

      int segmentCount = segmentCountAndRemainder[0].intValue()
          + (segmentCountAndRemainder[1].equals(BigInteger.ZERO) ? 0 : 1);

      LOG.info("Dividing token range [{},{}) into {} segments", start, stop, segmentCount);

      // Make big0 list of all the endpoints for the repair segments, including both start and stop
      List<BigInteger> endpointTokens = Lists.newArrayList();
      for (int j = 0; j <= segmentCount; j++) {
        BigInteger offset = rs.multiply(BigInteger.valueOf(j)).divide(BigInteger.valueOf(segmentCount));
        BigInteger reaperToken = start.add(offset);
        if (greaterThan(reaperToken, rangeMax)) {
          reaperToken = reaperToken.subtract(rangeSize);
        }
        endpointTokens.add(reaperToken);
      }

      // Append the segments between the endpoints
      for (int j = 0; j < segmentCount; j++) {
        repairSegments.add(new RingRange(endpointTokens.get(j), endpointTokens.get(j + 1)));
        LOG.debug("Segment #{}: [{},{})", j + 1, endpointTokens.get(j), endpointTokens.get(j + 1));
      }
    }

    // verify that the whole range is repaired
    BigInteger total = BigInteger.ZERO;
    for (RingRange segment : repairSegments) {
      BigInteger size = segment.span(rangeSize);
      total = total.add(size);
    }
    if (!total.equals(rangeSize) && !incrementalRepair) {
      throw new ReaperException("Not entire ring would get repaired");
    }
    return repairSegments;
  }

  protected boolean inRange(BigInteger token) {
    return !(lowerThan(token, rangeMin) || greaterThan(token, rangeMax));
  }
}
