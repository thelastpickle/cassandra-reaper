package com.spotify.reaper.service;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.spotify.reaper.ReaperException;
import com.spotify.reaper.core.RepairSegment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.List;

/**
 * Splits given Cassandra table's (column family's) token range into RepairSegments.
 *
 * The run order of RepairSegments in RepairRun defines the RepairStrategy.
 */
public class SegmentGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(SegmentGenerator.class);

  private final String partitioner;
  private final BigInteger MIN_SEGMENT_SIZE = new BigInteger("100");
  private BigInteger RANGE_MIN;
  private BigInteger RANGE_MAX;
  private BigInteger RANGE_SIZE;

  public SegmentGenerator(String partitioner) throws ReaperException {
    if (partitioner.endsWith("RandomPartitioner")) {
      RANGE_MIN = BigInteger.ZERO;
      RANGE_MAX = new BigInteger("2").pow(127).subtract(BigInteger.ONE);
    } else if (partitioner.endsWith("Murmur3Partitioner")) {
      RANGE_MIN = new BigInteger("2").pow(63).negate();
      RANGE_MAX = new BigInteger("2").pow(63).subtract(BigInteger.ONE);
    } else {
      throw new ReaperException("Unsupported partitioner " + partitioner);
    }
    RANGE_SIZE = RANGE_MAX.subtract(RANGE_MIN).add(BigInteger.ONE);
    this.partitioner = partitioner;
  }

  /**
   * Given a properly ordered list of tokens, compute at least {@code totalSegmentCount} repair
   * segments.
   * @param totalSegmentCount requested total amount of repair segments. This function may generate
   *                          more segments.
   * @param ringTokens list of all start tokens in a cluster. They have to be in ring order.
   * @return a list containing at least {@code totalSegmentCount} repair segments.
   * @throws ReaperException
   */
  public List<RepairSegment> generateSegments(int totalSegmentCount, List<BigInteger> ringTokens)
      throws ReaperException {
    int tokenRangeCount = ringTokens.size();
    BigInteger start;
    BigInteger stop;

//    BigInteger total = BigInteger.ZERO;
    List<RepairSegment> repairSegments = Lists.newArrayList();

    for (int i = 0; i < tokenRangeCount; i++) {
      start = ringTokens.get(i);
      stop = ringTokens.get((i + 1) % tokenRangeCount);

      if (!inRange(start) || !inRange(stop)) {
        throw new ReaperException(String.format("Tokens (%s,%s) not in range of %s",
                                                start, stop, partitioner));
      }
      if (start.equals(stop)) {
        throw new ReaperException(String.format("Tokens (%s,%s): two nodes have the same token",
                                                start, stop));
      }
      if (lowerThan(stop, start)) {
        // wrap around case
        stop = stop.add(RANGE_SIZE);
      }

      BigInteger rangeSize = stop.subtract(start);
      // the below, in essence, does this:
      // segmentCount = ceiling((rangeSize / RANGE_SIZE) * totalSegmentCount)
      BigInteger[] segmentCountAndRemainder =
          rangeSize.multiply(BigInteger.valueOf(totalSegmentCount)).divideAndRemainder(RANGE_SIZE);
      int segmentCount = segmentCountAndRemainder[0].intValue() +
                         (segmentCountAndRemainder[1].equals(BigInteger.ZERO) ? 0 : 1);

      LOG.info("Dividing token range [{},{}) into {} segments", start, stop, segmentCount);

      List<BigInteger> reaperTokens = Lists.newArrayList();
      for (int j = 0; j <= segmentCount; j++) {
        BigInteger reaperToken =
            start.add(
                rangeSize
                    .multiply(BigInteger.valueOf(j))
                    .divide(BigInteger.valueOf(segmentCount)));
        if (greaterThan(reaperToken, RANGE_MAX))
          reaperToken = reaperToken.subtract(RANGE_MAX);
        reaperTokens.add(reaperToken);
      }

      for (int j = 0; j < segmentCount; j++)
      {
        repairSegments.add(new RepairSegment.RepairSegmentBuilder()
                           .startToken(reaperTokens.get(j))
                           .endToken(reaperTokens.get(j + 1))
                           .build());
        LOG.debug("Segment #{}: [{},{})", j + 1, reaperTokens.get(j),
                  reaperTokens.get(j + 1));
      }
    }

//    if (!total.equals(RANGE_SIZE)) {
//      throw new ReaperException("Not entire ring would get repaired");
//    }
    return repairSegments;
  }

  protected boolean inRange(BigInteger token) {
    if (lowerThan(token, RANGE_MIN) || greaterThan(token, RANGE_MAX)) {
      return false;
    } else {
      return true;
    }
  }

  @VisibleForTesting
  protected static BigInteger max(BigInteger a, BigInteger b) {
    return greaterThan(a, b) ? a : b;
  }

  @VisibleForTesting
  protected static BigInteger min(BigInteger a, BigInteger b) {
    return lowerThan(a, b) ? a : b;
  }

  @VisibleForTesting
  protected static boolean lowerThan(BigInteger a, BigInteger b) {
    return a.compareTo(b) < 0;
  }

  @VisibleForTesting
  protected static boolean greaterThan(BigInteger a, BigInteger b) {
    return a.compareTo(b) > 0;
  }


}
