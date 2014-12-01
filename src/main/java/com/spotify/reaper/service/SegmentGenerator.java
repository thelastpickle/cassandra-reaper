package com.spotify.reaper.service;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.spotify.reaper.ReaperException;
import com.spotify.reaper.core.RepairSegment;

import java.math.BigInteger;
import java.util.List;

/**
 * Splits given Cassandra table's (column family's) token range into RepairSegments.
 *
 * The run order of RepairSegments in RepairRun defines the RepairStrategy.
 */
public class SegmentGenerator {

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

  public List<RepairSegment> generateSegments(int cnt, List<String> ring) throws ReaperException {
    List<RepairSegment> repairSegments = Lists.newArrayList();
    int ringSize = ring.size();
    BigInteger count = new BigInteger(String.valueOf(cnt));
    BigInteger start;
    BigInteger stop;
    BigInteger next;
    BigInteger cur;
    BigInteger total = BigInteger.ZERO;

    for (int i = 0; i < ringSize; i++) {
      start = new BigInteger(ring.get(i));
      stop = new BigInteger(ring.get((i+1) % ringSize));
      if (!inRange(start) || !inRange(stop)) {
        throw new ReaperException(String.format("Tokens (%s,%s) not in range of %s",
            start, stop, partitioner));
      }
      if (lowerThan(stop, start)) {
        // wrap around case
        stop = stop.add(RANGE_SIZE);
      }
      BigInteger segmentSize = stop.subtract(start).divide(count).add(BigInteger.ONE);
      BigInteger rangeLength = max(MIN_SEGMENT_SIZE, segmentSize);
      cur = start;
      while (lowerThan(cur, stop)) {
        next = min(stop, cur.add(rangeLength));
        BigInteger ocur = cur;
        BigInteger onext = next;
        if (greaterThan(onext, RANGE_MAX)) {
          onext = onext.subtract(RANGE_SIZE);
        }
        if (greaterThan(ocur, RANGE_MAX)) {
          ocur = ocur.subtract(RANGE_SIZE);
        }
        //repairSegments.add(new RepairSegment(ocur, onext));
        repairSegments.add(new RepairSegment.RepairSegmentBuilder()
                               .startToken(ocur)
                               .endToken(onext)
                               .build());
        total = total.add(next).subtract(cur);
        cur = next;
      }
    }

    if (!total.equals(RANGE_SIZE)) {
      throw new ReaperException("Not entire ring would get repaired");
    }
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
    return a.compareTo(b) == -1;
  }

  @VisibleForTesting
  protected static boolean greaterThan(BigInteger a, BigInteger b) {
    return a.compareTo(b) == 1;
  }


}
