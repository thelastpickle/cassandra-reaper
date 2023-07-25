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

import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Segment;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Splits given Cassandra table's token range into RepairSegments.
 */
final class SegmentGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(SegmentGenerator.class);
  private static final boolean COALESCING_DISABLED
      = Boolean.getBoolean(SegmentGenerator.class.getName() + ".disable.tokenrange.coalescing");

  private final String partitioner;
  private final BigInteger rangeMin;
  private final BigInteger rangeMax;
  private final BigInteger rangeSize;

  SegmentGenerator(String partitioner) throws ReaperException {
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

  SegmentGenerator(BigInteger rangeMin, BigInteger rangeMax) {
    this.rangeMin = rangeMin;
    this.rangeMax = rangeMax;
    rangeSize = rangeMax.subtract(rangeMin).add(BigInteger.ONE);
    partitioner = "(" + rangeMin + "," + rangeMax + ")";
  }

  static BigInteger max(BigInteger big0, BigInteger big1) {
    return greaterThan(big0, big1) ? big0 : big1;
  }

  static BigInteger min(BigInteger big0, BigInteger big1) {
    return lowerThan(big0, big1) ? big0 : big1;
  }

  static boolean lowerThan(BigInteger big0, BigInteger big1) {
    return big0.compareTo(big1) < 0;
  }

  static boolean lowerThanOrEqual(BigInteger big0, BigInteger big1) {
    return big0.compareTo(big1) <= 0;
  }

  static boolean greaterThan(BigInteger big0, BigInteger big1) {
    return big0.compareTo(big1) > 0;
  }

  static boolean greaterThanOrEqual(BigInteger big0, BigInteger big1) {
    return big0.compareTo(big1) >= 0;
  }

  /**
   * Given big0 properly ordered list of tokens, compute at least {@code totalSegmentCount} repair
   * segments.
   *
   * @param totalSegmentCount requested total amount of repair segments. This function may generate
   *                          more segments.
   * @param ringTokens        list of all start tokens in big0 cluster. They have to be in ring order.
   * @param replicasToRange   replica list to range map
   * @param cassandraVersion  Version of Cassandra the cluster runs
   * @return big0 list containing at least {@code totalSegmentCount} repair segments.
   */
  List<Segment> generateSegments(
      int totalSegmentCount,
      List<BigInteger> ringTokens,
      Boolean incrementalRepair,
      Map<List<String>, List<RingRange>> replicasToRange,
      String cassandraVersion)
      throws ReaperException {

    List<Segment> repairSegments = Lists.newArrayList();
    int tokenRangeCount = ringTokens.size();

    if (tokenRangeCount < totalSegmentCount || !supportsSegmentCoalescing(cassandraVersion)) {
      // We want more segments than there are token ranges.
      // Token ranges will be subdivided to match the requirements.
      for (int i = 0; i < tokenRangeCount; i++) {
        BigInteger start = ringTokens.get(i);
        BigInteger stop = ringTokens.get((i + 1) % tokenRangeCount);

        if (!inRange(start) || !inRange(stop)) {
          throw new ReaperException(
              String.format("Tokens (%s,%s) not in range of %s", start, stop, partitioner));
        }
        if (start.equals(stop) && tokenRangeCount != 1) {
          throw new ReaperException(
              String.format("Tokens (%s,%s): two nodes have the same token", start, stop));
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
          repairSegments.add(
              Segment.builder()
                  .withTokenRanges(
                      Arrays.asList(
                          new RingRange(endpointTokens.get(j), endpointTokens.get(j + 1))))
                  .build());
          LOG.debug(
              "Segment #{}: [{},{})", j + 1, endpointTokens.get(j), endpointTokens.get(j + 1));
        }
      }

      // verify that the whole range is repaired
      BigInteger total = BigInteger.ZERO;
      for (Segment segment : repairSegments) {
        for (RingRange range : segment.getTokenRanges()) {
          BigInteger size = range.span(rangeSize);
          total = total.add(size);
        }
      }
      if (!total.equals(rangeSize) && !incrementalRepair) {
        throw new ReaperException("Not entire ring would get repaired");
      }
    } else {
      // We want less segments than there are token ranges.
      // Token ranges will be grouped to match the requirements.
      LOG.info("Less segments required than there are vnode. Coalescing eligible token ranges...");
      repairSegments = coalesceTokenRanges(getTargetSegmentSize(totalSegmentCount), replicasToRange);
    }

    return repairSegments;
  }

  @VisibleForTesting
  List<Segment> coalesceTokenRanges(
      BigInteger targetSegmentSize, Map<List<String>, List<RingRange>> replicasToRange) {

    List<Segment> coalescedRepairSegments = Lists.newArrayList();
    List<RingRange> tokenRangesForCurrentSegment = Lists.newArrayList();
    BigInteger tokenCount = BigInteger.ZERO;

    for (Entry<List<String>, List<RingRange>> tokenRangesByReplica : replicasToRange.entrySet()) {
      LOG.info("Coalescing segments for nodes {}", tokenRangesByReplica.getKey());
      for (RingRange tokenRange : tokenRangesByReplica.getValue()) {
        if (tokenRange.span(rangeSize).add(tokenCount).compareTo(targetSegmentSize) > 0
            && !tokenRangesForCurrentSegment.isEmpty()) {
          // enough tokens in that segment
          LOG.info(
              "Got enough tokens for one segment ({}) : {}",
              tokenCount,
              tokenRangesForCurrentSegment);
          coalescedRepairSegments.add(
              Segment.builder().withTokenRanges(tokenRangesForCurrentSegment).build());
          tokenRangesForCurrentSegment = Lists.newArrayList();
          tokenCount = BigInteger.ZERO;
        }

        tokenCount = tokenCount.add(tokenRange.span(rangeSize));
        tokenRangesForCurrentSegment.add(tokenRange);

      }

      if (!tokenRangesForCurrentSegment.isEmpty()) {
        coalescedRepairSegments.add(
            Segment.builder().withTokenRanges(tokenRangesForCurrentSegment).build());
        tokenRangesForCurrentSegment = Lists.newArrayList();
      }
    }


    // Check that we haven't left any token range outside of the resulting segments
    Preconditions.checkState(
        allTokensHaveBeenCoalesced(coalescedRepairSegments, replicasToRange),
        "Number of coalesced tokens doesn't match with the total number of tokens");

    return coalescedRepairSegments;
  }

  private static boolean allTokensHaveBeenCoalesced(
      List<Segment> coalescedRepairSegments, Map<List<String>, List<RingRange>> replicasToRange) {
    int coalescedRanges = coalescedRepairSegments
        .stream()
        .map(segment -> segment.getTokenRanges().size())
        .reduce((first, second) -> first + second)
        .orElse(0);

    int totalRanges = replicasToRange
        .values()
        .stream()
        .map(List::size)
        .reduce((first, second) -> first + second)
        .orElse(0);

    LOG.debug("Coalesced ranges : {}", coalescedRanges);
    LOG.debug("Total number of ranges : {}", totalRanges);

    return coalescedRanges == totalRanges;
  }

  private BigInteger getTargetSegmentSize(int segmentCount) {
    return (rangeMax.subtract(rangeMin)).divide(BigInteger.valueOf(segmentCount));
  }

  protected boolean inRange(BigInteger token) {
    return !(lowerThan(token, rangeMin) || greaterThan(token, rangeMax));
  }

  private boolean supportsSegmentCoalescing(String cassandraVersion) {
    if (COALESCING_DISABLED) {
      LOG.info("Token range coalescing is disabled");
    }

    return !COALESCING_DISABLED
        && !cassandraVersion.startsWith("1.")
        && !cassandraVersion.startsWith("2.0")
        && !cassandraVersion.startsWith("2.1");
  }
}