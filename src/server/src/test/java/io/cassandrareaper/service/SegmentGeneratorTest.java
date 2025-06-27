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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Test;

public final class SegmentGeneratorTest {

  @Test
  public void testGenerateSegments() throws Exception {

    List<BigInteger> tokens =
        Lists.transform(
            Lists.newArrayList(
                "0",
                "1",
                "56713727820156410577229101238628035242",
                "56713727820156410577229101238628035243",
                "113427455640312821154458202477256070484",
                "113427455640312821154458202477256070485"),
            (String string) -> new BigInteger(string));

    SegmentGenerator generator = new SegmentGenerator("foo.bar.RandomPartitioner");

    List<Segment> segments =
        generator.generateSegments(10, tokens, Boolean.FALSE, Maps.newHashMap(), "2.2.10");

    assertEquals(15, segments.size());

    assertEquals("(0,1]", segments.get(0).getBaseRange().toString());

    assertEquals(
        "(56713727820156410577229101238628035242,56713727820156410577229101238628035243]",
        segments.get(5).getBaseRange().toString());

    assertEquals(
        "(113427455640312821154458202477256070484,113427455640312821154458202477256070485]",
        segments.get(10).getBaseRange().toString());

    tokens =
        Lists.transform(
            Lists.newArrayList(
                "5",
                "6",
                "56713727820156410577229101238628035242",
                "56713727820156410577229101238628035243",
                "113427455640312821154458202477256070484",
                "113427455640312821154458202477256070485"),
            (String string) -> new BigInteger(string));

    segments = generator.generateSegments(10, tokens, Boolean.FALSE, Maps.newHashMap(), "2.2.10");
    assertEquals(15, segments.size());

    assertEquals("(5,6]", segments.get(0).getBaseRange().toString());

    assertEquals(
        "(56713727820156410577229101238628035242,56713727820156410577229101238628035243]",
        segments.get(5).getBaseRange().toString());

    assertEquals(
        "(113427455640312821154458202477256070484,113427455640312821154458202477256070485]",
        segments.get(10).getBaseRange().toString());
  }

  @Test(expected = ReaperException.class)
  public void testZeroSizeRange() throws Exception {

    List<String> tokenStrings =
        Lists.newArrayList(
            "0",
            "1",
            "56713727820156410577229101238628035242",
            "56713727820156410577229101238628035242",
            "113427455640312821154458202477256070484",
            "113427455640312821154458202477256070485");

    List<BigInteger> tokens =
        Lists.transform(tokenStrings, (String string) -> new BigInteger(string));

    SegmentGenerator generator = new SegmentGenerator("foo.bar.RandomPartitioner");
    generator.generateSegments(10, tokens, Boolean.FALSE, Maps.newHashMap(), "2.2.10");
  }

  @Test
  public void testRotatedRing() throws Exception {
    List<String> tokenStrings =
        Lists.newArrayList(
            "56713727820156410577229101238628035243",
            "113427455640312821154458202477256070484",
            "113427455640312821154458202477256070485",
            "5",
            "6",
            "56713727820156410577229101238628035242");

    List<BigInteger> tokens =
        Lists.transform(tokenStrings, (String string) -> new BigInteger(string));

    SegmentGenerator generator = new SegmentGenerator("foo.bar.RandomPartitioner");

    List<Segment> segments =
        generator.generateSegments(10, tokens, Boolean.FALSE, Maps.newHashMap(), "2.2.10");

    assertEquals(15, segments.size());

    assertEquals(
        "(113427455640312821154458202477256070484,113427455640312821154458202477256070485]",
        segments.get(4).getBaseRange().toString());

    assertEquals("(5,6]", segments.get(9).getBaseRange().toString());

    assertEquals(
        "(56713727820156410577229101238628035242,56713727820156410577229101238628035243]",
        segments.get(14).getBaseRange().toString());
  }

  @Test(expected = ReaperException.class)
  public void testDisorderedRing() throws Exception {

    List<String> tokenStrings =
        Lists.newArrayList(
            "0",
            "113427455640312821154458202477256070485",
            "1",
            "56713727820156410577229101238628035242",
            "56713727820156410577229101238628035243",
            "113427455640312821154458202477256070484");

    List<BigInteger> tokens =
        Lists.transform(tokenStrings, (String string) -> new BigInteger(string));

    SegmentGenerator generator = new SegmentGenerator("foo.bar.RandomPartitioner");
    generator.generateSegments(10, tokens, Boolean.FALSE, Maps.newHashMap(), "2.2.10");
    // Will throw an exception when concluding that the repair segments don't add up.
    // This is because the tokens were supplied out of order.
  }

  @Test
  public void testMax() throws Exception {
    BigInteger one = BigInteger.ONE;
    BigInteger ten = BigInteger.TEN;
    assertEquals(ten, SegmentGenerator.max(one, ten));
    assertEquals(ten, SegmentGenerator.max(ten, one));
    assertEquals(one, SegmentGenerator.max(one, one));
    BigInteger minusTen = BigInteger.TEN.negate();
    assertEquals(one, SegmentGenerator.max(one, minusTen));
  }

  @Test
  public void testMin() throws Exception {
    BigInteger one = BigInteger.ONE;
    BigInteger ten = BigInteger.TEN;
    assertEquals(one, SegmentGenerator.min(one, ten));
    assertEquals(one, SegmentGenerator.min(ten, one));
    assertEquals(one, SegmentGenerator.min(one, one));
    BigInteger minusTen = BigInteger.TEN.negate();
    assertEquals(minusTen, SegmentGenerator.min(one, minusTen));
  }

  @Test
  public void testLowerThan() throws Exception {
    BigInteger one = BigInteger.ONE;
    BigInteger ten = BigInteger.TEN;
    assertTrue(SegmentGenerator.lowerThan(one, ten));
    assertFalse(SegmentGenerator.lowerThan(ten, one));
    assertFalse(SegmentGenerator.lowerThan(ten, ten));
    BigInteger minusTen = BigInteger.TEN.negate();
    assertTrue(SegmentGenerator.lowerThan(minusTen, one));
    assertFalse(SegmentGenerator.lowerThan(one, minusTen));
  }

  @Test
  public void testGreaterThan() throws Exception {
    BigInteger one = BigInteger.ONE;
    BigInteger ten = BigInteger.TEN;
    assertTrue(SegmentGenerator.greaterThan(ten, one));
    assertFalse(SegmentGenerator.greaterThan(one, ten));
    assertFalse(SegmentGenerator.greaterThan(one, one));
    BigInteger minusTen = BigInteger.TEN.negate();
    assertFalse(SegmentGenerator.greaterThan(minusTen, one));
    assertTrue(SegmentGenerator.greaterThan(one, minusTen));
  }

  @Test
  public void coalesceTokenRangesTests() throws ReaperException {
    SegmentGenerator sg = new SegmentGenerator(BigInteger.valueOf(1), BigInteger.valueOf(1600));
    Map<List<String>, List<String>> rangeToEndpoint = Maps.newHashMap();
    rangeToEndpoint.put(Arrays.asList("1", "200"), Arrays.asList("node1", "node2", "node3"));
    rangeToEndpoint.put(Arrays.asList("200", "400"), Arrays.asList("node1", "node2", "node3"));
    rangeToEndpoint.put(Arrays.asList("400", "600"), Arrays.asList("node1"));
    rangeToEndpoint.put(Arrays.asList("600", "800"), Arrays.asList("node1", "node2"));
    rangeToEndpoint.put(Arrays.asList("800", "1000"), Arrays.asList("node1", "node2"));
    rangeToEndpoint.put(Arrays.asList("1100", "1200"), Arrays.asList("node2", "node3", "node1"));

    Map<List<String>, List<RingRange>> replicasToRangeMap =
        RepairRunService.buildReplicasToRangeMap(rangeToEndpoint);

    List<Segment> segments = sg.coalesceTokenRanges(BigInteger.valueOf(1200), replicasToRangeMap);

    // We have 3 different sets of replicas so we can't have less than 3 segments
    assertEquals(3, segments.size());
  }

  @Test
  public void coalesceTokenRangesTooFewTokensPerSegmentTests() throws ReaperException {
    SegmentGenerator sg = new SegmentGenerator(BigInteger.valueOf(1), BigInteger.valueOf(1600));
    Map<List<String>, List<String>> rangeToEndpoint = Maps.newHashMap();
    rangeToEndpoint.put(Arrays.asList("1", "200"), Arrays.asList("node1", "node2", "node3"));
    rangeToEndpoint.put(Arrays.asList("200", "400"), Arrays.asList("node1", "node2", "node3"));
    rangeToEndpoint.put(Arrays.asList("400", "600"), Arrays.asList("node1", "node2", "node3"));
    rangeToEndpoint.put(Arrays.asList("600", "800"), Arrays.asList("node1", "node2", "node3"));
    rangeToEndpoint.put(Arrays.asList("800", "1000"), Arrays.asList("node1", "node2", "node3"));
    rangeToEndpoint.put(Arrays.asList("1100", "1200"), Arrays.asList("node2", "node3", "node1"));
    rangeToEndpoint.put(Arrays.asList("1200", "1300"), Arrays.asList("node2", "node3", "node1"));
    rangeToEndpoint.put(Arrays.asList("1300", "1400"), Arrays.asList("node2", "node3", "node1"));
    rangeToEndpoint.put(Arrays.asList("1400", "1500"), Arrays.asList("node2", "node3", "node1"));

    Map<List<String>, List<RingRange>> replicasToRangeMap =
        RepairRunService.buildReplicasToRangeMap(rangeToEndpoint);

    List<Segment> segments = sg.coalesceTokenRanges(BigInteger.valueOf(1), replicasToRangeMap);

    // number of tokens per segment is smaller than the number of tokens per range
    // Generating a single segment per token range
    assertEquals(9, segments.size());
  }

  @Test
  public void coalesceTokenRangesBy200TokensPerSegmentTests() throws ReaperException {
    SegmentGenerator sg = new SegmentGenerator(BigInteger.valueOf(1), BigInteger.valueOf(1500));
    Map<List<String>, List<String>> rangeToEndpoint = Maps.newHashMap();
    rangeToEndpoint.put(Arrays.asList("1", "200"), Arrays.asList("node1", "node2", "node3"));
    rangeToEndpoint.put(Arrays.asList("200", "400"), Arrays.asList("node1", "node2", "node3"));
    rangeToEndpoint.put(Arrays.asList("400", "600"), Arrays.asList("node1", "node2", "node3"));
    rangeToEndpoint.put(Arrays.asList("600", "800"), Arrays.asList("node1", "node2", "node3"));
    rangeToEndpoint.put(Arrays.asList("800", "1000"), Arrays.asList("node1", "node2", "node3"));
    rangeToEndpoint.put(Arrays.asList("1100", "1200"), Arrays.asList("node1", "node2", "node3"));
    rangeToEndpoint.put(Arrays.asList("1200", "1300"), Arrays.asList("node1", "node2", "node3"));
    rangeToEndpoint.put(Arrays.asList("1300", "1400"), Arrays.asList("node1", "node2", "node3"));
    rangeToEndpoint.put(Arrays.asList("1400", "1500"), Arrays.asList("node1", "node2", "node3"));

    Map<List<String>, List<RingRange>> replicasToRangeMap =
        RepairRunService.buildReplicasToRangeMap(rangeToEndpoint);

    List<Segment> segments = sg.coalesceTokenRanges(BigInteger.valueOf(200), replicasToRangeMap);

    // Ranges with 100 tokens will get coalesced two by two
    assertEquals(7, segments.size());
  }

  @Test
  void testConstructor_RandomPartitioner() throws ReaperException {
    // When: Creating generator with RandomPartitioner
    SegmentGenerator generator = new SegmentGenerator("org.apache.cassandra.dht.RandomPartitioner");

    // Then: Should set correct range for RandomPartitioner
    assertThat(generator.inRange(BigInteger.ZERO)).isTrue();
    assertThat(generator.inRange(new BigInteger("2").pow(127).subtract(BigInteger.ONE))).isTrue();
  }

  @Test
  void testConstructor_Murmur3Partitioner() throws ReaperException {
    // When: Creating generator with Murmur3Partitioner
    SegmentGenerator generator =
        new SegmentGenerator("org.apache.cassandra.dht.Murmur3Partitioner");

    // Then: Should set correct range for Murmur3Partitioner
    assertThat(generator.inRange(new BigInteger("2").pow(63).negate())).isTrue();
    assertThat(generator.inRange(new BigInteger("2").pow(63).subtract(BigInteger.ONE))).isTrue();
  }

  @Test
  void testConstructor_UnsupportedPartitioner() {
    // When/Then: Should throw exception for unsupported partitioner
    assertThatThrownBy(() -> new SegmentGenerator("UnsupportedPartitioner"))
        .isInstanceOf(ReaperException.class)
        .hasMessageContaining("Unsupported partitioner");
  }

  @Test
  void testConstructor_CustomRange() {
    // Given: Custom range
    BigInteger rangeMin = BigInteger.valueOf(-1000);
    BigInteger rangeMax = BigInteger.valueOf(1000);

    // When: Creating generator with custom range
    SegmentGenerator generator = new SegmentGenerator(rangeMin, rangeMax);

    // Then: Should use custom range
    assertThat(generator.inRange(BigInteger.valueOf(-500))).isTrue();
    assertThat(generator.inRange(BigInteger.valueOf(500))).isTrue();
    assertThat(generator.inRange(BigInteger.valueOf(-1001))).isFalse();
    assertThat(generator.inRange(BigInteger.valueOf(1001))).isFalse();
  }

  @Test
  void testUtilityMethods_Max() {
    // Given: Two BigIntegers
    BigInteger big1 = BigInteger.valueOf(100);
    BigInteger big2 = BigInteger.valueOf(200);

    // When/Then: Testing max method
    assertThat(SegmentGenerator.max(big1, big2)).isEqualTo(big2);
    assertThat(SegmentGenerator.max(big2, big1)).isEqualTo(big2);
    assertThat(SegmentGenerator.max(big1, big1)).isEqualTo(big1);
  }

  @Test
  void testUtilityMethods_Min() {
    // Given: Two BigIntegers
    BigInteger big1 = BigInteger.valueOf(100);
    BigInteger big2 = BigInteger.valueOf(200);

    // When/Then: Testing min method
    assertThat(SegmentGenerator.min(big1, big2)).isEqualTo(big1);
    assertThat(SegmentGenerator.min(big2, big1)).isEqualTo(big1);
    assertThat(SegmentGenerator.min(big1, big1)).isEqualTo(big1);
  }

  @Test
  void testUtilityMethods_LowerThan() {
    // Given: Two BigIntegers
    BigInteger big1 = BigInteger.valueOf(100);
    BigInteger big2 = BigInteger.valueOf(200);

    // When/Then: Testing lowerThan method
    assertThat(SegmentGenerator.lowerThan(big1, big2)).isTrue();
    assertThat(SegmentGenerator.lowerThan(big2, big1)).isFalse();
    assertThat(SegmentGenerator.lowerThan(big1, big1)).isFalse();
  }

  @Test
  void testUtilityMethods_LowerThanOrEqual() {
    // Given: Two BigIntegers
    BigInteger big1 = BigInteger.valueOf(100);
    BigInteger big2 = BigInteger.valueOf(200);

    // When/Then: Testing lowerThanOrEqual method
    assertThat(SegmentGenerator.lowerThanOrEqual(big1, big2)).isTrue();
    assertThat(SegmentGenerator.lowerThanOrEqual(big2, big1)).isFalse();
    assertThat(SegmentGenerator.lowerThanOrEqual(big1, big1)).isTrue();
  }

  @Test
  void testUtilityMethods_GreaterThan() {
    // Given: Two BigIntegers
    BigInteger big1 = BigInteger.valueOf(100);
    BigInteger big2 = BigInteger.valueOf(200);

    // When/Then: Testing greaterThan method
    assertThat(SegmentGenerator.greaterThan(big1, big2)).isFalse();
    assertThat(SegmentGenerator.greaterThan(big2, big1)).isTrue();
    assertThat(SegmentGenerator.greaterThan(big1, big1)).isFalse();
  }

  @Test
  void testUtilityMethods_GreaterThanOrEqual() {
    // Given: Two BigIntegers
    BigInteger big1 = BigInteger.valueOf(100);
    BigInteger big2 = BigInteger.valueOf(200);

    // When/Then: Testing greaterThanOrEqual method
    assertThat(SegmentGenerator.greaterThanOrEqual(big1, big2)).isFalse();
    assertThat(SegmentGenerator.greaterThanOrEqual(big2, big1)).isTrue();
    assertThat(SegmentGenerator.greaterThanOrEqual(big1, big1)).isTrue();
  }

  @Test
  void testGenerateSegments_MoreSegmentsThanTokenRanges() throws ReaperException {
    // Given: Generator and simple token ring
    SegmentGenerator generator = new SegmentGenerator(BigInteger.ZERO, BigInteger.valueOf(1000));
    List<BigInteger> ringTokens =
        Arrays.asList(
            BigInteger.valueOf(0),
            BigInteger.valueOf(250),
            BigInteger.valueOf(500),
            BigInteger.valueOf(750));
    Map<List<String>, List<RingRange>> replicasToRange = createSimpleReplicasToRange();

    // When: Requesting more segments than token ranges
    List<Segment> segments =
        generator.generateSegments(
            10, // More than 4 token ranges
            ringTokens,
            false, // Not incremental repair
            replicasToRange,
            "3.11.0");

    // Then: Should subdivide token ranges
    assertThat(segments).hasSizeGreaterThan(4);
    assertThat(segments).hasSize(10); // Should match requested count
  }

  @Test
  void testGenerateSegments_LessSegmentsThanTokenRanges() throws ReaperException {
    // Given: Generator and many token ranges
    SegmentGenerator generator = new SegmentGenerator(BigInteger.ZERO, BigInteger.valueOf(1000));
    List<BigInteger> ringTokens =
        Arrays.asList(
            BigInteger.valueOf(0),
            BigInteger.valueOf(100),
            BigInteger.valueOf(200),
            BigInteger.valueOf(300),
            BigInteger.valueOf(400),
            BigInteger.valueOf(500),
            BigInteger.valueOf(600),
            BigInteger.valueOf(700),
            BigInteger.valueOf(800),
            BigInteger.valueOf(900));
    Map<List<String>, List<RingRange>> replicasToRange = createComplexReplicasToRange();

    // When: Requesting fewer segments than token ranges
    List<Segment> segments =
        generator.generateSegments(
            3, // Less than 10 token ranges
            ringTokens,
            false, // Not incremental repair
            replicasToRange,
            "3.11.0");

    // Then: Should coalesce token ranges
    assertThat(segments).hasSizeLessThan(10);
  }

  @Test
  void testGenerateSegments_IncrementalRepair() throws ReaperException {
    // Given: Generator and token ring
    SegmentGenerator generator = new SegmentGenerator(BigInteger.ZERO, BigInteger.valueOf(1000));
    List<BigInteger> ringTokens = Arrays.asList(BigInteger.valueOf(0), BigInteger.valueOf(500));
    Map<List<String>, List<RingRange>> replicasToRange = createSimpleReplicasToRange();

    // When: Generating segments for incremental repair
    List<Segment> segments =
        generator.generateSegments(
            4,
            ringTokens,
            true, // Incremental repair
            replicasToRange,
            "3.11.0");

    // Then: Should generate segments normally (incremental flag affects validation)
    assertThat(segments).isNotEmpty();
  }

  @Test
  void testGenerateSegments_OldCassandraVersion() throws ReaperException {
    // Given: Generator and old Cassandra version
    SegmentGenerator generator = new SegmentGenerator(BigInteger.ZERO, BigInteger.valueOf(1000));
    List<BigInteger> ringTokens =
        Arrays.asList(
            BigInteger.valueOf(0),
            BigInteger.valueOf(100),
            BigInteger.valueOf(200),
            BigInteger.valueOf(300),
            BigInteger.valueOf(400),
            BigInteger.valueOf(500));
    Map<List<String>, List<RingRange>> replicasToRange = createComplexReplicasToRange();

    // When: Generating segments with old Cassandra version (no coalescing support)
    List<Segment> segments =
        generator.generateSegments(
            3, // Less than 6 token ranges
            ringTokens,
            false,
            replicasToRange,
            "2.0.0" // Old version
            );

    // Then: Should not coalesce (subdivide instead)
    assertThat(segments).hasSizeGreaterThanOrEqualTo(6);
  }

  @Test
  void testGenerateSegments_TokensOutOfRange() throws ReaperException {
    // Given: Generator with limited range
    SegmentGenerator generator =
        new SegmentGenerator(BigInteger.valueOf(100), BigInteger.valueOf(200));
    List<BigInteger> ringTokens =
        Arrays.asList(
            BigInteger.valueOf(50), // Out of range
            BigInteger.valueOf(150));
    Map<List<String>, List<RingRange>> replicasToRange = createSimpleReplicasToRange();

    // When/Then: Should throw exception for tokens out of range
    assertThatThrownBy(
            () -> generator.generateSegments(2, ringTokens, false, replicasToRange, "3.11.0"))
        .isInstanceOf(ReaperException.class)
        .hasMessageContaining("not in range");
  }

  @Test
  void testGenerateSegments_DuplicateTokens() throws ReaperException {
    // Given: Generator with duplicate tokens
    SegmentGenerator generator = new SegmentGenerator(BigInteger.ZERO, BigInteger.valueOf(1000));
    List<BigInteger> ringTokens =
        Arrays.asList(
            BigInteger.valueOf(100),
            BigInteger.valueOf(100), // Duplicate
            BigInteger.valueOf(200));
    Map<List<String>, List<RingRange>> replicasToRange = createSimpleReplicasToRange();

    // When/Then: Should throw exception for duplicate tokens
    assertThatThrownBy(
            () -> generator.generateSegments(2, ringTokens, false, replicasToRange, "3.11.0"))
        .isInstanceOf(ReaperException.class)
        .hasMessageContaining("two nodes have the same token");
  }

  @Test
  void testGenerateSegments_SingleToken() throws ReaperException {
    // Given: Generator with single token (special case)
    SegmentGenerator generator = new SegmentGenerator(BigInteger.ZERO, BigInteger.valueOf(1000));
    List<BigInteger> ringTokens = Arrays.asList(BigInteger.valueOf(100));
    Map<List<String>, List<RingRange>> replicasToRange = createSimpleReplicasToRange();

    // When: Generating segments with single token
    List<Segment> segments =
        generator.generateSegments(3, ringTokens, false, replicasToRange, "3.11.0");

    // Then: Should handle single token case
    assertThat(segments).hasSize(3);
  }

  @Test
  void testGenerateSegments_WrapAroundCase() throws ReaperException {
    // Given: Generator with wrap-around token range
    SegmentGenerator generator =
        new SegmentGenerator(BigInteger.valueOf(-100), BigInteger.valueOf(100));
    List<BigInteger> ringTokens = Arrays.asList(BigInteger.valueOf(50), BigInteger.valueOf(-50));
    Map<List<String>, List<RingRange>> replicasToRange = createSimpleReplicasToRange();

    // When: Generating segments with wrap-around
    List<Segment> segments =
        generator.generateSegments(4, ringTokens, false, replicasToRange, "3.11.0");

    // Then: Should handle wrap-around correctly
    assertThat(segments).hasSize(4);
  }

  @Test
  void testCoalesceTokenRanges_SimpleCase() {
    // Given: Generator and simple replicas to range map
    SegmentGenerator generator = new SegmentGenerator(BigInteger.ZERO, BigInteger.valueOf(1000));
    BigInteger targetSegmentSize = BigInteger.valueOf(400);
    Map<List<String>, List<RingRange>> replicasToRange = createSimpleReplicasToRange();

    // When: Coalescing token ranges
    List<Segment> segments = generator.coalesceTokenRanges(targetSegmentSize, replicasToRange);

    // Then: Should coalesce ranges appropriately
    assertThat(segments).isNotEmpty();
  }

  @Test
  void testCoalesceTokenRanges_LargeTargetSize() {
    // Given: Generator with very large target segment size
    SegmentGenerator generator = new SegmentGenerator(BigInteger.ZERO, BigInteger.valueOf(1000));
    BigInteger targetSegmentSize = BigInteger.valueOf(10000); // Larger than total range
    Map<List<String>, List<RingRange>> replicasToRange = createSimpleReplicasToRange();

    // When: Coalescing token ranges
    List<Segment> segments = generator.coalesceTokenRanges(targetSegmentSize, replicasToRange);

    // Then: Should create minimal segments
    assertThat(segments).isNotEmpty();
  }

  @Test
  void testCoalesceTokenRanges_SmallTargetSize() {
    // Given: Generator with very small target segment size
    SegmentGenerator generator = new SegmentGenerator(BigInteger.ZERO, BigInteger.valueOf(1000));
    BigInteger targetSegmentSize = BigInteger.valueOf(1); // Very small
    Map<List<String>, List<RingRange>> replicasToRange = createComplexReplicasToRange();

    // When: Coalescing token ranges
    List<Segment> segments = generator.coalesceTokenRanges(targetSegmentSize, replicasToRange);

    // Then: Should create many small segments
    assertThat(segments).isNotEmpty();
  }

  @Test
  void testCoalesceTokenRanges_EmptyReplicasMap() {
    // Given: Generator with empty replicas map
    SegmentGenerator generator = new SegmentGenerator(BigInteger.ZERO, BigInteger.valueOf(1000));
    BigInteger targetSegmentSize = BigInteger.valueOf(100);
    Map<List<String>, List<RingRange>> replicasToRange = new HashMap<>();

    // When: Coalescing empty token ranges
    List<Segment> segments = generator.coalesceTokenRanges(targetSegmentSize, replicasToRange);

    // Then: Should return empty list
    assertThat(segments).isEmpty();
  }

  @Test
  void testInRange_WithinRange() {
    // Given: Generator with specific range
    SegmentGenerator generator =
        new SegmentGenerator(BigInteger.valueOf(100), BigInteger.valueOf(200));

    // When/Then: Testing tokens within range
    assertThat(generator.inRange(BigInteger.valueOf(100))).isTrue();
    assertThat(generator.inRange(BigInteger.valueOf(150))).isTrue();
    assertThat(generator.inRange(BigInteger.valueOf(200))).isTrue();
  }

  @Test
  void testInRange_OutsideRange() {
    // Given: Generator with specific range
    SegmentGenerator generator =
        new SegmentGenerator(BigInteger.valueOf(100), BigInteger.valueOf(200));

    // When/Then: Testing tokens outside range
    assertThat(generator.inRange(BigInteger.valueOf(99))).isFalse();
    assertThat(generator.inRange(BigInteger.valueOf(201))).isFalse();
  }

  private Map<List<String>, List<RingRange>> createSimpleReplicasToRange() {
    Map<List<String>, List<RingRange>> replicasToRange = new HashMap<>();

    List<String> replicas1 = Arrays.asList("node1", "node2");
    List<RingRange> ranges1 =
        Arrays.asList(
            new RingRange(BigInteger.valueOf(0), BigInteger.valueOf(250)),
            new RingRange(BigInteger.valueOf(250), BigInteger.valueOf(500)));

    List<String> replicas2 = Arrays.asList("node2", "node3");
    List<RingRange> ranges2 =
        Arrays.asList(
            new RingRange(BigInteger.valueOf(500), BigInteger.valueOf(750)),
            new RingRange(BigInteger.valueOf(750), BigInteger.valueOf(1000)));

    replicasToRange.put(replicas1, ranges1);
    replicasToRange.put(replicas2, ranges2);

    return replicasToRange;
  }

  private Map<List<String>, List<RingRange>> createComplexReplicasToRange() {
    Map<List<String>, List<RingRange>> replicasToRange = new HashMap<>();

    // Create multiple replica sets with smaller ranges
    for (int i = 0; i < 5; i++) {
      List<String> replicas = Arrays.asList("node" + i, "node" + (i + 1));
      List<RingRange> ranges =
          Arrays.asList(
              new RingRange(BigInteger.valueOf(i * 200), BigInteger.valueOf((i + 1) * 200)));
      replicasToRange.put(replicas, ranges);
    }

    return replicasToRange;
  }
}
