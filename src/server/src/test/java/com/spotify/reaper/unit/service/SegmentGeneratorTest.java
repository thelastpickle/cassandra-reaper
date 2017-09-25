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

package com.spotify.reaper.unit.service;

import com.spotify.reaper.ReaperException;
import com.spotify.reaper.service.RingRange;
import com.spotify.reaper.service.SegmentGenerator;

import java.math.BigInteger;
import java.util.List;

import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public final class SegmentGeneratorTest {

  @Test
  public void testGenerateSegments() throws Exception {

    List<BigInteger> tokens = Lists.transform(Lists.newArrayList(
            "0",
            "1",
            "56713727820156410577229101238628035242",
            "56713727820156410577229101238628035243",
            "113427455640312821154458202477256070484",
            "113427455640312821154458202477256070485"),
        (String string) -> new BigInteger(string));

    SegmentGenerator generator = new SegmentGenerator("foo.bar.RandomPartitioner");
    List<RingRange> segments = generator.generateSegments(10, tokens, Boolean.FALSE);
    assertEquals(15, segments.size());

    assertEquals(
        "(0,1]",
        segments.get(0).toString());

    assertEquals(
        "(56713727820156410577229101238628035242,56713727820156410577229101238628035243]",
        segments.get(5).toString());

    assertEquals(
        "(113427455640312821154458202477256070484,113427455640312821154458202477256070485]",
        segments.get(10).toString());

    tokens = Lists.transform(Lists.newArrayList(
            "5",
            "6",
            "56713727820156410577229101238628035242",
            "56713727820156410577229101238628035243",
            "113427455640312821154458202477256070484",
            "113427455640312821154458202477256070485"),
        (String string) -> new BigInteger(string));

    segments = generator.generateSegments(10, tokens, Boolean.FALSE);
    assertEquals(15, segments.size());

    assertEquals(
        "(5,6]",
        segments.get(0).toString());

    assertEquals(
        "(56713727820156410577229101238628035242,56713727820156410577229101238628035243]",
        segments.get(5).toString());

    assertEquals(
        "(113427455640312821154458202477256070484,113427455640312821154458202477256070485]",
        segments.get(10).toString());
  }

  @Test(expected = ReaperException.class)
  public void testZeroSizeRange() throws Exception {

    List<String> tokenStrings = Lists.newArrayList(
        "0",
        "1",
        "56713727820156410577229101238628035242",
        "56713727820156410577229101238628035242",
        "113427455640312821154458202477256070484",
        "113427455640312821154458202477256070485");

    List<BigInteger> tokens = Lists.transform(tokenStrings, (String string) -> new BigInteger(string));

    SegmentGenerator generator = new SegmentGenerator("foo.bar.RandomPartitioner");
    generator.generateSegments(10, tokens, Boolean.FALSE);
  }

  @Test
  public void testRotatedRing() throws Exception {
    List<String> tokenStrings = Lists.newArrayList(
        "56713727820156410577229101238628035243",
        "113427455640312821154458202477256070484",
        "113427455640312821154458202477256070485",
        "5",
        "6",
        "56713727820156410577229101238628035242");

    List<BigInteger> tokens = Lists.transform(tokenStrings, (String string) -> new BigInteger(string));

    SegmentGenerator generator = new SegmentGenerator("foo.bar.RandomPartitioner");
    List<RingRange> segments = generator.generateSegments(10, tokens, Boolean.FALSE);
    assertEquals(15, segments.size());

    assertEquals(
        "(113427455640312821154458202477256070484,113427455640312821154458202477256070485]",
        segments.get(4).toString());

    assertEquals(

        "(5,6]",
        segments.get(9).toString());

    assertEquals(
        "(56713727820156410577229101238628035242,56713727820156410577229101238628035243]",
        segments.get(14).toString());
  }

  @Test(expected = ReaperException.class)
  public void testDisorderedRing() throws Exception {

    List<String> tokenStrings = Lists.newArrayList(
        "0",
        "113427455640312821154458202477256070485",
        "1",
        "56713727820156410577229101238628035242",
        "56713727820156410577229101238628035243",
        "113427455640312821154458202477256070484");

    List<BigInteger> tokens = Lists.transform(tokenStrings, (String string) -> new BigInteger(string));

    SegmentGenerator generator = new SegmentGenerator("foo.bar.RandomPartitioner");
    generator.generateSegments(10, tokens, Boolean.FALSE);
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
}
