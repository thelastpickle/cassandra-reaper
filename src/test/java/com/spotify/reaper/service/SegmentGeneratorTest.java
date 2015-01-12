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

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import com.spotify.reaper.ReaperException;

import org.junit.Test;

import java.math.BigInteger;
import java.util.List;

import javax.annotation.Nullable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SegmentGeneratorTest {

  @Test
  public void testGenerateSegments() throws Exception {
    List<BigInteger> tokens = Lists.transform(
        Lists.newArrayList(
            "0", "1",
            "56713727820156410577229101238628035242", "56713727820156410577229101238628035243",
            "113427455640312821154458202477256070484", "113427455640312821154458202477256070485"),
        new Function<String, BigInteger>() {
          @Nullable
          @Override
          public BigInteger apply(@Nullable String s) {
            return new BigInteger(s);
          }
        }
    );

    SegmentGenerator generator = new SegmentGenerator("foo.bar.RandomPartitioner");
    List<RingRange> segments = generator.generateSegments(10, tokens);
    assertEquals(15, segments.size());
    assertEquals("(0,1]",
                 segments.get(0).toString());
    assertEquals("(56713727820156410577229101238628035242,56713727820156410577229101238628035243]",
                 segments.get(5).toString());
    assertEquals("(113427455640312821154458202477256070484,113427455640312821154458202477256070485]",
                 segments.get(10).toString());


    tokens = Lists.transform(
        Lists.newArrayList(
            "5", "6",
            "56713727820156410577229101238628035242", "56713727820156410577229101238628035243",
            "113427455640312821154458202477256070484", "113427455640312821154458202477256070485"),
        new Function<String, BigInteger>() {
          @Nullable
          @Override
          public BigInteger apply(@Nullable String s) {
            return new BigInteger(s);
          }
        }
    );

    segments = generator.generateSegments(10, tokens);
    assertEquals(15, segments.size());
    assertEquals("(5,6]",
                 segments.get(0).toString());
    assertEquals("(56713727820156410577229101238628035242,56713727820156410577229101238628035243]",
                 segments.get(5).toString());
    assertEquals("(113427455640312821154458202477256070484,113427455640312821154458202477256070485]",
                 segments.get(10).toString());
  }

  @Test(expected=ReaperException.class)
  public void testZeroSizeRange() throws Exception {
    List<String> tokenStrings = Lists.newArrayList(
        "0", "1",
        "56713727820156410577229101238628035242", "56713727820156410577229101238628035242",
        "113427455640312821154458202477256070484", "113427455640312821154458202477256070485");
    List<BigInteger> tokens = Lists.transform(tokenStrings, new Function<String, BigInteger>() {
      @Nullable
      @Override
      public BigInteger apply(@Nullable String s) {
        return new BigInteger(s);
      }
    });

    SegmentGenerator generator = new SegmentGenerator("foo.bar.RandomPartitioner");
    generator.generateSegments(10, tokens);
  }

  @Test
  public void testRotatedRing() throws Exception {
    List<String> tokenStrings = Lists.newArrayList(
        "56713727820156410577229101238628035243", "113427455640312821154458202477256070484",
        "113427455640312821154458202477256070485", "5",
        "6", "56713727820156410577229101238628035242");
    List<BigInteger> tokens = Lists.transform(tokenStrings, new Function<String, BigInteger>() {
      @Nullable
      @Override
      public BigInteger apply(@Nullable String s) {
        return new BigInteger(s);
      }
    });

    SegmentGenerator generator = new SegmentGenerator("foo.bar.RandomPartitioner");
    List<RingRange> segments = generator.generateSegments(10, tokens);
    assertEquals(15, segments.size());
    assertEquals("(113427455640312821154458202477256070484,113427455640312821154458202477256070485]",
                 segments.get(4).toString());
    assertEquals("(5,6]",
                 segments.get(9).toString());
    assertEquals("(56713727820156410577229101238628035242,56713727820156410577229101238628035243]",
                 segments.get(14).toString());
  }

  @Test(expected=ReaperException.class)
  public void testDisorderedRing() throws Exception {
    List<String> tokenStrings = Lists.newArrayList(
        "0", "113427455640312821154458202477256070485", "1",
        "56713727820156410577229101238628035242", "56713727820156410577229101238628035243",
        "113427455640312821154458202477256070484");
    List<BigInteger> tokens = Lists.transform(tokenStrings, new Function<String, BigInteger>() {
      @Nullable
      @Override
      public BigInteger apply(@Nullable String s) {
        return new BigInteger(s);
      }
    });

    SegmentGenerator generator = new SegmentGenerator("foo.bar.RandomPartitioner");
    generator.generateSegments(10, tokens);
    // Will throw an exception when concluding that the repair segments don't add up.
    // This is because the tokens were supplied out of order.
  }

  @Test
  public void testMax() throws Exception {
    BigInteger one = BigInteger.ONE;
    BigInteger ten = BigInteger.TEN;
    BigInteger minusTen = BigInteger.TEN.negate();
    assertEquals(ten, SegmentGenerator.max(one, ten));
    assertEquals(ten, SegmentGenerator.max(ten, one));
    assertEquals(one, SegmentGenerator.max(one, one));
    assertEquals(one, SegmentGenerator.max(one, minusTen));
  }

  @Test
  public void testMin() throws Exception {
    BigInteger one = BigInteger.ONE;
    BigInteger ten = BigInteger.TEN;
    BigInteger minusTen = BigInteger.TEN.negate();
    assertEquals(one, SegmentGenerator.min(one, ten));
    assertEquals(one, SegmentGenerator.min(ten, one));
    assertEquals(one, SegmentGenerator.min(one, one));
    assertEquals(minusTen, SegmentGenerator.min(one, minusTen));
  }

  @Test
  public void testLowerThan() throws Exception {
    BigInteger one = BigInteger.ONE;
    BigInteger ten = BigInteger.TEN;
    BigInteger minusTen = BigInteger.TEN.negate();
    assertTrue(SegmentGenerator.lowerThan(one, ten));
    assertFalse(SegmentGenerator.lowerThan(ten, one));
    assertFalse(SegmentGenerator.lowerThan(ten, ten));
    assertTrue(SegmentGenerator.lowerThan(minusTen, one));
    assertFalse(SegmentGenerator.lowerThan(one, minusTen));
  }

  @Test
  public void testGreaterThan() throws Exception {
    BigInteger one = BigInteger.ONE;
    BigInteger ten = BigInteger.TEN;
    BigInteger minusTen = BigInteger.TEN.negate();
    assertTrue(SegmentGenerator.greaterThan(ten, one));
    assertFalse(SegmentGenerator.greaterThan(one, ten));
    assertFalse(SegmentGenerator.greaterThan(one, one));
    assertFalse(SegmentGenerator.greaterThan(minusTen, one));
    assertTrue(SegmentGenerator.greaterThan(one, minusTen));
  }
}
