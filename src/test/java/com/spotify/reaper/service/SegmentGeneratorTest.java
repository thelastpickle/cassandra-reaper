package com.spotify.reaper.service;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.spotify.reaper.ReaperException;
import com.spotify.reaper.core.RepairSegment;
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
    /*
    List<String> tokenStrings = Lists.newArrayList("0", "1",
                                             "56713727820156410577229101238628035242", "56713727820156410577229101238628035243",
                                             "113427455640312821154458202477256070484", "113427455640312821154458202477256070485");
    List<BigInteger> tokens = Lists.transform(tokenStrings, new Function<String, BigInteger>() {
      @Nullable
      @Override
      public BigInteger apply(@Nullable String s) {
        return new BigInteger(s);
      }
    });

    SegmentGenerator generator = new SegmentGenerator("foo.bar.RandomPartitioner");
    List<RepairSegment> segments = generator.generateSegments(3, tokens);
    assertEquals(11, segments.size());
    assertEquals("(0,1)", segments.get(0).toString());
    assertEquals("(1,18904575940052136859076367079542678415)", segments.get(1).toString());
    assertEquals("(18904575940052136859076367079542678415,37809151880104273718152734159085356829)",
                 segments.get(2).toString());
    assertEquals("(37809151880104273718152734159085356829,56713727820156410577229101238628035242)",
                 segments.get(3).toString());
    assertEquals("(56713727820156410577229101238628035242,75618303760208547436305468318170713657)",
                 segments.get(4).toString());
    assertEquals("(75618303760208547436305468318170713657,94522879700260684295381835397713392072)",
                 segments.get(5).toString());
    assertEquals("(94522879700260684295381835397713392072,113427455640312821154458202477256070484)",
                 segments.get(6).toString());
    assertEquals("(113427455640312821154458202477256070484,113427455640312821154458202477256070485)",
                 segments.get(7).toString());
    assertEquals("(113427455640312821154458202477256070485,132332031580364958013534569556798748900)",
                 segments.get(8).toString());
    assertEquals("(132332031580364958013534569556798748900,151236607520417094872610936636341427315)",
                 segments.get(9).toString());
    assertEquals("(151236607520417094872610936636341427315,0)", segments.get(10).toString());

    tokenStrings = Lists.newArrayList("5", "6",
                                "56713727820156410577229101238628035242", "56713727820156410577229101238628035242",
                                "113427455640312821154458202477256070484", "113427455640312821154458202477256070485");
    tokens = Lists.transform(tokenStrings, new Function<String, BigInteger>() {
      @Nullable
      @Override
      public BigInteger apply(@Nullable String s) {
        return new BigInteger(s);
      }
    });

    generator = new SegmentGenerator("foo.bar.RandomPartitioner");
    segments = generator.generateSegments(3, tokens);
    assertEquals(11, segments.size());
    assertEquals("(5,6)", segments.get(0).toString());
    assertEquals("(6,18904575940052136859076367079542678419)", segments.get(1).toString());
    assertEquals("(151236607520417094872610936636341427319,5)", segments.get(10).toString());

    tokenStrings = Lists.newArrayList("-9223372036854775808", "-9223372036854775807",
                                "-3074457345618258603", "-3074457345618258602", "3074457345618258602", "3074457345618258603");
    tokens = Lists.transform(tokenStrings, new Function<String, BigInteger>() {
      @Nullable
      @Override
      public BigInteger apply(@Nullable String s) {
        return new BigInteger(s);
      }
    });

    generator = new SegmentGenerator("foo.bar.Murmur3Partitioner");
    segments = generator.generateSegments(3, tokens);
    assertEquals(12, segments.size());
    */
  }

  @Test(expected=ReaperException.class)
  public void testZeroSizeRange() throws Exception {
    List<String> tokenStrings = Lists.newArrayList("0", "1",
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
    List<RepairSegment> segments = generator.generateSegments(3, tokens);
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
