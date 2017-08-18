package com.spotify.reaper.unit.service;

import com.google.common.collect.Lists;
import com.spotify.reaper.service.RingRange;

import org.junit.Test;

import java.math.BigInteger;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RingRangeTest {

  @Test
  public void testSpan() throws Exception {
    RingRange r_0_20 = new RingRange(BigInteger.valueOf(0l), BigInteger.valueOf(20l));
    BigInteger ringSize_200 = BigInteger.valueOf(200l);
    assertEquals(20, r_0_20.span(ringSize_200).intValue());

    RingRange r_20_0 = new RingRange(BigInteger.valueOf(20l), BigInteger.valueOf(0l));
    assertEquals(180, r_20_0.span(ringSize_200).intValue());
  }

  @Test
  public void testEncloses() throws Exception {
    RingRange r_0_20 = new RingRange(BigInteger.valueOf(0l), BigInteger.valueOf(20l));

    RingRange r_5_15 = new RingRange(BigInteger.valueOf(5l), BigInteger.valueOf(15l));
    assertTrue(r_0_20.encloses(r_5_15));

    RingRange r_5_25 = new RingRange(BigInteger.valueOf(5l), BigInteger.valueOf(25l));
    assertFalse(r_0_20.encloses(r_5_25));

    RingRange r_190_25 = new RingRange(BigInteger.valueOf(190l), BigInteger.valueOf(25l));
    assertFalse(r_0_20.encloses(r_190_25));

    RingRange r_0_15 = new RingRange(BigInteger.valueOf(0l), BigInteger.valueOf(15l));
    assertTrue(r_0_20.encloses(r_0_15));

    RingRange r_190_15 = new RingRange(BigInteger.valueOf(190l), BigInteger.valueOf(15l));
    assertFalse(r_0_20.encloses(r_190_15));

    RingRange r_190_0 = new RingRange(BigInteger.valueOf(190l), BigInteger.valueOf(0l));
    assertFalse(r_0_20.encloses(r_190_0));

    RingRange r_15_20 = new RingRange(BigInteger.valueOf(15l), BigInteger.valueOf(20l));
    assertTrue(r_0_20.encloses(r_15_20));

    assertTrue(r_0_20.encloses(r_0_20));

    RingRange r_20_25 = new RingRange(BigInteger.valueOf(20l), BigInteger.valueOf(25l));
    assertFalse(r_0_20.encloses(r_20_25));

    RingRange r_190_20 = new RingRange(BigInteger.valueOf(190l), BigInteger.valueOf(20l));
    assertFalse(r_190_20.encloses(r_5_25));

    RingRange r_200_10 = new RingRange(BigInteger.valueOf(200l), BigInteger.valueOf(10l));
    assertTrue(r_190_20.encloses(r_200_10));

    RingRange r_0_2 = new RingRange(BigInteger.valueOf(0l), BigInteger.valueOf(2l));
    RingRange r_5_190 = new RingRange(BigInteger.valueOf(5l), BigInteger.valueOf(190l));
    assertFalse(r_0_2.encloses(r_5_190));

    // 0_0 should enclose almost everything, but is not enclosed by anything
    RingRange r_15_5 = new RingRange(BigInteger.valueOf(15l), BigInteger.valueOf(5l));
    RingRange r_0_0 = new RingRange(BigInteger.valueOf(0l), BigInteger.valueOf(0l));
    assertTrue(r_0_0.encloses(r_0_20));
    assertTrue(r_0_0.encloses(r_15_20));
    // the exception is that 0_0 does not enclose 15_5
    // this is because 0_0 range means one node in the cluster, and that node can't become
    // a repair coordinator for segment 15_5. This is not a biggie though, because we
    // generate the segments carefully, and prevent situation like this
    assertFalse(r_0_0.encloses(r_15_5));
    assertTrue(r_0_0.encloses(r_190_0));

    assertFalse(r_0_20.encloses(r_0_0));
    assertFalse(r_15_20.encloses(r_0_0));
    assertFalse(r_5_15.encloses(r_0_0));
    assertFalse(r_190_0.encloses(r_0_0));
  }

  @Test
  public void isWrappingTest() {
    RingRange r_0_0 = new RingRange(BigInteger.valueOf(0l), BigInteger.valueOf(0l));
    assertTrue(r_0_0.isWrapping());
    RingRange r_0_1 = new RingRange(BigInteger.valueOf(0l), BigInteger.valueOf(1l));
    assertFalse(r_0_1.isWrapping());
    RingRange r_1_0 = new RingRange(BigInteger.valueOf(1l), BigInteger.valueOf(0l));
    assertTrue(r_1_0.isWrapping());
  }

  @Test
  public void mergeTest() {
    List<RingRange> ranges = Lists.newArrayList();
    ranges.add(new RingRange(BigInteger.valueOf(30l), BigInteger.valueOf(50l)));
    ranges.add(new RingRange(BigInteger.valueOf(10l), BigInteger.valueOf(30l)));
    ranges.add(new RingRange(BigInteger.valueOf(80l), BigInteger.valueOf(10l)));
    RingRange merged = RingRange.merge(ranges);
    assertEquals("80", merged.getStart().toString());
    assertEquals("50", merged.getEnd().toString());
  }
}
