/*
 * Copyright 2017-2017 Spotify AB
 * Copyright 2017-2018 The Last Pickle Ltd
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

import io.cassandrareaper.service.RingRange;

import java.math.BigInteger;
import java.util.List;

import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public final class RingRangeTest {

  @Test
  public void testSpan() throws Exception {
    RingRange r0To20 = new RingRange(BigInteger.valueOf(0L), BigInteger.valueOf(20L));
    BigInteger ringSize200 = BigInteger.valueOf(200L);
    assertEquals(20, r0To20.span(ringSize200).intValue());

    RingRange r20To0 = new RingRange(BigInteger.valueOf(20L), BigInteger.valueOf(0L));
    assertEquals(180, r20To0.span(ringSize200).intValue());
  }

  @Test
  public void testEncloses() throws Exception {
    RingRange r0To20 = new RingRange(BigInteger.valueOf(0L), BigInteger.valueOf(20L));

    RingRange r5To15 = new RingRange(BigInteger.valueOf(5L), BigInteger.valueOf(15L));
    assertTrue(r0To20.encloses(r5To15));

    RingRange r5To25 = new RingRange(BigInteger.valueOf(5L), BigInteger.valueOf(25L));
    assertFalse(r0To20.encloses(r5To25));

    RingRange r190To25 = new RingRange(BigInteger.valueOf(190L), BigInteger.valueOf(25L));
    assertFalse(r0To20.encloses(r190To25));

    RingRange r0To15 = new RingRange(BigInteger.valueOf(0L), BigInteger.valueOf(15L));
    assertTrue(r0To20.encloses(r0To15));

    RingRange r190To15 = new RingRange(BigInteger.valueOf(190L), BigInteger.valueOf(15L));
    assertFalse(r0To20.encloses(r190To15));

    RingRange r190To0 = new RingRange(BigInteger.valueOf(190L), BigInteger.valueOf(0L));
    assertFalse(r0To20.encloses(r190To0));

    RingRange r15To20 = new RingRange(BigInteger.valueOf(15L), BigInteger.valueOf(20L));
    assertTrue(r0To20.encloses(r15To20));

    assertTrue(r0To20.encloses(r0To20));

    RingRange r20To25 = new RingRange(BigInteger.valueOf(20L), BigInteger.valueOf(25L));
    assertFalse(r0To20.encloses(r20To25));

    RingRange r190To20 = new RingRange(BigInteger.valueOf(190L), BigInteger.valueOf(20L));
    assertFalse(r190To20.encloses(r5To25));

    RingRange r200To10 = new RingRange(BigInteger.valueOf(200L), BigInteger.valueOf(10L));
    assertTrue(r190To20.encloses(r200To10));

    RingRange r0To2 = new RingRange(BigInteger.valueOf(0L), BigInteger.valueOf(2L));
    RingRange r5To190 = new RingRange(BigInteger.valueOf(5L), BigInteger.valueOf(190L));
    assertFalse(r0To2.encloses(r5To190));

    // 0_0 should enclose almost everything, but is not enclosed by anything
    RingRange r15To5 = new RingRange(BigInteger.valueOf(15L), BigInteger.valueOf(5L));
    RingRange r0To0 = new RingRange(BigInteger.valueOf(0L), BigInteger.valueOf(0L));
    assertTrue(r0To0.encloses(r0To20));
    assertTrue(r0To0.encloses(r15To20));
    // the exception is that 0_0 does not enclose 15_5
    // this is because 0_0 range means one node in the cluster, and that node can't become
    // a repair coordinator for segment 15_5. This is not a biggie though, because we
    // generate the segments carefully, and prevent situation like this
    assertFalse(r0To0.encloses(r15To5));
    assertTrue(r0To0.encloses(r190To0));

    assertFalse(r0To20.encloses(r0To0));
    assertFalse(r15To20.encloses(r0To0));
    assertFalse(r5To15.encloses(r0To0));
    assertFalse(r190To0.encloses(r0To0));
  }

  @Test
  public void isWrappingTest() {
    RingRange r0To0 = new RingRange(BigInteger.valueOf(0L), BigInteger.valueOf(0L));
    assertTrue(r0To0.isWrapping());
    RingRange r0To1 = new RingRange(BigInteger.valueOf(0L), BigInteger.valueOf(1L));
    assertFalse(r0To1.isWrapping());
    RingRange r1To0 = new RingRange(BigInteger.valueOf(1L), BigInteger.valueOf(0L));
    assertTrue(r1To0.isWrapping());
  }

  @Test
  public void mergeTest() {
    List<RingRange> ranges = Lists.newArrayList();
    ranges.add(new RingRange(BigInteger.valueOf(30L), BigInteger.valueOf(50L)));
    ranges.add(new RingRange(BigInteger.valueOf(10L), BigInteger.valueOf(30L)));
    ranges.add(new RingRange(BigInteger.valueOf(80L), BigInteger.valueOf(10L)));
    RingRange merged = RingRange.merge(ranges);
    assertEquals("80", merged.getStart().toString());
    assertEquals("50", merged.getEnd().toString());
  }
}
