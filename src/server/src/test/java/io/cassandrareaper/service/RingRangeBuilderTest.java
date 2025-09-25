/*
 * Copyright 2025-2025 DataStax, Inc.
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

import java.math.BigInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/** Unit tests for RingRange.Builder class. */
public class RingRangeBuilderTest {

  @Test
  void testBuilder_WithBigIntegerValues() {
    // Given: BigInteger start and end values
    BigInteger start = BigInteger.valueOf(100);
    BigInteger end = BigInteger.valueOf(200);

    // When: Building RingRange
    RingRange range = new RingRange.Builder().withStart(start).withEnd(end).build();

    // Then: Should create range with correct values
    assertThat(range.getStart()).isEqualTo(start);
    assertThat(range.getEnd()).isEqualTo(end);
  }

  @Test
  void testBuilder_WithStringValues() {
    // Given: String start and end values
    String start = "1000";
    String end = "2000";

    // When: Building RingRange
    RingRange range = new RingRange.Builder().withStart(start).withEnd(end).build();

    // Then: Should create range with correct values
    assertThat(range.getStart()).isEqualTo(new BigInteger("1000"));
    assertThat(range.getEnd()).isEqualTo(new BigInteger("2000"));
  }

  @Test
  void testBuilder_MixedTypes() {
    // Given: Mixed BigInteger and String values
    BigInteger start = BigInteger.valueOf(500);
    String end = "1500";

    // When: Building RingRange
    RingRange range = new RingRange.Builder().withStart(start).withEnd(end).build();

    // Then: Should create range with correct values
    assertThat(range.getStart()).isEqualTo(BigInteger.valueOf(500));
    assertThat(range.getEnd()).isEqualTo(new BigInteger("1500"));
  }

  @Test
  void testBuilder_WithLargeValues() {
    // Given: Very large token values
    String start = "0";
    String end = "170141183460469231731687303715884105727"; // Max token value

    // When: Building RingRange
    RingRange range = new RingRange.Builder().withStart(start).withEnd(end).build();

    // Then: Should handle large values correctly
    assertThat(range.getStart()).isEqualTo(BigInteger.ZERO);
    assertThat(range.getEnd()).isEqualTo(new BigInteger("170141183460469231731687303715884105727"));
  }

  @Test
  void testBuilder_WithNegativeValues() {
    // Given: Negative token values
    String start = "-9223372036854775808"; // Min Long value
    String end = "9223372036854775807"; // Max Long value

    // When: Building RingRange
    RingRange range = new RingRange.Builder().withStart(start).withEnd(end).build();

    // Then: Should handle negative values correctly
    assertThat(range.getStart()).isEqualTo(new BigInteger("-9223372036854775808"));
    assertThat(range.getEnd()).isEqualTo(new BigInteger("9223372036854775807"));
  }

  @Test
  void testBuilder_WrappingRange() {
    // Given: Start greater than end (wrapping range)
    BigInteger start = BigInteger.valueOf(1000);
    BigInteger end = BigInteger.valueOf(500);

    // When: Building RingRange
    RingRange range = new RingRange.Builder().withStart(start).withEnd(end).build();

    // Then: Should create wrapping range
    assertThat(range.getStart()).isEqualTo(start);
    assertThat(range.getEnd()).isEqualTo(end);
    assertThat(range.isWrapping()).isTrue();
  }

  @Test
  void testBuilder_EqualStartAndEnd() {
    // Given: Start equals end
    BigInteger value = BigInteger.valueOf(1000);

    // When: Building RingRange
    RingRange range = new RingRange.Builder().withStart(value).withEnd(value).build();

    // Then: Should create range where start equals end
    assertThat(range.getStart()).isEqualTo(value);
    assertThat(range.getEnd()).isEqualTo(value);
    assertThat(range.isWrapping()).isTrue(); // When start == end, it's considered wrapping
  }

  @Test
  void testBuilder_NullStart_ShouldThrowException() {
    // Given: Builder with null start
    RingRange.Builder builder = new RingRange.Builder().withEnd(BigInteger.valueOf(100));

    // When/Then: Should throw NullPointerException
    assertThatThrownBy(() -> builder.build()).isInstanceOf(NullPointerException.class);
  }

  @Test
  void testBuilder_NullEnd_ShouldThrowException() {
    // Given: Builder with null end
    RingRange.Builder builder = new RingRange.Builder().withStart(BigInteger.valueOf(100));

    // When/Then: Should throw NullPointerException
    assertThatThrownBy(() -> builder.build()).isInstanceOf(NullPointerException.class);
  }

  @Test
  void testBuilder_InvalidStringValue_ShouldThrowException() {
    // Given: Invalid string value
    RingRange.Builder builder = new RingRange.Builder().withStart("100");

    // When/Then: Should throw NumberFormatException when setting invalid string
    assertThatThrownBy(() -> builder.withEnd("not-a-number"))
        .isInstanceOf(NumberFormatException.class);
  }

  @Test
  void testBuilder_ChainedCalls() {
    // Given: Chained builder calls
    RingRange range =
        new RingRange.Builder()
            .withStart("100")
            .withEnd(BigInteger.valueOf(200))
            .withStart(BigInteger.valueOf(150)) // Override start
            .build();

    // Then: Should use the last set values
    assertThat(range.getStart()).isEqualTo(BigInteger.valueOf(150));
    assertThat(range.getEnd()).isEqualTo(BigInteger.valueOf(200));
  }

  @Test
  void testBuilder_ZeroValues() {
    // Given: Zero values for start and end
    RingRange range =
        new RingRange.Builder().withStart(BigInteger.ZERO).withEnd(BigInteger.ZERO).build();

    // Then: Should create range with zero values
    assertThat(range.getStart()).isEqualTo(BigInteger.ZERO);
    assertThat(range.getEnd()).isEqualTo(BigInteger.ZERO);
    assertThat(range.isWrapping()).isTrue();
  }
}
