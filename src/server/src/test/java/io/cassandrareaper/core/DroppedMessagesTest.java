package io.cassandrareaper.core;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for DroppedMessages class. Tests the builder pattern and data access methods for
 * dropped message metrics.
 */
public class DroppedMessagesTest {

  @Test
  public void testBuilderWithAllFields() {
    // Given: A DroppedMessages built with all fields
    DroppedMessages droppedMessages =
        DroppedMessages.builder()
            .withName("READ_REQ")
            .withCount(100)
            .withOneMinuteRate(5.5)
            .withFiveMinuteRate(4.2)
            .withFifteenMinuteRate(3.1)
            .withMeanRate(2.8)
            .build();

    // When/Then: All getters should return the correct values
    assertThat(droppedMessages.getName()).isEqualTo("READ_REQ");
    assertThat(droppedMessages.getCount()).isEqualTo(100);
    assertThat(droppedMessages.getOneMinuteRate()).isEqualTo(5.5);
    assertThat(droppedMessages.getFiveMinuteRate()).isEqualTo(4.2);
    assertThat(droppedMessages.getFifteenMinuteRate()).isEqualTo(3.1);
    assertThat(droppedMessages.getMeanRate()).isEqualTo(2.8);
  }

  @Test
  public void testBuilderWithMinimalFields() {
    // Given: A DroppedMessages built with only name
    DroppedMessages droppedMessages = DroppedMessages.builder().withName("WRITE").build();

    // When/Then: Name should be set, others should be null
    assertThat(droppedMessages.getName()).isEqualTo("WRITE");
    assertThat(droppedMessages.getCount()).isNull();
    assertThat(droppedMessages.getOneMinuteRate()).isNull();
    assertThat(droppedMessages.getFiveMinuteRate()).isNull();
    assertThat(droppedMessages.getFifteenMinuteRate()).isNull();
    assertThat(droppedMessages.getMeanRate()).isNull();
  }

  @Test
  public void testBuilderWithNullValues() {
    // Given: A DroppedMessages built with explicit null values
    DroppedMessages droppedMessages =
        DroppedMessages.builder()
            .withName(null)
            .withCount(null)
            .withOneMinuteRate(null)
            .withFiveMinuteRate(null)
            .withFifteenMinuteRate(null)
            .withMeanRate(null)
            .build();

    // When/Then: All values should be null
    assertThat(droppedMessages.getName()).isNull();
    assertThat(droppedMessages.getCount()).isNull();
    assertThat(droppedMessages.getOneMinuteRate()).isNull();
    assertThat(droppedMessages.getFiveMinuteRate()).isNull();
    assertThat(droppedMessages.getFifteenMinuteRate()).isNull();
    assertThat(droppedMessages.getMeanRate()).isNull();
  }

  @Test
  public void testBuilderWithZeroValues() {
    // Given: A DroppedMessages built with zero values
    DroppedMessages droppedMessages =
        DroppedMessages.builder()
            .withName("BATCH_STORE")
            .withCount(0)
            .withOneMinuteRate(0.0)
            .withFiveMinuteRate(0.0)
            .withFifteenMinuteRate(0.0)
            .withMeanRate(0.0)
            .build();

    // When/Then: All values should be zero
    assertThat(droppedMessages.getName()).isEqualTo("BATCH_STORE");
    assertThat(droppedMessages.getCount()).isEqualTo(0);
    assertThat(droppedMessages.getOneMinuteRate()).isEqualTo(0.0);
    assertThat(droppedMessages.getFiveMinuteRate()).isEqualTo(0.0);
    assertThat(droppedMessages.getFifteenMinuteRate()).isEqualTo(0.0);
    assertThat(droppedMessages.getMeanRate()).isEqualTo(0.0);
  }

  @Test
  public void testBuilderWithNegativeValues() {
    // Given: A DroppedMessages built with negative values (edge case)
    DroppedMessages droppedMessages =
        DroppedMessages.builder()
            .withName("COUNTER_MUTATION")
            .withCount(-1)
            .withOneMinuteRate(-0.5)
            .withFiveMinuteRate(-1.2)
            .withFifteenMinuteRate(-2.3)
            .withMeanRate(-3.4)
            .build();

    // When/Then: Negative values should be preserved (no validation in builder)
    assertThat(droppedMessages.getName()).isEqualTo("COUNTER_MUTATION");
    assertThat(droppedMessages.getCount()).isEqualTo(-1);
    assertThat(droppedMessages.getOneMinuteRate()).isEqualTo(-0.5);
    assertThat(droppedMessages.getFiveMinuteRate()).isEqualTo(-1.2);
    assertThat(droppedMessages.getFifteenMinuteRate()).isEqualTo(-2.3);
    assertThat(droppedMessages.getMeanRate()).isEqualTo(-3.4);
  }

  @Test
  public void testBuilderWithLargeValues() {
    // Given: A DroppedMessages built with large values
    DroppedMessages droppedMessages =
        DroppedMessages.builder()
            .withName("RANGE_SLICE")
            .withCount(Integer.MAX_VALUE)
            .withOneMinuteRate(Double.MAX_VALUE)
            .withFiveMinuteRate(1000000.999)
            .withFifteenMinuteRate(500000.123)
            .withMeanRate(250000.456)
            .build();

    // When/Then: Large values should be preserved
    assertThat(droppedMessages.getName()).isEqualTo("RANGE_SLICE");
    assertThat(droppedMessages.getCount()).isEqualTo(Integer.MAX_VALUE);
    assertThat(droppedMessages.getOneMinuteRate()).isEqualTo(Double.MAX_VALUE);
    assertThat(droppedMessages.getFiveMinuteRate()).isEqualTo(1000000.999);
    assertThat(droppedMessages.getFifteenMinuteRate()).isEqualTo(500000.123);
    assertThat(droppedMessages.getMeanRate()).isEqualTo(250000.456);
  }

  @Test
  public void testBuilderWithSpecialDoubleValues() {
    // Given: A DroppedMessages built with special double values
    DroppedMessages droppedMessages =
        DroppedMessages.builder()
            .withName("MUTATION")
            .withCount(42)
            .withOneMinuteRate(Double.POSITIVE_INFINITY)
            .withFiveMinuteRate(Double.NEGATIVE_INFINITY)
            .withFifteenMinuteRate(Double.NaN)
            .withMeanRate(0.0)
            .build();

    // When/Then: Special double values should be preserved
    assertThat(droppedMessages.getName()).isEqualTo("MUTATION");
    assertThat(droppedMessages.getCount()).isEqualTo(42);
    assertThat(droppedMessages.getOneMinuteRate()).isEqualTo(Double.POSITIVE_INFINITY);
    assertThat(droppedMessages.getFiveMinuteRate()).isEqualTo(Double.NEGATIVE_INFINITY);
    assertThat(droppedMessages.getFifteenMinuteRate()).isNaN();
    assertThat(droppedMessages.getMeanRate()).isEqualTo(0.0);
  }

  @Test
  public void testBuilderWithEmptyStringName() {
    // Given: A DroppedMessages built with empty string name
    DroppedMessages droppedMessages =
        DroppedMessages.builder()
            .withName("")
            .withCount(5)
            .withOneMinuteRate(1.5)
            .withFiveMinuteRate(2.5)
            .withFifteenMinuteRate(3.5)
            .withMeanRate(4.5)
            .build();

    // When/Then: Empty string should be preserved
    assertThat(droppedMessages.getName()).isEqualTo("");
    assertThat(droppedMessages.getCount()).isEqualTo(5);
    assertThat(droppedMessages.getOneMinuteRate()).isEqualTo(1.5);
    assertThat(droppedMessages.getFiveMinuteRate()).isEqualTo(2.5);
    assertThat(droppedMessages.getFifteenMinuteRate()).isEqualTo(3.5);
    assertThat(droppedMessages.getMeanRate()).isEqualTo(4.5);
  }

  @Test
  public void testBuilderPartialSetters() {
    // Given: A DroppedMessages built with only some fields set
    DroppedMessages droppedMessages =
        DroppedMessages.builder()
            .withName("PAXOS_COMMIT")
            .withCount(25)
            .withFifteenMinuteRate(1.8)
            .build();

    // When/Then: Set fields should have values, others should be null
    assertThat(droppedMessages.getName()).isEqualTo("PAXOS_COMMIT");
    assertThat(droppedMessages.getCount()).isEqualTo(25);
    assertThat(droppedMessages.getOneMinuteRate()).isNull();
    assertThat(droppedMessages.getFiveMinuteRate()).isNull();
    assertThat(droppedMessages.getFifteenMinuteRate()).isEqualTo(1.8);
    assertThat(droppedMessages.getMeanRate()).isNull();
  }

  @Test
  public void testBuilderChaining() {
    // Given: A builder that chains multiple setter calls
    DroppedMessages.Builder builder = DroppedMessages.builder();
    DroppedMessages droppedMessages =
        builder
            .withName("READ")
            .withCount(10)
            .withOneMinuteRate(1.0)
            .withFiveMinuteRate(2.0)
            .withFifteenMinuteRate(3.0)
            .withMeanRate(4.0)
            .build();

    // When/Then: Builder chaining should work correctly
    assertThat(droppedMessages.getName()).isEqualTo("READ");
    assertThat(droppedMessages.getCount()).isEqualTo(10);
    assertThat(droppedMessages.getOneMinuteRate()).isEqualTo(1.0);
    assertThat(droppedMessages.getFiveMinuteRate()).isEqualTo(2.0);
    assertThat(droppedMessages.getFifteenMinuteRate()).isEqualTo(3.0);
    assertThat(droppedMessages.getMeanRate()).isEqualTo(4.0);
  }

  @Test
  public void testBuilderReuse() {
    // Given: A builder used to create multiple instances
    DroppedMessages.Builder builder = DroppedMessages.builder();

    // When: Building first instance
    DroppedMessages first = builder.withName("FIRST").withCount(1).build();

    // Then: First instance should have correct values
    assertThat(first.getName()).isEqualTo("FIRST");
    assertThat(first.getCount()).isEqualTo(1);

    // When: Building second instance with different values
    DroppedMessages second = builder.withName("SECOND").withCount(2).build();

    // Then: Second instance should have updated values
    assertThat(second.getName()).isEqualTo("SECOND");
    assertThat(second.getCount()).isEqualTo(2);
  }

  @Test
  public void testBuilderOverrideValues() {
    // Given: A builder where values are overridden
    DroppedMessages droppedMessages =
        DroppedMessages.builder()
            .withName("INITIAL")
            .withCount(100)
            .withName("FINAL") // Override name
            .withCount(200) // Override count
            .build();

    // When/Then: Final values should be used
    assertThat(droppedMessages.getName()).isEqualTo("FINAL");
    assertThat(droppedMessages.getCount()).isEqualTo(200);
  }
}
