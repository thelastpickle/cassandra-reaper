package io.cassandrareaper.core;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for MetricsHistogram class. Tests the builder pattern and data access methods for
 * metrics histogram representation.
 */
public class MetricsHistogramTest {

  @Test
  public void testBuilderWithAllFields() {
    // Given: A MetricsHistogram built with all fields
    MetricsHistogram histogram =
        MetricsHistogram.builder()
            .withName("ReadLatency")
            .withType("Histogram")
            .withP50(10.5)
            .withP75(15.2)
            .withP95(25.8)
            .withP98(35.1)
            .withP99(45.7)
            .withP999(95.3)
            .withMin(1.0)
            .withMean(12.5)
            .withMax(100.0)
            .withCount(1000)
            .withOneMinuteRate(50.5)
            .withFiveMinuteRate(45.2)
            .withFifteenMinuteRate(40.8)
            .withMeanRate(42.3)
            .withStdDev(8.7)
            .build();

    // When/Then: All getters should return the correct values
    assertThat(histogram.getName()).isEqualTo("ReadLatency");
    assertThat(histogram.getType()).isEqualTo("Histogram");
    assertThat(histogram.getP50()).isEqualTo(10.5);
    assertThat(histogram.getP75()).isEqualTo(15.2);
    assertThat(histogram.getP95()).isEqualTo(25.8);
    assertThat(histogram.getP98()).isEqualTo(35.1);
    assertThat(histogram.getP99()).isEqualTo(45.7);
    assertThat(histogram.getP999()).isEqualTo(95.3);
    assertThat(histogram.getMin()).isEqualTo(1.0);
    assertThat(histogram.getMean()).isEqualTo(12.5);
    assertThat(histogram.getMax()).isEqualTo(100.0);
    assertThat(histogram.getCount()).isEqualTo(1000);
    assertThat(histogram.getOneMinuteRate()).isEqualTo(50.5);
    assertThat(histogram.getFiveMinuteRate()).isEqualTo(45.2);
    assertThat(histogram.getFifteenMinuteRate()).isEqualTo(40.8);
    assertThat(histogram.getMeanRate()).isEqualTo(42.3);
    assertThat(histogram.getStdDev()).isEqualTo(8.7);
  }

  @Test
  public void testBuilderWithMinimalFields() {
    // Given: A MetricsHistogram built with only name and type
    MetricsHistogram histogram =
        MetricsHistogram.builder().withName("WriteLatency").withType("Timer").build();

    // When/Then: Name and type should be set, others should be null
    assertThat(histogram.getName()).isEqualTo("WriteLatency");
    assertThat(histogram.getType()).isEqualTo("Timer");
    assertThat(histogram.getP50()).isNull();
    assertThat(histogram.getP75()).isNull();
    assertThat(histogram.getP95()).isNull();
    assertThat(histogram.getP98()).isNull();
    assertThat(histogram.getP99()).isNull();
    assertThat(histogram.getP999()).isNull();
    assertThat(histogram.getMin()).isNull();
    assertThat(histogram.getMean()).isNull();
    assertThat(histogram.getMax()).isNull();
    assertThat(histogram.getCount()).isNull();
    assertThat(histogram.getOneMinuteRate()).isNull();
    assertThat(histogram.getFiveMinuteRate()).isNull();
    assertThat(histogram.getFifteenMinuteRate()).isNull();
    assertThat(histogram.getMeanRate()).isNull();
    assertThat(histogram.getStdDev()).isNull();
  }

  @Test
  public void testBuilderWithPercentileFields() {
    // Given: A MetricsHistogram built with percentile fields
    MetricsHistogram histogram =
        MetricsHistogram.builder()
            .withName("CompactionLatency")
            .withType("Histogram")
            .withP50(5.0)
            .withP75(10.0)
            .withP95(20.0)
            .withP98(30.0)
            .withP99(40.0)
            .withP999(90.0)
            .build();

    // When/Then: Percentile values should be preserved
    assertThat(histogram.getName()).isEqualTo("CompactionLatency");
    assertThat(histogram.getType()).isEqualTo("Histogram");
    assertThat(histogram.getP50()).isEqualTo(5.0);
    assertThat(histogram.getP75()).isEqualTo(10.0);
    assertThat(histogram.getP95()).isEqualTo(20.0);
    assertThat(histogram.getP98()).isEqualTo(30.0);
    assertThat(histogram.getP99()).isEqualTo(40.0);
    assertThat(histogram.getP999()).isEqualTo(90.0);
  }

  @Test
  public void testBuilderWithRateFields() {
    // Given: A MetricsHistogram built with rate fields
    MetricsHistogram histogram =
        MetricsHistogram.builder()
            .withName("RequestRate")
            .withType("Meter")
            .withOneMinuteRate(100.5)
            .withFiveMinuteRate(95.2)
            .withFifteenMinuteRate(90.8)
            .withMeanRate(88.3)
            .build();

    // When/Then: Rate values should be preserved
    assertThat(histogram.getName()).isEqualTo("RequestRate");
    assertThat(histogram.getType()).isEqualTo("Meter");
    assertThat(histogram.getOneMinuteRate()).isEqualTo(100.5);
    assertThat(histogram.getFiveMinuteRate()).isEqualTo(95.2);
    assertThat(histogram.getFifteenMinuteRate()).isEqualTo(90.8);
    assertThat(histogram.getMeanRate()).isEqualTo(88.3);
  }

  @Test
  public void testBuilderWithStatisticalFields() {
    // Given: A MetricsHistogram built with statistical fields
    MetricsHistogram histogram =
        MetricsHistogram.builder()
            .withName("GCDuration")
            .withType("Timer")
            .withMin(0.1)
            .withMean(5.5)
            .withMax(25.0)
            .withStdDev(3.2)
            .withCount(500)
            .build();

    // When/Then: Statistical values should be preserved
    assertThat(histogram.getName()).isEqualTo("GCDuration");
    assertThat(histogram.getType()).isEqualTo("Timer");
    assertThat(histogram.getMin()).isEqualTo(0.1);
    assertThat(histogram.getMean()).isEqualTo(5.5);
    assertThat(histogram.getMax()).isEqualTo(25.0);
    assertThat(histogram.getStdDev()).isEqualTo(3.2);
    assertThat(histogram.getCount()).isEqualTo(500);
  }

  @Test
  public void testBuilderChaining() {
    // Given: Using builder method chaining
    MetricsHistogram histogram =
        MetricsHistogram.builder()
            .withName("ChainTest")
            .withType("Histogram")
            .withP50(1.0)
            .withP95(5.0)
            .withCount(10)
            .withMean(2.5)
            .build();

    // When/Then: All chained values should be preserved
    assertThat(histogram.getName()).isEqualTo("ChainTest");
    assertThat(histogram.getType()).isEqualTo("Histogram");
    assertThat(histogram.getP50()).isEqualTo(1.0);
    assertThat(histogram.getP95()).isEqualTo(5.0);
    assertThat(histogram.getCount()).isEqualTo(10);
    assertThat(histogram.getMean()).isEqualTo(2.5);
  }

  @Test
  public void testBuilderWithNullValues() {
    // Given: A MetricsHistogram built with explicit null values
    MetricsHistogram histogram =
        MetricsHistogram.builder()
            .withName("NullTest")
            .withType(null)
            .withP50(null)
            .withCount(null)
            .build();

    // When/Then: Null values should be preserved
    assertThat(histogram.getName()).isEqualTo("NullTest");
    assertThat(histogram.getType()).isNull();
    assertThat(histogram.getP50()).isNull();
    assertThat(histogram.getCount()).isNull();
  }

  @Test
  public void testBuilderWithZeroValues() {
    // Given: A MetricsHistogram built with zero values
    MetricsHistogram histogram =
        MetricsHistogram.builder()
            .withName("ZeroTest")
            .withType("Counter")
            .withMin(0.0)
            .withMax(0.0)
            .withMean(0.0)
            .withCount(0)
            .withOneMinuteRate(0.0)
            .build();

    // When/Then: Zero values should be preserved
    assertThat(histogram.getName()).isEqualTo("ZeroTest");
    assertThat(histogram.getType()).isEqualTo("Counter");
    assertThat(histogram.getMin()).isEqualTo(0.0);
    assertThat(histogram.getMax()).isEqualTo(0.0);
    assertThat(histogram.getMean()).isEqualTo(0.0);
    assertThat(histogram.getCount()).isEqualTo(0);
    assertThat(histogram.getOneMinuteRate()).isEqualTo(0.0);
  }

  @Test
  public void testBuilderWithNegativeValues() {
    // Given: A MetricsHistogram built with negative values
    MetricsHistogram histogram =
        MetricsHistogram.builder()
            .withName("NegativeTest")
            .withType("Gauge")
            .withMin(-10.5)
            .withMean(-5.2)
            .withMax(-1.0)
            .withCount(-1)
            .build();

    // When/Then: Negative values should be preserved
    assertThat(histogram.getName()).isEqualTo("NegativeTest");
    assertThat(histogram.getType()).isEqualTo("Gauge");
    assertThat(histogram.getMin()).isEqualTo(-10.5);
    assertThat(histogram.getMean()).isEqualTo(-5.2);
    assertThat(histogram.getMax()).isEqualTo(-1.0);
    assertThat(histogram.getCount()).isEqualTo(-1);
  }

  @Test
  public void testRealWorldScenario() {
    // Given: A realistic Cassandra read latency histogram
    MetricsHistogram histogram =
        MetricsHistogram.builder()
            .withName("Read")
            .withType("Latency")
            .withP50(2.5)
            .withP75(5.1)
            .withP95(12.8)
            .withP98(18.3)
            .withP99(25.7)
            .withP999(45.2)
            .withMin(0.5)
            .withMean(4.2)
            .withMax(50.0)
            .withCount(10000)
            .withOneMinuteRate(150.0)
            .withFiveMinuteRate(145.8)
            .withFifteenMinuteRate(142.3)
            .withMeanRate(140.5)
            .withStdDev(6.8)
            .build();

    // When/Then: All realistic values should be preserved
    assertThat(histogram.getName()).isEqualTo("Read");
    assertThat(histogram.getType()).isEqualTo("Latency");
    assertThat(histogram.getP50()).isEqualTo(2.5);
    assertThat(histogram.getP95()).isEqualTo(12.8);
    assertThat(histogram.getP99()).isEqualTo(25.7);
    assertThat(histogram.getCount()).isEqualTo(10000);
    assertThat(histogram.getMeanRate()).isEqualTo(140.5);
    assertThat(histogram.getStdDev()).isEqualTo(6.8);
  }
}
