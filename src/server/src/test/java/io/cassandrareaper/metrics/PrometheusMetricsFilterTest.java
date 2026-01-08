package io.cassandrareaper.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.Timer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for PrometheusMetricsFilter class. Tests metric filtering functionality for Prometheus
 * export.
 */
public class PrometheusMetricsFilterTest {

  private PrometheusMetricsFilter filter;

  @BeforeEach
  public void setUp() {
    filter = new PrometheusMetricsFilter();
  }

  @AfterEach
  public void tearDown() {
    // Clean up any ignored metrics to avoid test interference
    PrometheusMetricsFilter.removeIgnoredMetric("test.metric.1");
    PrometheusMetricsFilter.removeIgnoredMetric("test.metric.2");
    PrometheusMetricsFilter.removeIgnoredMetric(
        "io.cassandrareaper.service.RepairRunner.segmentsDone");
  }

  @Test
  public void testMatchesWithNoIgnoredMetrics() {
    // Given: No metrics are ignored
    Metric mockMetric = mock(Counter.class);

    // When: Checking if a metric matches
    boolean matches = filter.matches("test.metric", mockMetric);

    // Then: Should return true (not filtered)
    assertThat(matches).isTrue();
  }

  @Test
  public void testMatchesWithIgnoredMetric() {
    // Given: A metric is added to the ignored list
    PrometheusMetricsFilter.ignoreMetric("test.metric.ignore");
    Metric mockMetric = mock(Gauge.class);

    // When: Checking if the ignored metric matches
    boolean matches = filter.matches("test.metric.ignore", mockMetric);

    // Then: Should return false (filtered out)
    assertThat(matches).isFalse();
  }

  @Test
  public void testMatchesWithNonIgnoredMetric() {
    // Given: A different metric is ignored
    PrometheusMetricsFilter.ignoreMetric("test.metric.ignore");
    Metric mockMetric = mock(Histogram.class);

    // When: Checking if a non-ignored metric matches
    boolean matches = filter.matches("test.metric.allow", mockMetric);

    // Then: Should return true (not filtered)
    assertThat(matches).isTrue();
  }

  @Test
  public void testIgnoreMetricAddsToIgnoredSet() {
    // Given: Initial state with no ignored metrics
    Metric mockMetric = mock(Timer.class);
    assertThat(filter.matches("test.metric.new", mockMetric)).isTrue();

    // When: Adding a metric to the ignored list
    PrometheusMetricsFilter.ignoreMetric("test.metric.new");

    // Then: The metric should now be filtered
    assertThat(filter.matches("test.metric.new", mockMetric)).isFalse();
  }

  @Test
  public void testRemoveIgnoredMetricRemovesFromSet() {
    // Given: A metric is in the ignored list
    PrometheusMetricsFilter.ignoreMetric("test.metric.remove");
    Metric mockMetric = mock(Meter.class);
    assertThat(filter.matches("test.metric.remove", mockMetric)).isFalse();

    // When: Removing the metric from the ignored list
    PrometheusMetricsFilter.removeIgnoredMetric("test.metric.remove");

    // Then: The metric should no longer be filtered
    assertThat(filter.matches("test.metric.remove", mockMetric)).isTrue();
  }

  @Test
  public void testMultipleIgnoredMetrics() {
    // Given: Multiple metrics are ignored
    PrometheusMetricsFilter.ignoreMetric("metric.one");
    PrometheusMetricsFilter.ignoreMetric("metric.two");
    PrometheusMetricsFilter.ignoreMetric("metric.three");
    Metric mockMetric = mock(Counter.class);

    // When/Then: All ignored metrics should be filtered
    assertThat(filter.matches("metric.one", mockMetric)).isFalse();
    assertThat(filter.matches("metric.two", mockMetric)).isFalse();
    assertThat(filter.matches("metric.three", mockMetric)).isFalse();

    // And non-ignored metrics should pass through
    assertThat(filter.matches("metric.four", mockMetric)).isTrue();
  }

  @Test
  public void testRealWorldRepairRunnerMetrics() {
    // Given: Real Repair Runner metrics that are typically ignored
    PrometheusMetricsFilter.ignoreMetric("io.cassandrareaper.service.RepairRunner.segmentsDone");
    PrometheusMetricsFilter.ignoreMetric("io.cassandrareaper.service.RepairRunner.segmentsTotal");
    Metric mockMetric = mock(Gauge.class);

    // When/Then: Ignored metrics should be filtered
    assertThat(filter.matches("io.cassandrareaper.service.RepairRunner.segmentsDone", mockMetric))
        .isFalse();
    assertThat(filter.matches("io.cassandrareaper.service.RepairRunner.segmentsTotal", mockMetric))
        .isFalse();

    // And similar but not ignored metrics should pass through
    assertThat(
            filter.matches(
                "io.cassandrareaper.service.RepairRunner.segmentsDonePerKeyspace", mockMetric))
        .isTrue();
  }

  @Test
  public void testEmptyMetricName() {
    // Given: An empty metric name
    PrometheusMetricsFilter.ignoreMetric("");
    Metric mockMetric = mock(Counter.class);

    // When: Checking empty metric name
    boolean matches = filter.matches("", mockMetric);

    // Then: Should be filtered
    assertThat(matches).isFalse();
  }

  @Test
  public void testNullMetricName() {
    // Given: A null metric name is ignored (edge case)
    PrometheusMetricsFilter.ignoreMetric("test.null.case");
    Metric mockMetric = mock(Gauge.class);

    // When: Checking null metric name (edge case)
    boolean matches = filter.matches(null, mockMetric);

    // Then: Should return true (null doesn't match any ignored metrics)
    assertThat(matches).isTrue();
  }

  @Test
  public void testCaseSensitiveMatching() {
    // Given: A metric with specific case is ignored
    PrometheusMetricsFilter.ignoreMetric("Test.Metric.CaseSensitive");
    Metric mockMetric = mock(Histogram.class);

    // When/Then: Exact case should be filtered
    assertThat(filter.matches("Test.Metric.CaseSensitive", mockMetric)).isFalse();

    // But different case should not be filtered
    assertThat(filter.matches("test.metric.casesensitive", mockMetric)).isTrue();
    assertThat(filter.matches("TEST.METRIC.CASESENSITIVE", mockMetric)).isTrue();
  }

  @Test
  public void testFilterWithDifferentMetricTypes() {
    // Given: A metric name is ignored
    PrometheusMetricsFilter.ignoreMetric("test.metric.types");

    // When/Then: All metric types should be filtered equally
    assertThat(filter.matches("test.metric.types", mock(Counter.class))).isFalse();
    assertThat(filter.matches("test.metric.types", mock(Gauge.class))).isFalse();
    assertThat(filter.matches("test.metric.types", mock(Histogram.class))).isFalse();
    assertThat(filter.matches("test.metric.types", mock(Meter.class))).isFalse();
    assertThat(filter.matches("test.metric.types", mock(Timer.class))).isFalse();
  }
}
