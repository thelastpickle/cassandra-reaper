package io.cassandrareaper.core;

import static org.assertj.core.api.Assertions.assertThat;

import org.joda.time.DateTime;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for GenericMetric class. Tests the builder pattern, data access methods, and utility
 * methods for generic metric representation.
 */
public class GenericMetricTest {

  private static final DateTime TEST_TIMESTAMP = DateTime.now();

  @Test
  public void testBuilderWithAllFields() {
    // Given: A GenericMetric built with all fields
    GenericMetric metric =
        GenericMetric.builder()
            .withClusterName("test-cluster")
            .withHost("127.0.0.1")
            .withMetricDomain("org.apache.cassandra.metrics")
            .withMetricType("ThreadPools")
            .withMetricScope("ReadStage")
            .withMetricName("ActiveTasks")
            .withMetricAttribute("Value")
            .withValue(42.5)
            .withTs(TEST_TIMESTAMP)
            .build();

    // When/Then: All getters should return the correct values
    assertThat(metric.getClusterName()).isEqualTo("test-cluster");
    assertThat(metric.getHost()).isEqualTo("127.0.0.1");
    assertThat(metric.getMetricDomain()).isEqualTo("org.apache.cassandra.metrics");
    assertThat(metric.getMetricType()).isEqualTo("ThreadPools");
    assertThat(metric.getMetricScope()).isEqualTo("ReadStage");
    assertThat(metric.getMetricName()).isEqualTo("ActiveTasks");
    assertThat(metric.getMetricAttribute()).isEqualTo("Value");
    assertThat(metric.getValue()).isEqualTo(42.5);
    assertThat(metric.getTs()).isEqualTo(TEST_TIMESTAMP);
  }

  @Test
  public void testGetMetricFullId() {
    // Given: A GenericMetric with domain, type, scope, and name
    GenericMetric metric =
        GenericMetric.builder()
            .withMetricDomain("org.apache.cassandra.metrics")
            .withMetricType("ClientRequest")
            .withMetricScope("Read")
            .withMetricName("Latency")
            .build();

    // When: Getting the full metric ID
    String fullId = metric.getMetricFullId();

    // Then: Should return the formatted string
    assertThat(fullId)
        .isEqualTo("org.apache.cassandra.metrics:type=ClientRequest,scope=Read,name=Latency");
  }

  @Test
  public void testToString() {
    // Given: A GenericMetric with all relevant fields
    GenericMetric metric =
        GenericMetric.builder()
            .withMetricDomain("org.apache.cassandra.metrics")
            .withMetricType("DroppedMessage")
            .withMetricScope("READ")
            .withMetricName("Dropped")
            .withValue(123.45)
            .build();

    // When: Converting to string
    String result = metric.toString();

    // Then: Should return the formatted string with value
    assertThat(result)
        .isEqualTo(
            "org.apache.cassandra.metrics:type=DroppedMessage,scope=READ,name=Dropped,value=123.45");
  }

  @Test
  public void testBuilderWithMinimalFields() {
    // Given: A GenericMetric built with minimal fields
    GenericMetric metric =
        GenericMetric.builder().withClusterName("minimal-cluster").withValue(1.0).build();

    // When/Then: Set fields should be preserved, others should be null
    assertThat(metric.getClusterName()).isEqualTo("minimal-cluster");
    assertThat(metric.getValue()).isEqualTo(1.0);
    assertThat(metric.getHost()).isNull();
    assertThat(metric.getMetricDomain()).isNull();
    assertThat(metric.getMetricType()).isNull();
    assertThat(metric.getMetricScope()).isNull();
    assertThat(metric.getMetricName()).isNull();
    assertThat(metric.getMetricAttribute()).isNull();
    assertThat(metric.getTs()).isNull();
  }

  @Test
  public void testBuilderWithNullScope() {
    // Given: A GenericMetric built with null scope
    GenericMetric metric =
        GenericMetric.builder()
            .withMetricScope(null)
            .withMetricDomain("test.domain")
            .withMetricType("TestType")
            .withMetricName("TestName")
            .build();

    // When/Then: Null scope should be converted to space
    assertThat(metric.getMetricScope()).isEqualTo(" ");
    assertThat(metric.getMetricFullId())
        .isEqualTo("test.domain:type=TestType,scope= ,name=TestName");
  }

  @Test
  public void testBuilderWithEmptyStrings() {
    // Given: A GenericMetric built with empty strings
    GenericMetric metric =
        GenericMetric.builder()
            .withClusterName("")
            .withHost("")
            .withMetricDomain("")
            .withMetricType("")
            .withMetricScope("")
            .withMetricName("")
            .withMetricAttribute("")
            .withValue(0.0)
            .build();

    // When/Then: Empty strings should be preserved
    assertThat(metric.getClusterName()).isEqualTo("");
    assertThat(metric.getHost()).isEqualTo("");
    assertThat(metric.getMetricDomain()).isEqualTo("");
    assertThat(metric.getMetricType()).isEqualTo("");
    assertThat(metric.getMetricScope()).isEqualTo("");
    assertThat(metric.getMetricName()).isEqualTo("");
    assertThat(metric.getMetricAttribute()).isEqualTo("");
    assertThat(metric.getValue()).isEqualTo(0.0);
  }

  @Test
  public void testBuilderChaining() {
    // Given: Using builder method chaining
    GenericMetric metric =
        GenericMetric.builder()
            .withClusterName("chain-cluster")
            .withHost("192.168.1.100")
            .withMetricDomain("test.metrics")
            .withValue(99.9)
            .build();

    // When/Then: All chained values should be preserved
    assertThat(metric.getClusterName()).isEqualTo("chain-cluster");
    assertThat(metric.getHost()).isEqualTo("192.168.1.100");
    assertThat(metric.getMetricDomain()).isEqualTo("test.metrics");
    assertThat(metric.getValue()).isEqualTo(99.9);
  }

  @Test
  public void testBuilderWithNegativeValue() {
    // Given: A GenericMetric with negative value
    GenericMetric metric =
        GenericMetric.builder().withClusterName("negative-test").withValue(-42.5).build();

    // When/Then: Negative value should be preserved
    assertThat(metric.getClusterName()).isEqualTo("negative-test");
    assertThat(metric.getValue()).isEqualTo(-42.5);
  }

  @Test
  public void testBuilderWithZeroValue() {
    // Given: A GenericMetric with zero value
    GenericMetric metric =
        GenericMetric.builder().withClusterName("zero-test").withValue(0.0).build();

    // When/Then: Zero value should be preserved
    assertThat(metric.getClusterName()).isEqualTo("zero-test");
    assertThat(metric.getValue()).isEqualTo(0.0);
  }

  @Test
  public void testRealWorldScenario() {
    // Given: A realistic Cassandra thread pool metric
    DateTime timestamp = DateTime.parse("2023-06-15T10:30:00.000Z");
    GenericMetric metric =
        GenericMetric.builder()
            .withClusterName("production-cluster")
            .withHost("cassandra-node-01.example.com")
            .withMetricDomain("org.apache.cassandra.metrics")
            .withMetricType("ThreadPools")
            .withMetricScope("ReadStage")
            .withMetricName("ActiveTasks")
            .withMetricAttribute("Value")
            .withValue(15.0)
            .withTs(timestamp)
            .build();

    // When/Then: All realistic values should be preserved
    assertThat(metric.getClusterName()).isEqualTo("production-cluster");
    assertThat(metric.getHost()).isEqualTo("cassandra-node-01.example.com");
    assertThat(metric.getMetricType()).isEqualTo("ThreadPools");
    assertThat(metric.getMetricScope()).isEqualTo("ReadStage");
    assertThat(metric.getMetricName()).isEqualTo("ActiveTasks");
    assertThat(metric.getValue()).isEqualTo(15.0);
    assertThat(metric.getTs()).isEqualTo(timestamp);
    assertThat(metric.getMetricFullId())
        .isEqualTo(
            "org.apache.cassandra.metrics:type=ThreadPools,scope=ReadStage,name=ActiveTasks");
    assertThat(metric.toString())
        .isEqualTo(
            "org.apache.cassandra.metrics:type=ThreadPools,scope=ReadStage,name=ActiveTasks,value=15.0");
  }
}
