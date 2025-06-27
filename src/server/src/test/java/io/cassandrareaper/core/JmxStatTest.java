package io.cassandrareaper.core;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for JmxStat class. Tests the builder pattern, data access methods, and toString method
 * for JMX statistics representation.
 */
public class JmxStatTest {

  @Test
  public void testBuilderWithAllFields() {
    // Given: A JmxStat built with all fields
    JmxStat jmxStat =
        JmxStat.builder()
            .withMbeanName(
                "org.apache.cassandra.metrics:type=ThreadPools,path=request,scope=ReadStage,name=ActiveTasks")
            .withDomain("org.apache.cassandra.metrics")
            .withType("ThreadPools")
            .withScope("ReadStage")
            .withName("ActiveTasks")
            .withAttribute("Value")
            .withValue(5.0)
            .build();

    // When/Then: All getters should return the correct values
    assertThat(jmxStat.getMbeanName())
        .isEqualTo(
            "org.apache.cassandra.metrics:type=ThreadPools,path=request,scope=ReadStage,name=ActiveTasks");
    assertThat(jmxStat.getDomain()).isEqualTo("org.apache.cassandra.metrics");
    assertThat(jmxStat.getType()).isEqualTo("ThreadPools");
    assertThat(jmxStat.getScope()).isEqualTo("ReadStage");
    assertThat(jmxStat.getName()).isEqualTo("ActiveTasks");
    assertThat(jmxStat.getAttribute()).isEqualTo("Value");
    assertThat(jmxStat.getValue()).isEqualTo(5.0);
  }

  @Test
  public void testBuilderWithMinimalFields() {
    // Given: A JmxStat built with minimal fields
    JmxStat jmxStat =
        JmxStat.builder()
            .withMbeanName("test.domain:type=Test")
            .withAttribute("Count")
            .withValue(10.0)
            .build();

    // When/Then: Set fields should have values, unset fields should be null
    assertThat(jmxStat.getMbeanName()).isEqualTo("test.domain:type=Test");
    assertThat(jmxStat.getAttribute()).isEqualTo("Count");
    assertThat(jmxStat.getValue()).isEqualTo(10.0);
    assertThat(jmxStat.getDomain()).isNull();
    assertThat(jmxStat.getType()).isNull();
    assertThat(jmxStat.getScope()).isNull();
    assertThat(jmxStat.getName()).isNull();
  }

  @Test
  public void testBuilderWithNullValues() {
    // Given: A JmxStat built with null values
    JmxStat jmxStat =
        JmxStat.builder()
            .withMbeanName(null)
            .withDomain(null)
            .withType(null)
            .withScope(null)
            .withName(null)
            .withAttribute(null)
            .withValue(null)
            .build();

    // When/Then: All getters should return null
    assertThat(jmxStat.getMbeanName()).isNull();
    assertThat(jmxStat.getDomain()).isNull();
    assertThat(jmxStat.getType()).isNull();
    assertThat(jmxStat.getScope()).isNull();
    assertThat(jmxStat.getName()).isNull();
    assertThat(jmxStat.getAttribute()).isNull();
    assertThat(jmxStat.getValue()).isNull();
  }

  @Test
  public void testToStringWithAllFields() {
    // Given: A JmxStat with all fields set
    JmxStat jmxStat =
        JmxStat.builder()
            .withMbeanName(
                "org.apache.cassandra.metrics:type=DroppedMessage,scope=READ,name=Dropped")
            .withScope("READ")
            .withName("Dropped")
            .withAttribute("Count")
            .withValue(42.0)
            .build();

    // When: Converting to string
    String result = jmxStat.toString();

    // Then: Should follow the expected format: mbeanName/scope/name/attribute = value
    assertThat(result)
        .isEqualTo(
            "org.apache.cassandra.metrics:type=DroppedMessage,scope=READ,name=Dropped/READ/Dropped/Count = 42.0");
  }

  @Test
  public void testToStringWithNullFields() {
    // Given: A JmxStat with null fields
    JmxStat jmxStat =
        JmxStat.builder()
            .withMbeanName("test.bean")
            .withScope(null)
            .withName(null)
            .withAttribute("Value")
            .withValue(null)
            .build();

    // When: Converting to string
    String result = jmxStat.toString();

    // Then: Should handle null values gracefully
    assertThat(result).isEqualTo("test.bean/null/null/Value = null");
  }

  @Test
  public void testBuilderChaining() {
    // Given: Using builder method chaining
    JmxStat jmxStat =
        JmxStat.builder()
            .withMbeanName("chain.test:type=Test")
            .withDomain("chain.test")
            .withType("Test")
            .withScope("TestScope")
            .withName("TestName")
            .withAttribute("TestAttribute")
            .withValue(123.45)
            .build();

    // When/Then: All chained values should be preserved
    assertThat(jmxStat.getMbeanName()).isEqualTo("chain.test:type=Test");
    assertThat(jmxStat.getDomain()).isEqualTo("chain.test");
    assertThat(jmxStat.getType()).isEqualTo("Test");
    assertThat(jmxStat.getScope()).isEqualTo("TestScope");
    assertThat(jmxStat.getName()).isEqualTo("TestName");
    assertThat(jmxStat.getAttribute()).isEqualTo("TestAttribute");
    assertThat(jmxStat.getValue()).isEqualTo(123.45);
  }

  @Test
  public void testRealWorldCassandraMetrics() {
    // Given: Real-world Cassandra metrics examples
    JmxStat threadPoolStat =
        JmxStat.builder()
            .withMbeanName(
                "org.apache.cassandra.metrics:type=ThreadPools,path=request,scope=ReadStage,name=PendingTasks")
            .withDomain("org.apache.cassandra.metrics")
            .withType("ThreadPools")
            .withScope("ReadStage")
            .withName("PendingTasks")
            .withAttribute("Value")
            .withValue(0.0)
            .build();

    JmxStat droppedMessageStat =
        JmxStat.builder()
            .withMbeanName(
                "org.apache.cassandra.metrics:type=DroppedMessage,scope=MUTATION,name=Dropped")
            .withDomain("org.apache.cassandra.metrics")
            .withType("DroppedMessage")
            .withScope("MUTATION")
            .withName("Dropped")
            .withAttribute("Count")
            .withValue(15.0)
            .build();

    // When/Then: Should handle realistic Cassandra metric data correctly
    assertThat(threadPoolStat.getScope()).isEqualTo("ReadStage");
    assertThat(threadPoolStat.getName()).isEqualTo("PendingTasks");
    assertThat(threadPoolStat.getValue()).isEqualTo(0.0);

    assertThat(droppedMessageStat.getScope()).isEqualTo("MUTATION");
    assertThat(droppedMessageStat.getName()).isEqualTo("Dropped");
    assertThat(droppedMessageStat.getValue()).isEqualTo(15.0);
  }

  @Test
  public void testZeroAndNegativeValues() {
    // Given: JmxStat with zero and negative values
    JmxStat zeroStat =
        JmxStat.builder()
            .withMbeanName("test:type=Zero")
            .withAttribute("Value")
            .withValue(0.0)
            .build();

    JmxStat negativeStat =
        JmxStat.builder()
            .withMbeanName("test:type=Negative")
            .withAttribute("Value")
            .withValue(-42.5)
            .build();

    // When/Then: Should handle zero and negative values correctly
    assertThat(zeroStat.getValue()).isEqualTo(0.0);
    assertThat(negativeStat.getValue()).isEqualTo(-42.5);
    assertThat(zeroStat.toString()).contains("= 0.0");
    assertThat(negativeStat.toString()).contains("= -42.5");
  }

  @Test
  public void testEmptyStringFields() {
    // Given: A JmxStat with empty string fields
    JmxStat jmxStat =
        JmxStat.builder()
            .withMbeanName("")
            .withDomain("")
            .withType("")
            .withScope("")
            .withName("")
            .withAttribute("")
            .withValue(1.0)
            .build();

    // When/Then: Should handle empty strings correctly
    assertThat(jmxStat.getMbeanName()).isEqualTo("");
    assertThat(jmxStat.getDomain()).isEqualTo("");
    assertThat(jmxStat.getType()).isEqualTo("");
    assertThat(jmxStat.getScope()).isEqualTo("");
    assertThat(jmxStat.getName()).isEqualTo("");
    assertThat(jmxStat.getAttribute()).isEqualTo("");
    assertThat(jmxStat.toString()).isEqualTo("/// = 1.0");
  }
}
