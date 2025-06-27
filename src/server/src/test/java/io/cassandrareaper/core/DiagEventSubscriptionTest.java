package io.cassandrareaper.core;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for DiagEventSubscription class. Tests cover constructor validation, getters,
 * equals/hashCode, and toString methods.
 */
public class DiagEventSubscriptionTest {

  private static final UUID TEST_ID = UUID.randomUUID();
  private static final String TEST_CLUSTER = "test-cluster";
  private static final String TEST_DESCRIPTION = "test description";
  private static final Set<String> TEST_NODES = Sets.newHashSet("node1", "node2");
  private static final Set<String> TEST_EVENTS = Sets.newHashSet("event1", "event2");
  private static final String TEST_FILE_LOGGER = "test-file-logger";
  private static final String TEST_HTTP_ENDPOINT = "http://test-endpoint";

  @Test
  public void testDefaultConstructor() {
    // Test default constructor required for Jackson JSON parsing
    DiagEventSubscription subscription = new DiagEventSubscription();

    assertThat(subscription.getId()).isNotPresent();
    assertThat(subscription.getCluster()).isNull();
    assertThat(subscription.getDescription()).isNull();
    assertThat(subscription.getNodes()).isNull();
    assertThat(subscription.getEvents()).isNull();
    assertThat(subscription.getExportSse()).isFalse();
    assertThat(subscription.getExportFileLogger()).isNull();
    assertThat(subscription.getExportHttpEndpoint()).isNull();
  }

  @Test
  public void testConstructorWithAllParameters() {
    DiagEventSubscription subscription =
        new DiagEventSubscription(
            Optional.of(TEST_ID),
            TEST_CLUSTER,
            Optional.of(TEST_DESCRIPTION),
            TEST_NODES,
            TEST_EVENTS,
            true,
            TEST_FILE_LOGGER,
            TEST_HTTP_ENDPOINT);

    assertThat(subscription.getId()).contains(TEST_ID);
    assertThat(subscription.getCluster()).isEqualTo(TEST_CLUSTER);
    assertThat(subscription.getDescription()).isEqualTo(TEST_DESCRIPTION);
    assertThat(subscription.getNodes()).isEqualTo(TEST_NODES);
    assertThat(subscription.getEvents()).isEqualTo(TEST_EVENTS);
    assertThat(subscription.getExportSse()).isTrue();
    assertThat(subscription.getExportFileLogger()).isEqualTo(TEST_FILE_LOGGER);
    assertThat(subscription.getExportHttpEndpoint()).isEqualTo(TEST_HTTP_ENDPOINT);
  }

  @Test
  public void testConstructorWithOptionalParameters() {
    DiagEventSubscription subscription =
        new DiagEventSubscription(
            Optional.empty(),
            TEST_CLUSTER,
            Optional.empty(),
            TEST_NODES,
            TEST_EVENTS,
            false,
            null,
            null);

    assertThat(subscription.getId()).isNotPresent();
    assertThat(subscription.getCluster()).isEqualTo(TEST_CLUSTER);
    assertThat(subscription.getDescription()).isNull();
    assertThat(subscription.getNodes()).isEqualTo(TEST_NODES);
    assertThat(subscription.getEvents()).isEqualTo(TEST_EVENTS);
    assertThat(subscription.getExportSse()).isFalse();
    assertThat(subscription.getExportFileLogger()).isNull();
    assertThat(subscription.getExportHttpEndpoint()).isNull();
  }

  @Test
  public void testConstructorValidatesNullCluster() {
    assertThatThrownBy(
            () ->
                new DiagEventSubscription(
                    Optional.empty(),
                    null,
                    Optional.empty(),
                    TEST_NODES,
                    TEST_EVENTS,
                    false,
                    null,
                    null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void testConstructorValidatesNullNodes() {
    assertThatThrownBy(
            () ->
                new DiagEventSubscription(
                    Optional.empty(),
                    TEST_CLUSTER,
                    Optional.empty(),
                    null,
                    TEST_EVENTS,
                    false,
                    null,
                    null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void testConstructorValidatesEmptyNodes() {
    assertThatThrownBy(
            () ->
                new DiagEventSubscription(
                    Optional.empty(),
                    TEST_CLUSTER,
                    Optional.empty(),
                    Collections.emptySet(),
                    TEST_EVENTS,
                    false,
                    null,
                    null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testConstructorValidatesNullEvents() {
    assertThatThrownBy(
            () ->
                new DiagEventSubscription(
                    Optional.empty(),
                    TEST_CLUSTER,
                    Optional.empty(),
                    TEST_NODES,
                    null,
                    false,
                    null,
                    null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void testConstructorValidatesEmptyEvents() {
    assertThatThrownBy(
            () ->
                new DiagEventSubscription(
                    Optional.empty(),
                    TEST_CLUSTER,
                    Optional.empty(),
                    TEST_NODES,
                    Collections.emptySet(),
                    false,
                    null,
                    null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testWithId() {
    DiagEventSubscription original =
        new DiagEventSubscription(
            Optional.empty(),
            TEST_CLUSTER,
            Optional.of(TEST_DESCRIPTION),
            TEST_NODES,
            TEST_EVENTS,
            true,
            TEST_FILE_LOGGER,
            TEST_HTTP_ENDPOINT);

    DiagEventSubscription withId = original.withId(TEST_ID);

    assertThat(withId.getId()).contains(TEST_ID);
    assertThat(withId.getCluster()).isEqualTo(TEST_CLUSTER);
    assertThat(withId.getDescription()).isEqualTo(TEST_DESCRIPTION);
    assertThat(withId.getNodes()).isEqualTo(TEST_NODES);
    assertThat(withId.getEvents()).isEqualTo(TEST_EVENTS);
    assertThat(withId.getExportSse()).isTrue();
    assertThat(withId.getExportFileLogger()).isEqualTo(TEST_FILE_LOGGER);
    assertThat(withId.getExportHttpEndpoint()).isEqualTo(TEST_HTTP_ENDPOINT);
  }

  @Test
  public void testEqualsAndHashCode() {
    UUID id1 = UUID.randomUUID();
    UUID id2 = UUID.randomUUID();

    DiagEventSubscription subscription1 =
        new DiagEventSubscription(
            Optional.of(id1),
            TEST_CLUSTER,
            Optional.of(TEST_DESCRIPTION),
            TEST_NODES,
            TEST_EVENTS,
            true,
            TEST_FILE_LOGGER,
            TEST_HTTP_ENDPOINT);

    DiagEventSubscription subscription2 =
        new DiagEventSubscription(
            Optional.of(id1),
            "different-cluster",
            Optional.of("different description"),
            Sets.newHashSet("different-node"),
            Sets.newHashSet("different-event"),
            false,
            "different-logger",
            "different-endpoint");

    DiagEventSubscription subscription3 =
        new DiagEventSubscription(
            Optional.of(id2),
            TEST_CLUSTER,
            Optional.of(TEST_DESCRIPTION),
            TEST_NODES,
            TEST_EVENTS,
            true,
            TEST_FILE_LOGGER,
            TEST_HTTP_ENDPOINT);

    // Same ID should be equal
    assertThat(subscription1).isEqualTo(subscription2);
    assertThat(subscription1.hashCode()).isEqualTo(subscription2.hashCode());

    // Different ID should not be equal
    assertThat(subscription1).isNotEqualTo(subscription3);

    // Reflexive
    assertThat(subscription1).isEqualTo(subscription1);

    // Null comparison
    assertThat(subscription1).isNotEqualTo(null);

    // Different class comparison
    assertThat(subscription1).isNotEqualTo("not a subscription");
  }

  @Test
  public void testEqualsWithNullIds() {
    DiagEventSubscription subscription1 =
        new DiagEventSubscription(
            Optional.empty(),
            TEST_CLUSTER,
            Optional.empty(),
            TEST_NODES,
            TEST_EVENTS,
            false,
            null,
            null);

    DiagEventSubscription subscription2 =
        new DiagEventSubscription(
            Optional.empty(),
            "different-cluster",
            Optional.empty(),
            Sets.newHashSet("different-node"),
            Sets.newHashSet("different-event"),
            true,
            "different-logger",
            "different-endpoint");

    // Both have null IDs, should be equal
    assertThat(subscription1).isEqualTo(subscription2);
    assertThat(subscription1.hashCode()).isEqualTo(subscription2.hashCode());
  }

  @Test
  public void testToString() {
    DiagEventSubscription subscription =
        new DiagEventSubscription(
            Optional.of(TEST_ID),
            TEST_CLUSTER,
            Optional.of(TEST_DESCRIPTION),
            TEST_NODES,
            TEST_EVENTS,
            true,
            TEST_FILE_LOGGER,
            TEST_HTTP_ENDPOINT);

    String toString = subscription.toString();

    assertThat(toString).contains("DiagEventSubscription{");
    assertThat(toString).contains("id=" + TEST_ID);
    assertThat(toString).contains("cluster='" + TEST_CLUSTER + "'");
    assertThat(toString).contains("description='" + TEST_DESCRIPTION + "'");
    assertThat(toString).contains("nodes=" + TEST_NODES);
    assertThat(toString).contains("events=" + TEST_EVENTS);
    assertThat(toString).contains("exportSse=true");
    assertThat(toString).contains("exportFileLogger='" + TEST_FILE_LOGGER + "'");
    assertThat(toString).contains("exportHttpEndpoint='" + TEST_HTTP_ENDPOINT + "'");
  }

  @Test
  public void testToStringWithNullValues() {
    DiagEventSubscription subscription =
        new DiagEventSubscription(
            Optional.empty(),
            TEST_CLUSTER,
            Optional.empty(),
            TEST_NODES,
            TEST_EVENTS,
            false,
            null,
            null);

    String toString = subscription.toString();

    assertThat(toString).contains("DiagEventSubscription{");
    assertThat(toString).contains("id=null");
    assertThat(toString).contains("description='null'");
    assertThat(toString).contains("exportSse=false");
    assertThat(toString).contains("exportFileLogger='null'");
    assertThat(toString).contains("exportHttpEndpoint='null'");
  }
}
