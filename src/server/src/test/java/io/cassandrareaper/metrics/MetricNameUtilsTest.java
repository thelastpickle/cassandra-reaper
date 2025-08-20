package io.cassandrareaper.metrics;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for MetricNameUtils utility class. Tests cover name cleaning, UUID conversion, and
 * hostname cleaning methods. Follows Clean Code principles with descriptive test names and clear
 * assertions.
 */
public class MetricNameUtilsTest {

  @Test
  public void testConstructorThrowsIllegalStateException() throws Exception {
    // When/Then: Constructor should throw IllegalStateException for utility class
    assertThatThrownBy(
            () -> {
              try {
                var constructor = MetricNameUtils.class.getDeclaredConstructor();
                constructor.setAccessible(true);
                constructor.newInstance();
              } catch (Exception e) {
                if (e.getCause() instanceof IllegalStateException) {
                  throw (IllegalStateException) e.getCause();
                }
                throw new RuntimeException(e);
              }
            })
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Utility class");
  }

  @Test
  public void testCleanNameRemovesSpecialCharacters() {
    // Given: Name with special characters
    String dirtyName = "test@name#with$special%characters";

    // When: Cleaning the name
    String cleanedName = MetricNameUtils.cleanName(dirtyName);

    // Then: Only alphanumeric, hyphens, and underscores should remain
    assertThat(cleanedName).isEqualTo("testnamewithspecialcharacters");
  }

  @Test
  public void testCleanNamePreservesAllowedCharacters() {
    // Given: Name with only allowed characters
    String allowedName = "test-name_with123";

    // When: Cleaning the name
    String cleanedName = MetricNameUtils.cleanName(allowedName);

    // Then: Name should remain unchanged
    assertThat(cleanedName).isEqualTo(allowedName);
  }

  @Test
  public void testCleanNameWithEmptyString() {
    // Given: Empty string
    String emptyName = "";

    // When: Cleaning the name
    String cleanedName = MetricNameUtils.cleanName(emptyName);

    // Then: Should return empty string
    assertThat(cleanedName).isEmpty();
  }

  @Test
  public void testCleanNameWithOnlySpecialCharacters() {
    // Given: String with only special characters
    String specialCharsOnly = "@#$%^&*()+=[]{}|\\:;\"'<>?,./ ";

    // When: Cleaning the name
    String cleanedName = MetricNameUtils.cleanName(specialCharsOnly);

    // Then: Should return empty string
    assertThat(cleanedName).isEmpty();
  }

  @Test
  public void testCleanNameWithMixedContent() {
    // Given: Name with mixed allowed and disallowed characters
    String mixedName = "cluster-1@datacenter_2#keyspace.table";

    // When: Cleaning the name
    String cleanedName = MetricNameUtils.cleanName(mixedName);

    // Then: Should keep only allowed characters
    assertThat(cleanedName).isEqualTo("cluster-1datacenter_2keyspacetable");
  }

  @Test
  public void testCleanIdConvertsUuidToString() {
    // Given: A UUID
    UUID testUuid = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");

    // When: Converting UUID to clean ID
    String cleanId = MetricNameUtils.cleanId(testUuid);

    // Then: Should return UUID string representation
    assertThat(cleanId).isEqualTo("550e8400-e29b-41d4-a716-446655440000");
  }

  @Test
  public void testCleanIdWithRandomUuid() {
    // Given: A random UUID
    UUID randomUuid = UUID.randomUUID();

    // When: Converting UUID to clean ID
    String cleanId = MetricNameUtils.cleanId(randomUuid);

    // Then: Should return UUID string representation
    assertThat(cleanId).isEqualTo(randomUuid.toString());
  }

  @Test
  public void testCleanHostNameWithNormalHostname() {
    // Given: Normal hostname
    String hostname = "cassandra.example.com";

    // When: Cleaning the hostname
    String cleanedHostname = MetricNameUtils.cleanHostName(hostname);

    // Then: Dots should be replaced with hyphens
    assertThat(cleanedHostname).isEqualTo("cassandra-example-com");
  }

  @Test
  public void testCleanHostNameWithSpecialCharacters() {
    // Given: Hostname with special characters
    String hostname = "host@name#with$special%.chars";

    // When: Cleaning the hostname
    String cleanedHostname = MetricNameUtils.cleanHostName(hostname);

    // Then: Dots replaced with hyphens, special chars removed
    assertThat(cleanedHostname).isEqualTo("hostnamewithspecial-chars");
  }

  @Test
  public void testCleanHostNameWithNullInput() {
    // Given: Null hostname
    String hostname = null;

    // When: Cleaning the hostname
    String cleanedHostname = MetricNameUtils.cleanHostName(hostname);

    // Then: Should return "null"
    assertThat(cleanedHostname).isEqualTo("null");
  }

  @Test
  public void testCleanHostNameWithEmptyString() {
    // Given: Empty hostname
    String hostname = "";

    // When: Cleaning the hostname
    String cleanedHostname = MetricNameUtils.cleanHostName(hostname);

    // Then: Should return empty string
    assertThat(cleanedHostname).isEmpty();
  }

  @Test
  public void testCleanHostNameWithIPAddress() {
    // Given: IP address as hostname
    String hostname = "192.168.1.100";

    // When: Cleaning the hostname
    String cleanedHostname = MetricNameUtils.cleanHostName(hostname);

    // Then: Dots should be replaced with hyphens
    assertThat(cleanedHostname).isEqualTo("192-168-1-100");
  }

  @Test
  public void testCleanHostNameWithIPv6Address() {
    // Given: IPv6 address as hostname
    String hostname = "2001:0db8:85a3:0000:0000:8a2e:0370:7334";

    // When: Cleaning the hostname
    String cleanedHostname = MetricNameUtils.cleanHostName(hostname);

    // Then: Colons should be removed (not allowed chars)
    assertThat(cleanedHostname).isEqualTo("20010db885a3000000008a2e03707334");
  }

  @Test
  public void testCleanHostNameWithAllowedCharacters() {
    // Given: Hostname with only allowed characters
    String hostname = "host-name_123";

    // When: Cleaning the hostname
    String cleanedHostname = MetricNameUtils.cleanHostName(hostname);

    // Then: Should remain unchanged
    assertThat(cleanedHostname).isEqualTo(hostname);
  }

  @Test
  public void testCleanHostNameWithOnlySpecialCharacters() {
    // Given: Hostname with only special characters and dots
    String hostname = "@#$%^&*().+=";

    // When: Cleaning the hostname
    String cleanedHostname = MetricNameUtils.cleanHostName(hostname);

    // Then: Should return only hyphens where dots were
    assertThat(cleanedHostname).isEqualTo("-");
  }
}
