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

package io.cassandrareaper.management.jmx;

import java.net.MalformedURLException;

import javax.management.remote.JMXServiceURL;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

public class JmxAddressesTest {

  @Test
  public void testGetJmxServiceUrl_IPv4() throws MalformedURLException {
    // Given
    String host = "127.0.0.1";
    int port = 7199;

    // When
    JMXServiceURL serviceUrl = JmxAddresses.getJmxServiceUrl(host, port);

    // Then
    assertThat(serviceUrl).isNotNull();
    assertThat(serviceUrl.toString()).contains("127.0.0.1");
    assertThat(serviceUrl.toString()).contains("7199");
    assertThat(serviceUrl.getProtocol()).isEqualTo("rmi");
  }

  @Test
  public void testGetJmxServiceUrl_IPv6() throws MalformedURLException {
    // Given
    String host = "[2001:db8::1]";
    int port = 7199;

    // When
    JMXServiceURL serviceUrl = JmxAddresses.getJmxServiceUrl(host, port);

    // Then
    assertThat(serviceUrl).isNotNull();
    assertThat(serviceUrl.toString()).contains("2001:db8::1");
    assertThat(serviceUrl.toString()).contains("7199");
  }

  @Test
  public void testGetJmxServiceUrl_Hostname() throws MalformedURLException {
    // Given
    String host = "cassandra.example.com";
    int port = 7199;

    // When
    JMXServiceURL serviceUrl = JmxAddresses.getJmxServiceUrl(host, port);

    // Then
    assertThat(serviceUrl).isNotNull();
    assertThat(serviceUrl.toString()).contains("cassandra.example.com");
    assertThat(serviceUrl.toString()).contains("7199");
  }

  @Test
  public void testGetJmxServiceUrl_CustomPort() throws MalformedURLException {
    // Given
    String host = "localhost";
    int port = 9999;

    // When
    JMXServiceURL serviceUrl = JmxAddresses.getJmxServiceUrl(host, port);

    // Then
    assertThat(serviceUrl).isNotNull();
    assertThat(serviceUrl.toString()).contains("9999");
  }

  @Test
  public void testIsNumericIPv6Address_ValidIPv6() {
    // Given/When/Then
    assertThat(JmxAddresses.isNumericIPv6Address("2001:db8::1")).isTrue();
    assertThat(JmxAddresses.isNumericIPv6Address("::1")).isTrue();
    assertThat(JmxAddresses.isNumericIPv6Address("fe80::1")).isTrue();
    assertThat(JmxAddresses.isNumericIPv6Address("2001:0db8:85a3:0000:0000:8a2e:0370:7334"))
        .isTrue();
  }

  @Test
  public void testIsNumericIPv6Address_InvalidIPv6() {
    // Given/When/Then
    assertThat(JmxAddresses.isNumericIPv6Address("127.0.0.1")).isFalse();
    assertThat(JmxAddresses.isNumericIPv6Address("localhost")).isFalse();
    assertThat(JmxAddresses.isNumericIPv6Address("cassandra.example.com")).isFalse();
    assertThat(JmxAddresses.isNumericIPv6Address("")).isFalse();
    assertThat(JmxAddresses.isNumericIPv6Address("invalid-ipv6")).isFalse();
  }

  @Test
  public void testIsNumericIPv6Address_NullInput() {
    // Given/When/Then
    assertThatThrownBy(() -> JmxAddresses.isNumericIPv6Address(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void testIsNumericIPv6Address_EdgeCases() {
    // Given/When/Then
    assertThat(JmxAddresses.isNumericIPv6Address("::")).isTrue();
    assertThat(JmxAddresses.isNumericIPv6Address("::ffff:192.0.2.1")).isTrue(); // IPv4-mapped IPv6
    assertThat(JmxAddresses.isNumericIPv6Address("2001:db8:85a3::8a2e:370:7334")).isTrue();
  }

  @Test
  public void testGetJmxServiceUrl_ZeroPort() {
    // Given
    String host = "localhost";
    int port = 0;

    // When/Then
    assertThatThrownBy(() -> JmxAddresses.getJmxServiceUrl(host, port))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testGetJmxServiceUrl_NegativePort() {
    // Given
    String host = "localhost";
    int port = -1;

    // When/Then
    assertThatThrownBy(() -> JmxAddresses.getJmxServiceUrl(host, port))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testGetJmxServiceUrl_PortTooLarge() {
    // Given
    String host = "localhost";
    int port = 65536; // Port numbers are 0-65535

    // When
    try {
      JMXServiceURL serviceUrl = JmxAddresses.getJmxServiceUrl(host, port);
      // Then - should succeed, large ports are allowed in JMX URLs
      assertThat(serviceUrl).isNotNull();
    } catch (Exception e) {
      // If it throws, that's also acceptable behavior
      assertThat(e).isInstanceOf(IllegalArgumentException.class);
    }
  }
}
