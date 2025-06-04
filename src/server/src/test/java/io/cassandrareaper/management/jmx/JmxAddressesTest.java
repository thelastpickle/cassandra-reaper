/*
 * Copyright 2018-2018 The Last Pickle Ltd
 *
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

import static io.cassandrareaper.management.jmx.JmxAddresses.getJmxServiceUrl;
import static io.cassandrareaper.management.jmx.JmxAddresses.isNumericIPv6Address;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class JmxAddressesTest {
  @Test
  public void isNumericIPv6AddressTest() throws Exception {
    // numeric IPv6 addresses
    assertTrue(isNumericIPv6Address("::1"));
    assertTrue(isNumericIPv6Address("2a06:6b8:b010:d007::1:c8"));
    assertTrue(isNumericIPv6Address("[2a06:6b8:b010:d007::1:c8]"));

    // numeric IPv4 addresses
    assertFalse(isNumericIPv6Address("127.0.0.1"));
    assertFalse(isNumericIPv6Address("1.2.3.4"));

    // domain names
    assertFalse(isNumericIPv6Address("localhost"));
    assertFalse(isNumericIPv6Address("example.com"));

    // corner case
    assertFalse(isNumericIPv6Address(""));
  }

  @Test
  public void getJmxServiceUrlIPv6NumericTest() throws Exception {
    int port = 8888;
    assertEquals(jmxUrlPath("[::1]", port), getJmxServiceUrl("::1", port).getURLPath());
    assertEquals(jmxUrlPath("[::1]", port), getJmxServiceUrl("[::1]", port).getURLPath());
    assertEquals(jmxUrlPath("127.0.0.1", port), getJmxServiceUrl("127.0.0.1", port).getURLPath());
    assertEquals(
        jmxUrlPath("[2a06:6b8:b010:d007::1:c8]", port),
        getJmxServiceUrl("2a06:6b8:b010:d007::1:c8", port).getURLPath());
    assertEquals(jmxUrlPath("localhost", port), getJmxServiceUrl("localhost", port).getURLPath());
    assertEquals(
        jmxUrlPath("example.com", port), getJmxServiceUrl("example.com", port).getURLPath());
  }

  private String jmxUrlPath(String host, int port) {
    return String.format("/jndi/rmi://%s:%d/jmxrmi", host, port);
  }
}
