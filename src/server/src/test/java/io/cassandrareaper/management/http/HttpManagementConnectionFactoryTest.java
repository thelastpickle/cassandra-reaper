/*
 * Copyright 2026-2026 DataStax, Inc.
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

package io.cassandrareaper.management.http;

import static org.junit.Assert.assertEquals;

import okhttp3.HttpUrl;
import org.junit.Test;

public class HttpManagementConnectionFactoryTest {

  @Test
  public void testBasePathForIpv4Host() {
    assertEquals(
        "http://10.0.0.1:8080",
        HttpManagementConnectionFactory.managementApiBasePath("http", "10.0.0.1", 8080, ""));
  }

  @Test
  public void testBasePathForHostname() {
    assertEquals(
        "https://cassandra-0.example:8080",
        HttpManagementConnectionFactory.managementApiBasePath(
            "https", "cassandra-0.example", 8080, ""));
  }

  @Test
  public void testBasePathBracketsIpv6Host() {
    String basePath =
        HttpManagementConnectionFactory.managementApiBasePath(
            "http", "2001:db8:0:0:0:0:0:5", 8080, "");
    assertEquals("http://[2001:db8:0:0:0:0:0:5]:8080", basePath);
    HttpUrl url = HttpUrl.get(basePath);
    assertEquals("2001:db8::5", url.host());
    assertEquals(8080, url.port());
  }

  @Test
  public void testBasePathKeepsAlreadyBracketedIpv6Host() {
    assertEquals(
        "http://[::1]:8080",
        HttpManagementConnectionFactory.managementApiBasePath("http", "[::1]", 8080, ""));
  }
}
