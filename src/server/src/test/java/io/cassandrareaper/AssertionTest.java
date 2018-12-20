/*
 * Copyright 2017-2017 Spotify AB
 * Copyright 2017-2018 The Last Pickle Ltd
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

package io.cassandrareaper;

import java.nio.file.Paths;

import org.fest.assertions.api.Assertions;
import org.junit.Test;

public final class AssertionTest {

  @Test
  public void test_assertions_enabled() {
    boolean asserted = false;
    try {
      assert false;
    } catch (AssertionError error) {
      asserted = true;
    }
    if (!asserted) {
      throw new AssertionError("assertions are not enabled");
    }
  }

  @Test
  public void test_versions_downloaded() {
    String[] versions = System.getProperty("cucumber.versions-to-test").split("\n");
    for (String line : versions) {
      if (!(line.contains("Examples") || line.contains("version"))) {
        String version = line.replace("|", "").trim();
        if (!version.isEmpty()) {
          Assertions
            .assertThat(Paths.get("test-jars", String.format("cassandra-reaper-%s.jar", version)).toFile())
            .exists();
        }
      }
    }
  }

}
