/*
 *
 * Copyright 2019-2019 The Last Pickle Ltd
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

package io.cassandrareaper.acceptance;

import io.cassandrareaper.acceptance.ReaperTestJettyRunner.ReaperJettyTestSupport;

import java.util.Optional;

import org.assertj.core.api.Assertions;
import org.junit.Test;

public final class ReaperJettyTestSupportTest {

  @Test
  public void test_versions_applicationClass() throws Exception {
    String[] versions = System.getProperty("cucumber.versions-to-test.original").split("\n");
    for (String line : versions) {
      if (!(line.contains("Examples") || line.contains("version"))) {
        String version = line.replace("|", "").trim();
        if (!version.isEmpty()) {
          Assertions.assertThat(
              ReaperJettyTestSupport.getApplicationClass(Optional.of(version)))
            .isNotNull();
        }
      }
    }
  }

  @Test
  public void test_versions_reaper() throws Exception {
    String[] versions = System.getProperty("cucumber.versions-to-test.original").split("\n");
    for (String line : versions) {
      if (!(line.contains("Examples") || line.contains("version"))) {
        String version = line.replace("|", "").trim();
        if (!version.isEmpty()) {
          new ReaperJettyTestSupport(
              "classpath:io.cassandrareaper.acceptance/integration_reaper_functionality.feature",
              Optional.of(version));
        }
      }
    }
  }
}
