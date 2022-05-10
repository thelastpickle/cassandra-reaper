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

package io.cassandrareaper.metrics;

import java.util.Optional;
import java.util.UUID;

public final class MetricNameUtils {

  private MetricNameUtils() {
    throw new IllegalStateException("Utility class");
  }

  public static String cleanName(String name) {
    return name.replaceAll("[^A-Za-z0-9]", "");
  }

  public static String cleanId(UUID uuid) {
    return uuid.toString().replaceAll("-", "");
  }

  public static String cleanHostName(String hostname) {
    return Optional.ofNullable(hostname).orElse("null")
        .replace('.', 'x')
        .replaceAll("[^A-Za-z0-9]", "");
  }
}
