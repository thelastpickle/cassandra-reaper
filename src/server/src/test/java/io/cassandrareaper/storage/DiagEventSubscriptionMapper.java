/*
 * Copyright 2018-2018 Stefan Podkowinski
 * Copyright 2019-2019 The Last Pickle Ltd
 * Copyright 2021-2021 DataStax, Inc.
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

package io.cassandrareaper.storage;

import io.cassandrareaper.core.DiagEventSubscription;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.ImmutableSet;

public final class DiagEventSubscriptionMapper {

  private Set<String> parseStringArray(Object obj) {
    Set<String> values = Collections.emptySet();
    if (obj instanceof String[]) {
      values = ImmutableSet.copyOf((String[]) obj);
    } else if (obj instanceof Object[]) {
      Object[] ocf = (Object[]) obj;
      values = ImmutableSet.copyOf(Arrays.copyOf(ocf, ocf.length, String[].class));
    }
    return values;
  }

  public static DiagEventSubscription fromParamMap(Map<String, String> map) {

    UUID id = null;
    String sid = map.get("id");
    if (sid != null) {
      id = UuidUtil.fromSequenceId(Long.valueOf(sid));
    }

    Set<String> nodes = Collections.emptySet();
    if (map.containsKey("nodes")) {
      nodes = ImmutableSet.copyOf(map.get("nodes").split(","));
    }

    Set<String> events = Collections.emptySet();
    if (map.containsKey("events")) {
      events = ImmutableSet.copyOf(map.get("events").split(","));
    }

    boolean exportSse = false;
    if (map.containsKey("exportSse")) {
      exportSse = Boolean.valueOf(map.get("exportSse"));
    }

    String clusterName = map.get("clusterName");
    String description = map.get("description");
    String exportFileLogger = map.get("exportFileLogger");
    String exportHttpEndpoint = map.get("exportHttpEndpoint");

    return new DiagEventSubscription(
        Optional.ofNullable(id),
        clusterName,
        Optional.ofNullable(description),
        nodes,
        events,
        exportSse,
        exportFileLogger,
        exportHttpEndpoint);
  }
}
