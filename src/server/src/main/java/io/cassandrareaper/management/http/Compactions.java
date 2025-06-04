/*
 * Copyright 2020-2020 The Last Pickle Ltd
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

import java.util.Map;

import com.datastax.mgmtapi.client.model.Compaction;
import com.google.common.collect.ImmutableMap;

/** Helper methods to manipulate {@link com.datastax.mgmtapi.client.model.Compaction} instances. */
final class Compactions {

  private Compactions() {
    // intentionally empty
  }

  static Map<String, String> asMap(Compaction compaction) {
    ImmutableMap.Builder<String, String> map = ImmutableMap.builder();
    putIfNotNull(map, Compaction.SERIALIZED_NAME_COLUMNFAMILY, compaction.getColumnfamily());
    putIfNotNull(map, Compaction.SERIALIZED_NAME_COMPACTION_ID, compaction.getCompactionId());
    putIfNotNull(map, Compaction.SERIALIZED_NAME_COMPLETED, compaction.getCompleted());
    putIfNotNull(map, Compaction.SERIALIZED_NAME_DESCRIPTION, compaction.getDescription());
    putIfNotNull(map, Compaction.SERIALIZED_NAME_ID, compaction.getId());
    putIfNotNull(map, Compaction.SERIALIZED_NAME_KEYSPACE, compaction.getKeyspace());
    putIfNotNull(map, Compaction.SERIALIZED_NAME_OPERATION_ID, compaction.getOperationId());
    putIfNotNull(map, Compaction.SERIALIZED_NAME_OPERATION_TYPE, compaction.getOperationType());
    putIfNotNull(map, Compaction.SERIALIZED_NAME_SSTABLES, compaction.getSstables());
    putIfNotNull(map, Compaction.SERIALIZED_NAME_TARGET_DIRECTORY, compaction.getTargetDirectory());
    putIfNotNull(map, Compaction.SERIALIZED_NAME_TASK_TYPE, compaction.getTaskType());
    putIfNotNull(map, Compaction.SERIALIZED_NAME_TOTAL, compaction.getTotal());
    putIfNotNull(map, Compaction.SERIALIZED_NAME_UNIT, compaction.getUnit());
    return map.build();
  }

  private static void putIfNotNull(
      ImmutableMap.Builder<String, String> map, String key, Object value) {
    if (value != null) {
      map.put(key, value.toString());
    }
  }
}
