/*
 * Copyright 2016-2017 Spotify AB
 * Copyright 2016-2019 The Last Pickle Ltd
 * Copyright 2020-2020 DataStax, Inc.
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

package io.cassandrareaper.storage.operations;

import io.cassandrareaper.storage.OpType;

public interface IOperationsDao {

  void storeOperations(
      String clusterName, OpType operationType, String host, String operationsJson);

  String listOperations(String clusterName, OpType operationType, String host);

  /** Purges old node operation info from the database (no-op for databases w/ TTL) */
  void purgeNodeOperations();
}
