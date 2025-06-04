/*
 * Copyright 2015-2017 Spotify AB
 * Copyright 2016-2018 The Last Pickle Ltd
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

package io.cassandrareaper.service;

import io.cassandrareaper.core.Segment;

import java.util.Set;

import org.apache.cassandra.repair.RepairParallelism;

/** Represents the parameters of a repair command given to Cassandra. */
public final class RepairParameters {

  public final Segment tokenRange;
  public final String keyspaceName;
  public final Set<String> columnFamilies;
  public final RepairParallelism repairParallelism;

  public RepairParameters(
      Segment tokenRange,
      String keyspaceName,
      Set<String> columnFamilies,
      RepairParallelism repairParallelism) {

    this.tokenRange = tokenRange;
    this.keyspaceName = keyspaceName;
    this.columnFamilies = columnFamilies;
    this.repairParallelism = repairParallelism;
  }
}
