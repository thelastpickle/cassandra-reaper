/*
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
package com.spotify.reaper.repair;

import com.spotify.reaper.repair.segment.RingRange;
import org.apache.cassandra.repair.RepairParallelism;

import java.util.Set;

/**
 * Represents the parameters of a repair command given to Cassandra.
 */
public class RepairParameters {
  public final RingRange tokenRange;
  public final String keyspaceName;
  public final Set<String> columnFamilies;
  public final RepairParallelism repairParallelism;

  public RepairParameters(RingRange tokenRange, String keyspaceName, Set<String> columnFamilies,
      RepairParallelism repairParallelism) {
    this.tokenRange = tokenRange;
    this.keyspaceName = keyspaceName;
    this.columnFamilies = columnFamilies;
    this.repairParallelism = repairParallelism;
  }
}
