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

package io.cassandrareaper.storage.repairsegment;

import io.cassandrareaper.core.RepairSegment;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface IRepairSegmentDao {
  boolean updateRepairSegment(RepairSegment newRepairSegment);

  /**
   * Update the repair segment without a lock as it couldn't be grabbed.
   *
   * @param newRepairSegment repair segment to update
   * @return true if the segment was updated, false otherwise
   */
  boolean updateRepairSegmentUnsafe(RepairSegment newRepairSegment);

  Optional<RepairSegment> getRepairSegment(UUID runId, UUID segmentId);

  Collection<RepairSegment> getRepairSegmentsForRun(UUID runId);

  /**
   * @param runId the run id that the segment belongs to.
   * @return a segment enclosed by the range with state NOT_STARTED, or nothing.
   */
  List<RepairSegment> getNextFreeSegments(UUID runId);

  Collection<RepairSegment> getSegmentsWithState(UUID runId, RepairSegment.State segmentState);

  int getSegmentAmountForRepairRun(UUID runId);

  int getSegmentAmountForRepairRunWithState(UUID runId, RepairSegment.State state);
}
