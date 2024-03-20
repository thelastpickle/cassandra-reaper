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
import io.cassandrareaper.storage.MemoryStorageFacade;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.Lists;

public class MemoryRepairSegmentDao implements IRepairSegmentDao {

  private final MemoryStorageFacade memoryStorageFacade;

  public MemoryRepairSegmentDao(MemoryStorageFacade memoryStorageFacade) {
    this.memoryStorageFacade = memoryStorageFacade;
  }

  public int deleteRepairSegmentsForRun(UUID runId) {
    Collection<RepairSegment> segments = memoryStorageFacade.getRepairSegmentsByRunId(runId);
    if (null != segments) {
      for (RepairSegment segment : segments) {
        memoryStorageFacade.removeRepairSegment(segment.getId());
      }
    }
    return segments != null ? segments.size() : 0;
  }

  public void addRepairSegments(Collection<RepairSegment.Builder> segments, UUID runId) {
    for (RepairSegment.Builder segment : segments) {
      RepairSegment newRepairSegment = segment.withRunId(runId).withId(UUIDs.timeBased()).build();
      memoryStorageFacade.addRepairSegment(newRepairSegment);
    }
  }

  @Override
  public boolean updateRepairSegment(RepairSegment newRepairSegment) {
    if (memoryStorageFacade.getRepairSegmentDao().getRepairSegment(newRepairSegment.getRunId(),
        newRepairSegment.getId()) == null) {
      return false;
    } else {
      memoryStorageFacade.addRepairSegment(newRepairSegment);
      return true;
    }
  }

  @Override
  public boolean updateRepairSegmentUnsafe(RepairSegment newRepairSegment) {
    return updateRepairSegment(newRepairSegment);
  }


  @Override
  public Optional<RepairSegment> getRepairSegment(UUID runId, UUID segmentId) {
    return Optional.ofNullable(memoryStorageFacade.getRepairSegmentById(runId));
  }

  @Override
  public Collection<RepairSegment> getRepairSegmentsForRun(UUID runId) {
    return memoryStorageFacade.getRepairSegmentsByRunId(runId);
  }

  @Override
  public List<RepairSegment> getNextFreeSegments(UUID runId) {
    return memoryStorageFacade.getRepairSegmentsByRunId(runId).stream()
        .filter(seg -> seg.getState() == RepairSegment.State.NOT_STARTED)
        .collect(Collectors.toList());
  }

  @Override
  public Collection<RepairSegment> getSegmentsWithState(UUID runId, RepairSegment.State segmentState) {
    List<RepairSegment> segments = Lists.newArrayList();
    for (RepairSegment segment : memoryStorageFacade.getRepairSegmentsByRunId(runId)) {
      if (segment.getState() == segmentState) {
        segments.add(segment);
      }
    }
    return segments;
  }

  @Override
  public int getSegmentAmountForRepairRun(UUID runId) {
    Collection<RepairSegment> segments = memoryStorageFacade.getRepairSegmentsByRunId(runId);
    return segments == null ? 0 : segments.size();
  }

  @Override
  public int getSegmentAmountForRepairRunWithState(UUID runId, RepairSegment.State state) {
    Collection<RepairSegment> segments = memoryStorageFacade.getRepairSegmentsByRunId(runId);
    int amount = 0;
    if (null != segments) {
      for (RepairSegment segment : segments) {
        if (segment.getState() == state) {
          amount += 1;
        }
      }
    }
    return amount;
  }
}