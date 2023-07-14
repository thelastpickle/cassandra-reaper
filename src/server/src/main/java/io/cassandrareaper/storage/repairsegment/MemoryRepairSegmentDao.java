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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class MemoryRepairSegmentDao implements IRepairSegmentDao {

  public final ConcurrentMap<UUID, LinkedHashMap<UUID, RepairSegment>> repairSegmentsByRunId = Maps.newConcurrentMap();
  private final MemoryStorageFacade memoryStorageFacade;
  private final ConcurrentMap<UUID, RepairSegment> repairSegments = Maps.newConcurrentMap();

  public MemoryRepairSegmentDao(MemoryStorageFacade memoryStorageFacade) {
    this.memoryStorageFacade = memoryStorageFacade;
  }

  public int deleteRepairSegmentsForRun(UUID runId) {
    Map<UUID, RepairSegment> segmentsMap = repairSegmentsByRunId.remove(runId);
    if (null != segmentsMap) {
      for (RepairSegment segment : segmentsMap.values()) {
        this.repairSegments.remove(segment.getId());
      }
    }
    return segmentsMap != null ? segmentsMap.size() : 0;
  }

  public void addRepairSegments(Collection<RepairSegment.Builder> segments, UUID runId) {
    LinkedHashMap<UUID, RepairSegment> newSegments = Maps.newLinkedHashMap();
    for (RepairSegment.Builder segment : segments) {
      RepairSegment newRepairSegment = segment.withRunId(runId).withId(UUIDs.timeBased()).build();
      this.repairSegments.put(newRepairSegment.getId(), newRepairSegment);
      newSegments.put(newRepairSegment.getId(), newRepairSegment);
    }
    repairSegmentsByRunId.put(runId, newSegments);

  }

  @Override
  public boolean updateRepairSegment(RepairSegment newRepairSegment) {
    if (memoryStorageFacade.getRepairSegmentDao().getRepairSegment(newRepairSegment.getRunId(),
        newRepairSegment.getId()) == null) {
      return false;
    } else {
      this.repairSegments.put(newRepairSegment.getId(), newRepairSegment);
      LinkedHashMap<UUID, RepairSegment> updatedSegment = repairSegmentsByRunId.get(newRepairSegment.getRunId());
      updatedSegment.put(newRepairSegment.getId(), newRepairSegment);
      return true;
    }
  }

  @Override
  public Optional<RepairSegment> getRepairSegment(UUID runId, UUID segmentId) {
    return Optional.ofNullable(repairSegments.get(segmentId));
  }

  @Override
  public Collection<RepairSegment> getRepairSegmentsForRun(UUID runId) {
    return repairSegmentsByRunId.get(runId).values();
  }

  @Override
  public List<RepairSegment> getNextFreeSegments(UUID runId) {
    return repairSegmentsByRunId.get(runId).values().stream()
        .filter(seg -> seg.getState() == RepairSegment.State.NOT_STARTED)
        .collect(Collectors.toList());
  }

  @Override
  public Collection<RepairSegment> getSegmentsWithState(UUID runId, RepairSegment.State segmentState) {
    List<RepairSegment> segments = Lists.newArrayList();
    for (RepairSegment segment : repairSegmentsByRunId.get(runId).values()) {
      if (segment.getState() == segmentState) {
        segments.add(segment);
      }
    }
    return segments;
  }

  @Override
  public int getSegmentAmountForRepairRun(UUID runId) {
    Map<UUID, RepairSegment> segmentsMap = repairSegmentsByRunId.get(runId);
    return segmentsMap == null ? 0 : segmentsMap.size();
  }

  @Override
  public int getSegmentAmountForRepairRunWithState(UUID runId, RepairSegment.State state) {
    Map<UUID, RepairSegment> segmentsMap = repairSegmentsByRunId.get(runId);
    int amount = 0;
    if (null != segmentsMap) {
      for (RepairSegment segment : segmentsMap.values()) {
        if (segment.getState() == state) {
          amount += 1;
        }
      }
    }
    return amount;
  }
}