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

package io.cassandrareaper.storage.postgresql;

import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.RepairSegment;
import io.cassandrareaper.core.RepairSegment.State;
import io.cassandrareaper.core.Segment;
import io.cassandrareaper.storage.JsonParseUtils;

import java.math.BigInteger;
import java.util.UUID;

import org.joda.time.DateTime;

public class PostgresRepairSegment {
  private final UUID id;
  private final UUID runId;
  private final UUID repairUnitId;
  private final Segment tokenRange;
  private final int failCount;
  private final State state;
  private final String coordinatorHost;
  private final DateTime startTime;
  private final DateTime endTime;
  private final String tokenRangesTxt;

  public PostgresRepairSegment(RepairSegment original) throws ReaperException {
    runId = original.getRunId();
    id = original.getId();
    repairUnitId = original.getRepairUnitId();
    tokenRange = original.getTokenRange();
    failCount = original.getFailCount();
    state = original.getState();
    coordinatorHost = original.getCoordinatorHost();
    startTime = original.getStartTime();
    endTime = original.getEndTime();
    tokenRangesTxt = JsonParseUtils.writeTokenRangesTxt(original.getTokenRange().getTokenRanges());
  }

  public UUID getId() {
    return id;
  }

  public UUID getRunId() {
    return runId;
  }

  public UUID getRepairUnitId() {
    return repairUnitId;
  }

  public Segment getTokenRange() {
    return tokenRange;
  }

  public int getFailCount() {
    return failCount;
  }

  public State getState() {
    return state;
  }

  public String getCoordinatorHost() {
    return coordinatorHost;
  }

  public DateTime getStartTime() {
    return startTime;
  }

  public DateTime getEndTime() {
    return endTime;
  }

  public String getTokenRangesTxt() {
    return tokenRangesTxt;
  }

  public BigInteger getStartToken() {
    return tokenRange.getBaseRange().getStart();
  }

  public BigInteger getEndToken() {
    return tokenRange.getBaseRange().getEnd();
  }
}
