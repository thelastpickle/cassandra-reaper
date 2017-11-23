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

package io.cassandrareaper.core;

import io.cassandrareaper.service.RingRange;

import java.math.BigInteger;
import java.util.UUID;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import org.joda.time.DateTime;

public final class RepairSegment {

  private final UUID id;
  private final UUID runId;
  private final UUID repairUnitId;
  private final RingRange tokenRange;
  private final int failCount;
  private final State state;
  private final String coordinatorHost;
  private final DateTime startTime;
  private final DateTime endTime;

  private RepairSegment(Builder builder, @Nullable UUID id) {
    this.id = id;
    this.runId = builder.runId;
    this.repairUnitId = builder.repairUnitId;
    this.tokenRange = builder.tokenRange;
    this.failCount = builder.failCount;
    this.state = builder.state;
    this.coordinatorHost = builder.coordinatorHost;
    this.startTime = builder.startTime;
    this.endTime = builder.endTime;
  }

  public static Builder builder(RingRange tokenRange, UUID repairUnitId) {
    return new Builder(tokenRange, repairUnitId);
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

  public RingRange getTokenRange() {
    return tokenRange;
  }

  public BigInteger getStartToken() {
    return tokenRange.getStart();
  }

  public BigInteger getEndToken() {
    return tokenRange.getEnd();
  }

  public int getFailCount() {
    return failCount;
  }

  public RepairSegment.State getState() {
    return state;
  }

  @Nullable
  public String getCoordinatorHost() {
    return coordinatorHost;
  }

  @Nullable
  public DateTime getStartTime() {
    return startTime;
  }

  @Nullable
  public DateTime getEndTime() {
    return endTime;
  }

  public Builder with() {
    return new Builder(this);
  }

  public enum State {
    NOT_STARTED,
    RUNNING,
    DONE
  }

  public static final class Builder {

    public final RingRange tokenRange;
    private final UUID repairUnitId;
    private UUID runId;
    private int failCount;
    private State state;
    private String coordinatorHost;
    private DateTime startTime;
    private DateTime endTime;

    private Builder(RingRange tokenRange, UUID repairUnitId) {
      Preconditions.checkNotNull(tokenRange);
      Preconditions.checkNotNull(repairUnitId);
      this.repairUnitId = repairUnitId;
      this.tokenRange = tokenRange;
      this.failCount = 0;
      this.state = State.NOT_STARTED;
    }

    private Builder(RepairSegment original) {
      runId = original.runId;
      repairUnitId = original.repairUnitId;
      tokenRange = original.tokenRange;
      failCount = original.failCount;
      state = original.state;
      coordinatorHost = original.coordinatorHost;
      startTime = original.startTime;
      endTime = original.endTime;
    }

    public Builder withRunId(UUID runId) {
      Preconditions.checkNotNull(runId);
      this.runId = runId;
      return this;
    }

    public Builder failCount(int failCount) {
      this.failCount = failCount;
      return this;
    }

    public Builder state(State state) {
      Preconditions.checkNotNull(state);
      this.state = state;
      return this;
    }

    public Builder coordinatorHost(@Nullable String coordinatorHost) {
      this.coordinatorHost = coordinatorHost;
      return this;
    }

    public Builder startTime(@Nullable DateTime startTime) {
      Preconditions.checkState(
          null != startTime || null == endTime,
          "unsetting startTime only permitted if endTime unset");

      this.startTime = startTime;
      return this;
    }

    public Builder endTime(DateTime endTime) {
      Preconditions.checkNotNull(endTime);
      this.endTime = endTime;
      return this;
    }

    public RepairSegment build(@Nullable UUID segmentId) {
      // a null segmentId is a special case where the storage uses a sequence for it
      Preconditions.checkNotNull(runId);
      Preconditions.checkState(null != startTime || null == endTime, "if endTime is set, so must startTime be set");
      Preconditions.checkState(null == endTime || State.DONE == state, "endTime can only be set if segment is DONE");

      Preconditions.checkState(
          null != startTime || State.NOT_STARTED == state,
          "startTime must be set if segment is RUNNING or DONE");

      return new RepairSegment(this, segmentId);
    }
  }
}
