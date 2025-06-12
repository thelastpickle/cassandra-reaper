/*
 * Copyright 2014-2017 Spotify AB
 * Copyright 2016-2019 The Last Pickle Ltd
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

package io.cassandrareaper.core;

import java.math.BigInteger;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.base.Preconditions;
import jakarta.annotation.Nullable;
import org.joda.time.DateTime;

@JsonDeserialize(builder = RepairSegment.Builder.class)
public final class RepairSegment {

  private static final boolean STRICT = !Boolean.getBoolean("reaper.disableSegmentChecks");

  private final UUID id;
  private final UUID runId;
  private final UUID repairUnitId;
  private final Segment tokenRange;
  private final int failCount;
  private final State state;
  private final String coordinatorHost;
  private final DateTime startTime;
  private final DateTime endTime;
  private final Map<String, String> replicas = new ConcurrentHashMap<>();
  // hostID field is only ever populated for incremental repairs. For full repairs it is always
  // null.
  private final UUID hostID;

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
    if (builder.replicas != null) {
      this.replicas.putAll(builder.replicas);
    }
    this.hostID = builder.hostID;
  }

  public static Builder builder(Segment tokenRange, UUID repairUnitId) {
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

  public Segment getTokenRange() {
    return tokenRange;
  }

  @JsonIgnore
  public BigInteger getStartToken() {
    return tokenRange.getBaseRange().getStart();
  }

  @JsonIgnore
  public BigInteger getEndToken() {
    return tokenRange.getBaseRange().getEnd();
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

  public Map<String, String> getReplicas() {
    return replicas;
  }

  public Builder with() {
    return new Builder(this);
  }

  public boolean hasStartTime() {
    return null != startTime;
  }

  public boolean hasEndTime() {
    return null != endTime;
  }

  public UUID getHostID() {
    return hostID;
  }

  /** Reset to NOT_STARTED state, with nulled startTime and endTime. */
  public Builder reset() {
    Builder builder = new Builder(this);
    builder.state = State.NOT_STARTED;
    builder.startTime = null;
    builder.endTime = null;
    return builder;
  }

  public enum State {
    // don't change order or insert items (ordial is used in db)
    NOT_STARTED,
    RUNNING,
    DONE,
    STARTED
  }

  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "with")
  public static final class Builder {

    private UUID repairUnitId;
    private Segment tokenRange;
    private UUID id;
    private UUID runId;
    private int failCount;
    private State state;
    private String coordinatorHost;
    private DateTime startTime;
    private DateTime endTime;
    private Map<String, String> replicas;
    private UUID hostID;

    private Builder() {}

    private Builder(Segment tokenRange, UUID repairUnitId) {
      Preconditions.checkNotNull(tokenRange);
      Preconditions.checkNotNull(repairUnitId);
      this.repairUnitId = repairUnitId;
      this.tokenRange = tokenRange;
      this.failCount = 0;
      this.state = State.NOT_STARTED;
      this.replicas = tokenRange.getReplicas();
      this.hostID = null;
    }

    private Builder(RepairSegment original) {
      runId = original.runId;
      id = original.id;
      repairUnitId = original.repairUnitId;
      tokenRange = original.tokenRange;
      failCount = original.failCount;
      state = original.state;
      coordinatorHost = original.coordinatorHost;
      startTime = original.startTime;
      endTime = original.endTime;
      replicas = original.replicas;
      hostID = original.hostID;
    }

    public Builder withRunId(UUID runId) {
      Preconditions.checkNotNull(runId);
      this.runId = runId;
      return this;
    }

    public Builder withRepairUnitId(UUID repairUnitId) {
      Preconditions.checkNotNull(repairUnitId);
      this.repairUnitId = repairUnitId;
      return this;
    }

    public Builder withTokenRange(Segment tokenRange) {
      this.tokenRange = tokenRange;
      return this;
    }

    public Builder withFailCount(int failCount) {
      this.failCount = failCount;
      return this;
    }

    public Builder withState(State state) {
      Preconditions.checkNotNull(state);
      this.state = state;
      return this;
    }

    public Builder withCoordinatorHost(@Nullable String coordinatorHost) {
      this.coordinatorHost = coordinatorHost;
      return this;
    }

    public Builder withStartTime(DateTime startTime) {
      Preconditions.checkState(
          null != startTime || null == endTime,
          "unsetting startTime only permitted if endTime unset");

      this.startTime = startTime;
      return this;
    }

    public Builder withEndTime(DateTime endTime) {
      this.endTime = endTime;
      return this;
    }

    public Builder withId(@Nullable UUID segmentId) {
      this.id = segmentId;
      return this;
    }

    public Builder withReplicas(Map<String, String> replicas) {
      this.replicas = replicas;
      return this;
    }

    public Builder withHostID(UUID hostID) {
      this.hostID = hostID;
      return this;
    }

    public RepairSegment build() {
      // a null segmentId is a special case where the storage uses a sequence for it
      Preconditions.checkNotNull(runId);
      if (STRICT) {
        Preconditions.checkState(
            null != startTime || null == endTime, "if endTime is set, so must startTime be set");
        Preconditions.checkState(
            null == endTime || State.DONE == state, "endTime can only be set if segment is DONE");

        Preconditions.checkState(
            null != startTime || State.NOT_STARTED == state,
            "startTime must be set if segment is RUNNING or DONE");
      } else {
        if (null != endTime && State.DONE != state) {
          endTime = null;
        }
        if (null == startTime && null != endTime) {
          startTime = endTime.minusSeconds(1);
        }
      }

      return new RepairSegment(this, this.id);
    }
  }
}
