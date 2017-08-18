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
package com.spotify.reaper.core;

import com.spotify.reaper.service.RingRange;

import org.joda.time.DateTime;

import java.math.BigInteger;
import java.util.UUID;

public class RepairSegment {

  private final UUID id;
  private final UUID runId;
  private final UUID repairUnitId;
  private final RingRange tokenRange;
  private final int failCount;
  private final State state;
  private final String coordinatorHost;
  private final Integer repairCommandId; // received when triggering repair in Cassandra
  private final DateTime startTime;
  private final DateTime endTime;

  private RepairSegment(Builder builder, UUID id) {
    this.id = id;
    this.runId = builder.runId;
    this.repairUnitId = builder.repairUnitId;
    this.tokenRange = builder.tokenRange;
    this.failCount = builder.failCount;
    this.state = builder.state;
    this.coordinatorHost = builder.coordinatorHost;
    this.repairCommandId = builder.repairCommandId;
    this.startTime = builder.startTime;
    this.endTime = builder.endTime;
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

  public String getCoordinatorHost() {
    return coordinatorHost;
  }

  public Integer getRepairCommandId() {
    return repairCommandId;
  }

  public DateTime getStartTime() {
    return startTime;
  }

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

  public static class Builder {


    public final RingRange tokenRange;
    private final UUID repairUnitId;
    private UUID runId;
    private int failCount;
    private State state;
    private String coordinatorHost;
    private Integer repairCommandId;
    private DateTime startTime;
    private DateTime endTime;

    public Builder(RingRange tokenRange, UUID repairUnitId) {
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
      repairCommandId = original.repairCommandId;
      startTime = original.startTime;
      endTime = original.endTime;
    }

    public Builder withRunId(UUID runId){
        this.runId = runId;
        return this;
    }

    public Builder failCount(int failCount) {
      this.failCount = failCount;
      return this;
    }

    public Builder state(State state) {
      this.state = state;
      return this;
    }

    public Builder coordinatorHost(String coordinatorHost) {
      this.coordinatorHost = coordinatorHost;
      return this;
    }

    public Builder repairCommandId(Integer repairCommandId) {
      this.repairCommandId = repairCommandId;
      return this;
    }

    public Builder startTime(DateTime startTime) {
      this.startTime = startTime;
      return this;
    }

    public Builder endTime(DateTime endTime) {
      this.endTime = endTime;
      return this;
    }

    public RepairSegment build(UUID id) {
      return new RepairSegment(this, id);
    }
  }
}
