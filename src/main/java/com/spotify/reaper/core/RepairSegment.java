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

public class RepairSegment {

  private final long id;
  private final Integer repairCommandId; // received when triggering repair in Cassandra
  private final long columnFamilyId;
  private final long runId;
  private final RingRange tokenRange;
  private final State state;
  private final DateTime startTime;
  private final DateTime endTime;

  public long getId() {
    return id;
  }

  public int getRepairCommandId() {
    return this.repairCommandId;
  }

  public long getColumnFamilyId() {
    return columnFamilyId;
  }

  public long getRunId() {
    return runId;
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

  public State getState() {
    return state;
  }

  public DateTime getStartTime() {
    return startTime;
  }

  public DateTime getEndTime() {
    return endTime;
  }

  public enum State {
    NOT_STARTED,
    RUNNING,
    ERROR,
    DONE
  }

  private RepairSegment(Builder builder,long id) {
    this.id = id;
    this.repairCommandId = builder.repairCommandId;
    this.columnFamilyId = builder.columnFamilyId;
    this.runId = builder.runId;
    this.tokenRange = builder.tokenRange;
    this.state = builder.state;
    this.startTime = builder.startTime;
    this.endTime = builder.endTime;
  }

  public Builder with() {
    return new Builder(this);
  }

  public static class Builder {

    public final long runId;
    public final RingRange tokenRange;
    private State state;
    private long columnFamilyId;
    private int repairCommandId;
    private DateTime startTime;
    private DateTime endTime;

    public Builder(long runId, RingRange tokenRange, State state) {
      this.runId = runId;
      this.tokenRange = tokenRange;
      this.state = state;
    }

    private Builder(RepairSegment original) {
      runId = original.runId;
      tokenRange = original.tokenRange;
      state = original.state;
      columnFamilyId = original.columnFamilyId;
      repairCommandId = original.repairCommandId;
      startTime = original.startTime;
      endTime = original.endTime;
    }

    public Builder state(State state) {
      this.state = state;
      return this;
    }

    public Builder columnFamilyId(long columnFamilyId) {
      this.columnFamilyId = columnFamilyId;
      return this;
    }

    public Builder repairCommandId(int repairCommandId) {
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

    public RepairSegment build(long id) {
      return new RepairSegment(this, id);
    }
  }
}
