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

import com.google.common.collect.Range;

import org.joda.time.DateTime;

import java.math.BigInteger;

public class RepairSegment {

  private final long id;
  private final Integer repairCommandId; // received when triggering repair in Cassandra
  private final ColumnFamily columnFamily;
  private final long runID;
  private final BigInteger startToken; // open
  private final BigInteger endToken; // closed
  private final State state;
  private final DateTime startTime;
  private final DateTime endTime;

  public long getId() {
    return id;
  }

  public int getRepairCommandId() {
    return this.repairCommandId;
  }

  public ColumnFamily getColumnFamily() {
    return columnFamily;
  }

  public long getRunID() {
    return runID;
  }

  public BigInteger getStartToken() {
    return startToken;
  }

  public BigInteger getEndToken() {
    return endToken;
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

  public static RepairSegment getCopy(RepairSegment origSegment, State newState,
                                      int newRepairCommandId,
                                      DateTime newStartTime, DateTime newEndTime) {
    return new Builder(origSegment.getColumnFamily(), origSegment.getRunID(),
                                     origSegment.getStartToken(), origSegment.getEndToken(),
                                     newState)
        .repairCommandId(newRepairCommandId)
        .startTime(newStartTime)
        .endTime(newEndTime).build(origSegment.getId());
  }

  public enum State {
    NOT_STARTED,
    RUNNING,
    ERROR,
    DONE
  }

  private RepairSegment(Builder builder, long id) {
    this.id = id;
    this.repairCommandId = builder.repairCommandId;
    this.columnFamily = builder.columnFamily;
    this.runID = builder.runID;
    this.startToken = builder.startToken;
    this.endToken = builder.endToken;
    this.state = builder.state;
    this.startTime = builder.startTime;
    this.endTime = builder.endTime;
  }

  public static class Builder {

    public final ColumnFamily columnFamily;
    public final long runID;
    public final BigInteger startToken;
    public final BigInteger endToken;
    public final State state;
    private int repairCommandId;
    private DateTime startTime;
    private DateTime endTime;

    public Builder(ColumnFamily columnFamily, long runID,
                   BigInteger startToken, BigInteger endToken, State state) {
      this.columnFamily = columnFamily;
      this.runID = runID;
      this.startToken = startToken;
      this.endToken = endToken;
      this.state = state;
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

    @Override
    public String toString() {
      return String.format("(%s,%s]", startToken.toString(), endToken.toString());
    }
  }

  public String toString() {
    return String.format("(%s,%s]",
                         startToken.toString(),
                         endToken.toString());
  }
}
