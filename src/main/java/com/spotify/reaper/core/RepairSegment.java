package com.spotify.reaper.core;

import com.google.common.collect.Range;

import org.joda.time.DateTime;

import java.math.BigInteger;

public class RepairSegment {

  private final long id;
  private final Integer repairCommandId; // received when triggering repair in Cassandra
  private final ColumnFamily columnFamily;
  private final long runID;
  private final Range<BigInteger> tokenRange;
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
    return tokenRange.lowerEndpoint();
  }

  public BigInteger getEndToken() {
    return tokenRange.upperEndpoint();
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
    DONE
  }

  private RepairSegment(Builder builder, long id) {
    this.id = id;
    this.repairCommandId = builder.repairCommandId;
    this.columnFamily = builder.columnFamily;
    this.runID = builder.runID;
    this.tokenRange = Range.openClosed(builder.startToken, builder.endToken);
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
      this.repairCommandId = repairCommandId;
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
                         tokenRange.lowerEndpoint().toString(),
                         tokenRange.upperEndpoint().toString());
  }
}
