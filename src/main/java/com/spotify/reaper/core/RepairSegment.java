package com.spotify.reaper.core;

import org.joda.time.DateTime;

import java.math.BigInteger;

public class RepairSegment {
  private Long id;
  private final ColumnFamily columnFamily;
  private final long runID;
  private final int priority; // int/long/BigInteger?
  private final BigInteger startToken;
  private final BigInteger endToken;
  private final State state;
  private final DateTime startTime;
  private final DateTime endTime;

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public ColumnFamily getColumnFamily() {
    return columnFamily;
  }

  public long getRunID() {
    return runID;
  }

  public int getPriority() {
    return priority;
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

  public enum State {
    NOT_STARTED,
    RUNNING,
    DONE
  }

  private RepairSegment(RepairSegmentBuilder builder) {
    this.id = builder.id;
    this.columnFamily = builder.columnFamily;
    this.runID = builder.runID;
    this.priority = builder.priority;
    this.startToken = builder.startToken;
    this.endToken = builder.endToken;
    this.state = builder.state;
    this.startTime = builder.startTime;
    this.endTime = builder.endTime;
  }


  public static class RepairSegmentBuilder {
    private Long id;
    private ColumnFamily columnFamily;
    private long runID;
    private int priority;
    private BigInteger startToken;
    private BigInteger endToken;
    private RepairSegment.State state;
    private DateTime startTime;
    private DateTime endTime;

    public RepairSegmentBuilder id(long id) {
      this.id = id;
      return this;
    }

    public RepairSegmentBuilder columnFamily(ColumnFamily columnFamily) {
      this.columnFamily = columnFamily;
      return this;
    }

    public RepairSegmentBuilder runID(long runID) {
      this.runID = runID;
      return this;
    }

    public RepairSegmentBuilder priority(int priority) {
      this.priority = priority;
      return this;
    }

    public RepairSegmentBuilder startToken(BigInteger startToken) {
      this.startToken = startToken;
      return this;
    }

    public RepairSegmentBuilder endToken(BigInteger endToken) {
      this.endToken = endToken;
      return this;
    }

    public RepairSegmentBuilder state(RepairSegment.State state) {
      this.state = state;
      return this;
    }

    public RepairSegmentBuilder startTime(DateTime startTime) {
      this.startTime = startTime;
      return this;
    }

    public RepairSegmentBuilder endTime(DateTime endTime) {
      this.endTime = endTime;
      return this;
    }


    public RepairSegment build() {
      return new RepairSegment(this);
    }
  }


  @Override
  public String toString() {
    return String.format("(%s,%s)", startToken.toString(), endToken.toString());
  }
}
