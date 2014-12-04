package com.spotify.reaper.core;

import org.joda.time.DateTime;

import java.math.BigInteger;

public class RepairSegment {

  private Long id;
  private final ColumnFamily columnFamily;
  private final long runID;
  private final BigInteger startToken; // open/exclusive
  private final BigInteger endToken; // closed/inclusive
  private final State state;
  private final DateTime startTime;
  private final DateTime endTime;

  public Long getId() {
    return id;
  }

  public void setId(long id) {
    assert this.id == null : "cannot reset id after once set";
    this.id = id;
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

  public enum State {
    NOT_STARTED,
    RUNNING,
    DONE
  }

  private RepairSegment(Builder builder) {
    this.id = builder.id;
    this.columnFamily = builder.columnFamily;
    this.runID = builder.runID;
    this.startToken = builder.startToken;
    this.endToken = builder.endToken;
    this.state = builder.state;
    this.startTime = builder.startTime;
    this.endTime = builder.endTime;
  }

  public static class Builder {

    private Long id;
    private ColumnFamily columnFamily;
    private long runID;
    private BigInteger startToken;
    private BigInteger endToken;
    private RepairSegment.State state;
    private DateTime startTime;
    private DateTime endTime;

    public Builder id(long id) {
      this.id = id;
      return this;
    }

    public Builder columnFamily(ColumnFamily columnFamily) {
      this.columnFamily = columnFamily;
      return this;
    }

    public Builder runID(long runID) {
      this.runID = runID;
      return this;
    }

    public Builder startToken(BigInteger startToken) {
      this.startToken = startToken;
      return this;
    }

    public Builder endToken(BigInteger endToken) {
      this.endToken = endToken;
      return this;
    }

    public Builder state(RepairSegment.State state) {
      this.state = state;
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


    public RepairSegment build() {
      return new RepairSegment(this);
    }
  }

  @Override
  public String toString() {
    return String.format("(%s,%s]", startToken.toString(), endToken.toString());
  }
}
