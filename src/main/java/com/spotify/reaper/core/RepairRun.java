package com.spotify.reaper.core;

import org.joda.time.DateTime;

public class RepairRun {

  private final long id;

  // IDEA: maybe we want to have start and stop token for parallel runners on same repair run?
  //private final long startToken;
  //private final long endToken;

  private final String cause;
  private final String owner;
  private final RunState runState;
  private final DateTime creationTime;
  private final DateTime startTime;
  private final DateTime endTime;
  private final double intensity;

  public long getId() {
    return id;
  }

  public String getCause() {
    return cause;
  }

  public String getOwner() {
    return owner;
  }

  public RunState getState() {
    return runState;
  }

  public DateTime getCreationTime() {
    return creationTime;
  }

  public DateTime getStartTime() {
    return startTime;
  }

  public DateTime getEndTime() {
    return endTime;
  }

  public double getIntensity() {
    return intensity;
  }

  public enum RunState {
    NOT_STARTED,
    RUNNING,
    ERROR,
    DONE,
    PAUSED
  }

  private RepairRun(Builder builder, long id) {
    this.id = id;
    this.cause = builder.cause;
    this.owner = builder.owner;
    this.runState = builder.runState;
    this.creationTime = builder.creationTime;
    this.startTime = builder.startTime;
    this.endTime = builder.endTime;
    this.intensity = builder.intensity;
  }

  public static class Builder {

    public final RunState runState;
    public final DateTime creationTime;
    public final double intensity;
    private String cause;
    private String owner;
    private DateTime startTime;
    private DateTime endTime;

    public Builder(RunState runState, DateTime creationTime, double intensity) {
      this.runState = runState;
      this.creationTime = creationTime;
      this.intensity = intensity;
    }

    public Builder cause(String cause) {
      this.cause = cause;
      return this;
    }

    public Builder owner(String owner) {
      this.owner = owner;
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

    public RepairRun build(long id) {
      return new RepairRun(this, id);
    }
  }
}
