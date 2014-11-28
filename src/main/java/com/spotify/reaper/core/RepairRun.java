package com.spotify.reaper.core;

import org.joda.time.DateTime;

public class RepairRun {
  private Long id;
  private final String cause;
  private final String owner;
  private final State state;
  private final DateTime creationTime;
  private final DateTime startTime;
  private final DateTime endTime;
  private final double intensity;

  public enum State {
    NOT_STARTED,
    RUNNING,
    DONE,
    PAUSED
  }

  private RepairRun(RepairRunBuilder builder) {
    this.id = builder.id;
    this.cause = builder.cause;
    this.owner = builder.owner;
    this.state = builder.state;
    this.creationTime = builder.creationTime;
    this.startTime = builder.startTime;
    this.endTime = builder.endTime;
    this.intensity = builder.intensity;
  }


  public static class RepairRunBuilder {
    private Long id;
    private String cause;
    private String owner;
    private State state;
    private DateTime creationTime;
    private DateTime startTime;
    private DateTime endTime;
    private double intensity;

    public RepairRunBuilder id(long id) {
      this.id = id;
      return this;
    }

    public RepairRunBuilder cause(String cause) {
      this.cause = cause;
      return this;
    }

    public RepairRunBuilder owner(String owner) {
      this.owner = owner;
      return this;
    }

    public RepairRunBuilder state(State state) {
      this.state = state;
      return this;
    }

    public RepairRunBuilder creationTime(DateTime creationTime) {
      this.creationTime = creationTime;
      return this;
    }

    public RepairRunBuilder startTime(DateTime startTime) {
      this.startTime = startTime;
      return this;
    }

    public RepairRunBuilder endTime(DateTime endTime) {
      this.endTime = endTime;
      return this;
    }

    public RepairRunBuilder intensity(double intensity) {
      this.intensity = intensity;
      return this;
    }


    public RepairRun build() {
      return new RepairRun(this);
    }
  }
}
