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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

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
  private final int totalSegments;
  private final int completedSegments;

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

  @JsonIgnore
  public DateTime getCreationTime() {
    return creationTime;
  }

  @JsonProperty("creationTime")
  public String getCreationTimeISO8601() {
    if (creationTime == null) return null;
    return creationTime.toDateTime(DateTimeZone.UTC).toString(
        "YYYY-MM-dd'T'HH:mm:ss'Z'");
  }

  @JsonIgnore
  public DateTime getStartTime() {
    return startTime;
  }

  @JsonProperty("startTime")
  public String getStartTimeISO8601() {
    if (startTime == null) return null;
    return startTime.toDateTime(DateTimeZone.UTC).toString(
        "YYYY-MM-dd'T'HH:mm:ss'Z'");
  }

  @JsonIgnore
  public DateTime getEndTime() {
    return endTime;
  }

  @JsonProperty("endTime")
  public String getEndTimeISO8601() {
    if (endTime == null) return null;
    return endTime.toDateTime(DateTimeZone.UTC).toString(
        "YYYY-MM-dd'T'HH:mm:ss'Z'");
  }

  public double getIntensity() {
    return intensity;
  }

  public int getTotalSegments() {
    return totalSegments;
  }

  public int getCompletedSegments() {
    return completedSegments;
  }

  public static RepairRun getCopy(RepairRun repairRun, RunState newState,
                                  DateTime startTime, DateTime endTime, int completedSegments) {
    return new RepairRun.Builder(newState,
                                 repairRun.getCreationTime(), repairRun.getIntensity(),
                                 repairRun.getTotalSegments(), completedSegments)
        .cause(repairRun.getCause())
        .owner(repairRun.getOwner())
        .startTime(startTime)
        .endTime(endTime)
        .build(repairRun.getId());
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
    this.totalSegments = builder.totalSegments;
    this.completedSegments = builder.completedSegments;
  }

  public static class Builder {

    public final RunState runState;
    public final DateTime creationTime;
    public final double intensity;
    public final int totalSegments;
    public final int completedSegments;
    private String cause;
    private String owner;
    private DateTime startTime;
    private DateTime endTime;

    public Builder(RunState runState, DateTime creationTime,
                   double intensity, int totalSegments, int completedSegments) {
      this.runState = runState;
      this.creationTime = creationTime;
      this.intensity = intensity;
      this.totalSegments = totalSegments;
      this.completedSegments = completedSegments;
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
