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

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import com.google.common.base.Preconditions;
import org.apache.cassandra.repair.RepairParallelism;
import org.joda.time.DateTime;
import org.joda.time.DateTimeComparator;

import static java.lang.Math.min;

public final class RepairRun implements Comparable<RepairRun> {

  private final UUID id;

  // IDEA: maybe we want to have start and stop token for parallel runners on same repair run?
  // private final long startToken;
  // private final long endToken;
  private final String cause;
  private final String owner;
  private final String clusterName;
  private final UUID repairUnitId;
  private final RunState runState;
  private final DateTime creationTime;
  private final DateTime startTime;
  private final DateTime endTime;
  private final DateTime pauseTime;
  private final double intensity;
  private final String lastEvent;
  private final int segmentCount;
  private final RepairParallelism repairParallelism;
  private final Set<String> tables;
  private final boolean adaptiveSchedule;

  private RepairRun(Builder builder, UUID id) {
    this.id = id;
    this.clusterName = builder.clusterName;
    this.repairUnitId = builder.repairUnitId;
    this.cause = builder.cause;
    this.owner = builder.owner;
    this.runState = builder.runState;
    this.creationTime = builder.creationTime;
    this.startTime = builder.startTime;
    this.endTime = builder.endTime;
    this.pauseTime = builder.pauseTime;
    this.intensity = builder.intensity;
    this.lastEvent = builder.lastEvent;
    this.segmentCount = builder.segmentCount;
    this.repairParallelism = builder.repairParallelism;
    this.tables = builder.tables;
    this.adaptiveSchedule = builder.adaptiveSchedule;
  }

  public static Builder builder(String clusterName, UUID repairUnitId) {
    return new Builder(clusterName, repairUnitId);
  }

  public UUID getId() {
    return id;
  }

  public UUID getRepairUnitId() {
    return repairUnitId;
  }

  public String getClusterName() {
    return clusterName;
  }

  public String getCause() {
    return cause;
  }

  public String getOwner() {
    return owner;
  }

  public RunState getRunState() {
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

  public DateTime getPauseTime() {
    return pauseTime;
  }

  public double getIntensity() {
    return intensity;
  }

  public String getLastEvent() {
    return lastEvent;
  }

  public int getSegmentCount() {
    return segmentCount;
  }

  public RepairParallelism getRepairParallelism() {
    return repairParallelism;
  }

  public Set<String> getTables() {
    return tables;
  }

  public Boolean getAdaptiveSchedule() {
    return adaptiveSchedule;
  }

  public Builder with() {
    return new Builder(this);
  }

  /**
   * Order RepairRun instances by time. Primarily endTime, secondarily startTime. Descending, i.e. latest first.
   *
   * @param other the RepairRun compared to
   * @return negative if this RepairRun is later than the specified RepairRun. Positive if earlier. 0 if equal.
   */
  @Override
  public int compareTo(RepairRun other) {
    DateTimeComparator comparator = DateTimeComparator.getInstance();
    int endTimeComparison = comparator.compare(endTime, other.endTime);
    if (endTimeComparison != 0) {
      return -endTimeComparison;
    } else {
      return -comparator.compare(startTime, other.startTime);
    }
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if (!(other instanceof RepairRun)) {
      return false;
    }
    RepairRun run = (RepairRun) other;
    return this.id.equals(run.id) && this.repairUnitId.equals(run.repairUnitId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.id, this.repairUnitId);
  }

  @Override
  public String toString() {
    return String.format("%s[%s] for %s", getClass().getSimpleName(), id.toString(), clusterName);
  }

  // The values in this enum are declared in order of "interestingness",
  // this is used to order RepairRuns in the UI so that e.g. RUNNING runs come first.
  public enum RunState {
    RUNNING,
    PAUSED,
    NOT_STARTED,

    ERROR,
    DONE,
    ABORTED,
    DELETED;

    public boolean isActive() {
      return this == RUNNING || this == PAUSED;
    }

    public boolean isTerminated() {
      return this == DONE || this == ERROR || this == ABORTED || this == DELETED;
    }
  }

  public static final class Builder {

    public final String clusterName;
    public final UUID repairUnitId;
    private RunState runState = RunState.NOT_STARTED;
    private DateTime creationTime = DateTime.now();
    private Double intensity;
    private String cause = "";
    private String owner = "";
    private DateTime startTime;
    private DateTime endTime;
    private DateTime pauseTime;
    private String lastEvent = "no events";
    private Integer segmentCount;
    private RepairParallelism repairParallelism;
    private Set<String> tables;
    private boolean adaptiveSchedule;


    private Builder(String clusterName, UUID repairUnitId) {
      this.clusterName = clusterName;
      this.repairUnitId = repairUnitId;
    }

    private Builder(RepairRun original) {
      clusterName = original.clusterName;
      repairUnitId = original.repairUnitId;
      runState = original.runState;
      creationTime = original.creationTime;
      intensity = original.intensity;
      cause = original.cause;
      owner = original.owner;
      startTime = original.startTime;
      endTime = original.endTime;
      pauseTime = original.pauseTime;
      lastEvent = original.lastEvent;
      segmentCount = original.segmentCount;
      repairParallelism = original.repairParallelism;
      tables = original.tables;
      adaptiveSchedule = original.adaptiveSchedule;
    }

    public Builder runState(RunState runState) {
      this.runState = runState;
      if (RunState.PAUSED != runState) {
        pauseTime = null;
      }
      return this;
    }

    public Builder creationTime(DateTime creationTime) {
      this.creationTime = creationTime;
      return this;
    }

    public Builder intensity(double intensity) {
      this.intensity = intensity;
      return this;
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

    public Builder pauseTime(DateTime pauseTime) {
      this.pauseTime = pauseTime;
      return this;
    }

    public Builder lastEvent(String event) {
      this.lastEvent = event;
      return this;
    }

    public Builder segmentCount(int segmentCount) {
      this.segmentCount = segmentCount;
      return this;
    }

    public Builder repairParallelism(RepairParallelism repairParallelism) {
      this.repairParallelism = repairParallelism;
      return this;
    }

    public Builder tables(Set<String> tables) {
      this.tables = Collections.unmodifiableSet(tables);
      return this;
    }

    public Builder adaptiveSchedule(boolean adaptive) {
      this.adaptiveSchedule = adaptive;
      return this;
    }

    public RepairRun build(UUID id) {
      Preconditions.checkState(null != repairParallelism, "repairParallelism(..) must be called before build(..)");
      Preconditions.checkState(null != intensity, "intensity(..) must be called before build(..)");
      Preconditions.checkState(null != segmentCount, "segmentCount(..) must be called before build(..)");
      Preconditions.checkState(null != tables, "tables(..) must be called before build(..)");

      Preconditions.checkState(
          RunState.NOT_STARTED == runState || null != startTime,
          "startTime only valid when runState is not NOT_STARTED. %s %s", runState, startTime);

      Preconditions.checkState(
          RunState.NOT_STARTED != runState || null == startTime,
          "startTime must be null when runState is NOT_STARTED. %s %s", runState, startTime);

      Preconditions.checkState(
          RunState.PAUSED == runState || null == pauseTime,
          "pausedTime only valid when runState is PAUSED. %s %s", runState, pauseTime);

      Preconditions.checkState(
          RunState.PAUSED != runState || null != pauseTime,
          "pausedTime must be set when runState is PAUSED. %s %s", runState, pauseTime);

      Preconditions.checkState(
          runState.isTerminated() || null == endTime,
          "endTime only valid when runState is terminated. %s %s", runState, endTime);

      Preconditions.checkState(
          !runState.isTerminated() || null != endTime,
          "endTime must be set when runState is terminated. %s %s", runState, endTime);

      return new RepairRun(this, id);
    }
  }

    public static void SortByRunState (List<RepairRun> repairRunCollection) {
    Comparator<RepairRun> comparator = new Comparator<RepairRun>() {
      @Override
      public int compare(RepairRun o1, RepairRun o2) {
        if ((!o1.getRunState().isTerminated()) && o2.getRunState().isTerminated()) {
          return -1; // o1 appears first.
        }  else if (o1.getRunState().isTerminated() && !o2.getRunState().isTerminated()) {
          return 1; // o2 appears first.
        } else { // Both RunStates have equal isFinished() values; compare on time instead.
          return o1.getId().compareTo(o2.getId());
        }
      }
    };
    Collections.sort(repairRunCollection, comparator);
  }
}
