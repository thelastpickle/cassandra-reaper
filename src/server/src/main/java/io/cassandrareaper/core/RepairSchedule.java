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

package io.cassandrareaper.core;

import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.cassandra.repair.RepairParallelism;
import org.joda.time.DateTime;

public final class RepairSchedule {

  private final UUID id;

  private final UUID repairUnitId;
  private final State state;
  private final int daysBetween;
  private final DateTime nextActivation;
  private final ImmutableList<UUID> runHistory;
  private final int segmentCount;
  private final RepairParallelism repairParallelism;
  private final double intensity;
  private final DateTime creationTime;
  private final String owner;
  private final DateTime pauseTime;

  private RepairSchedule(Builder builder, UUID id) {
    this.id = id;
    this.repairUnitId = builder.repairUnitId;
    this.state = builder.state;
    this.daysBetween = builder.daysBetween;
    this.nextActivation = builder.nextActivation;
    this.runHistory = builder.runHistory;
    this.segmentCount = builder.segmentCount;
    this.repairParallelism = builder.repairParallelism;
    this.intensity = builder.intensity;
    this.creationTime = builder.creationTime;
    this.owner = builder.owner;
    this.pauseTime = builder.pauseTime;
  }

  public UUID getId() {
    return id;
  }

  public UUID getRepairUnitId() {
    return repairUnitId;
  }

  public State getState() {
    return state;
  }

  public int getDaysBetween() {
    return daysBetween;
  }

  public DateTime getFollowingActivation() {
    return getNextActivation().plusDays(getDaysBetween());
  }

  public DateTime getNextActivation() {
    return nextActivation;
  }

  public ImmutableList<UUID> getRunHistory() {
    return runHistory;
  }

  /**
   * Required for JDBI mapping into database. Generic collection type would be hard to map into Postgres array types.
   */
  public LongCollectionSqlType getRunHistorySql() {
    List<Long> list = runHistory.stream().map(UUID::getMostSignificantBits).collect(Collectors.toList());
    return new LongCollectionSqlType(list);
  }

  public int getSegmentCount() {
    return segmentCount;
  }

  public RepairParallelism getRepairParallelism() {
    return repairParallelism;
  }

  public double getIntensity() {
    return intensity;
  }

  public DateTime getCreationTime() {
    return creationTime;
  }

  public String getOwner() {
    return owner;
  }

  public DateTime getPauseTime() {
    return pauseTime;
  }

  public Builder with() {
    return new Builder(this);
  }

  public enum State {
    ACTIVE,
    PAUSED,
    DELETED
  }

  public static class Builder {

    public final UUID repairUnitId;
    private State state;
    private int daysBetween;
    private DateTime nextActivation;
    private ImmutableList<UUID> runHistory;
    private int segmentCount;
    private RepairParallelism repairParallelism;
    private double intensity;
    private DateTime creationTime;
    private String owner;
    private DateTime pauseTime;

    public Builder(
        UUID repairUnitId,
        State state,
        int daysBetween,
        DateTime nextActivation,
        ImmutableList<UUID> runHistory,
        int segmentCount,
        RepairParallelism repairParallelism,
        double intensity,
        DateTime creationTime) {
      this.repairUnitId = repairUnitId;
      this.state = state;
      this.daysBetween = daysBetween;
      this.nextActivation = nextActivation;
      this.runHistory = runHistory;
      this.segmentCount = segmentCount;
      this.repairParallelism = repairParallelism;
      this.intensity = intensity;
      this.creationTime = creationTime;
    }

    private Builder(RepairSchedule original) {
      repairUnitId = original.repairUnitId;
      state = original.state;
      daysBetween = original.daysBetween;
      nextActivation = original.nextActivation;
      runHistory = original.runHistory;
      segmentCount = original.segmentCount;
      repairParallelism = original.repairParallelism;
      intensity = original.intensity;
      creationTime = original.creationTime;
      owner = original.owner;
      pauseTime = original.pauseTime;
      intensity = original.intensity;
    }

    public Builder state(State state) {
      this.state = state;
      return this;
    }

    public Builder daysBetween(int daysBetween) {
      this.daysBetween = daysBetween;
      return this;
    }

    public Builder nextActivation(DateTime nextActivation) {
      this.nextActivation = nextActivation;
      return this;
    }

    public Builder runHistory(ImmutableList<UUID> runHistory) {
      this.runHistory = runHistory;
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

    public Builder intensity(double intensity) {
      this.intensity = intensity;
      return this;
    }

    public Builder creationTime(DateTime creationTime) {
      this.creationTime = creationTime;
      return this;
    }

    public Builder owner(String owner) {
      this.owner = owner;
      return this;
    }

    public Builder pauseTime(DateTime pauseTime) {
      this.pauseTime = pauseTime;
      return this;
    }

    public RepairSchedule build(UUID id) {
      return new RepairSchedule(this, id);
    }
  }

  /**
   * This is required to be able to map in generic manner into Postgres array types through JDBI.
   */
  public static final class LongCollectionSqlType {

    private final Collection<Long> collection;

    public LongCollectionSqlType(Collection<Long> collection) {
      this.collection = collection;
    }

    public Collection<Long> getValue() {
      return null == collection ? collection : Lists.newArrayList();
    }
  }
}
