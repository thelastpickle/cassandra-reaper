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
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
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
  private final RepairParallelism repairParallelism;
  private final double intensity;
  private final DateTime creationTime;
  private final String owner;
  private final DateTime pauseTime;
  private final int segmentCountPerNode;
  private final boolean adaptive;

  private RepairSchedule(Builder builder, UUID id) {
    this.id = id;
    this.repairUnitId = builder.repairUnitId;
    this.state = builder.state;
    this.daysBetween = builder.daysBetween;
    this.nextActivation = builder.nextActivation;
    this.runHistory = builder.runHistory;
    this.repairParallelism = builder.repairParallelism;
    this.intensity = builder.intensity;
    this.creationTime = builder.creationTime;
    this.owner = builder.owner;
    this.pauseTime = builder.pauseTime;
    this.segmentCountPerNode = builder.segmentCountPerNode;
    this.adaptive = builder.adaptive;
  }

  public static Builder builder(UUID repairUnitId) {
    return new Builder(repairUnitId);
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

  public int getSegmentCountPerNode() {
    return segmentCountPerNode;
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

  public boolean getAdaptive() {
    return adaptive;
  }

  public Builder with() {
    return new Builder(this);
  }

  @Override
  public String toString() {
    return String.format("%s[%s]", getClass().getSimpleName(), id.toString());
  }

  public enum State {
    ACTIVE,
    PAUSED,
    DELETED
  }

  public static final class Builder {

    public final UUID repairUnitId;
    private State state = RepairSchedule.State.ACTIVE;
    private Integer daysBetween;
    private DateTime nextActivation;
    private ImmutableList<UUID> runHistory = ImmutableList.<UUID>of();
    private RepairParallelism repairParallelism;
    private Double intensity;
    private DateTime creationTime = DateTime.now();
    private String owner = "";
    private DateTime pauseTime;
    private Integer segmentCountPerNode;
    private boolean adaptive = false;

    private Builder(UUID repairUnitId) {
      this.repairUnitId = repairUnitId;
    }

    private Builder(RepairSchedule original) {
      repairUnitId = original.repairUnitId;
      state = original.state;
      daysBetween = original.daysBetween;
      nextActivation = original.nextActivation;
      runHistory = original.runHistory;
      repairParallelism = original.repairParallelism;
      intensity = original.intensity;
      creationTime = original.creationTime;
      owner = original.owner;
      pauseTime = original.pauseTime;
      intensity = original.intensity;
      segmentCountPerNode = original.segmentCountPerNode;
      adaptive = original.adaptive;
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

    public Builder segmentCountPerNode(int segmentCountPerNode) {
      this.segmentCountPerNode = segmentCountPerNode;
      return this;
    }

    public Builder adaptive(boolean adaptive) {
      this.adaptive = adaptive;
      return this;
    }

    public RepairSchedule build(UUID id) {
      Preconditions.checkState(null != daysBetween, "daysBetween(..) must be called before build(..)");
      Preconditions.checkState(null != nextActivation, "nextActivation(..) must be called before build(..)");
      Preconditions.checkState(null != repairParallelism, "repairParallelism(..) must be called before build(..)");
      Preconditions.checkState(null != intensity, "intensity(..) must be called before build(..)");
      Preconditions.checkState(null != segmentCountPerNode, "segmentCountPerNode(..) must be called before build(..)");
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
      return null != collection ? collection : Lists.newArrayList();
    }
  }
}
