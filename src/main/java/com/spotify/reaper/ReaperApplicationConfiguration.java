package com.spotify.reaper;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

import io.dropwizard.Configuration;

public class ReaperApplicationConfiguration extends Configuration {

  private int segmentCount;

  @NotEmpty
  private String repairStrategy;

  private boolean snapshotRepair;

  private double repairIntensity;


  @JsonProperty
  public int getSegmentCount() {
    return segmentCount;
  }

  @JsonProperty
  public void setSegmentCount(int segmentCount) {
    this.segmentCount = segmentCount;
  }


  @JsonProperty
  public String getRepairStrategy() {
    return repairStrategy;
  }

  @JsonProperty
  public void setRepairStrategy(String repairStrategy) {
    this.repairStrategy = repairStrategy;
  }


  @JsonProperty
  public boolean getSnapshotRepair() {
    return snapshotRepair;
  }

  @JsonProperty
  public void setSnapshotRepair(boolean snapshotRepair) {
    this.snapshotRepair = snapshotRepair;
  }


  @JsonProperty
  public double getRepairIntensity() {
    return repairIntensity;
  }

  @JsonProperty
  public void setRepairIntensity(double repairIntensity) {
    this.repairIntensity = repairIntensity;
  }
}
