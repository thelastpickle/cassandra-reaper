package com.spotify.reaper;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import io.dropwizard.Configuration;
import io.dropwizard.db.DataSourceFactory;

public class ReaperApplicationConfiguration extends Configuration {

  private int segmentCount;

  private boolean snapshotRepair;

  private double repairIntensity;

  @NotEmpty
  private String storageType;

  @Valid
  @NotNull
  @JsonProperty
  private DataSourceFactory database = new DataSourceFactory();

  @JsonProperty
  public int getSegmentCount() {
    return segmentCount;
  }

  @JsonProperty
  public void setSegmentCount(int segmentCount) {
    this.segmentCount = segmentCount;
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

  @JsonProperty
  public void setStorageType(String storageType) {
    this.storageType = storageType;
  }

  @JsonProperty
  public String getStorageType() {
    return storageType;
  }

  @JsonProperty
  public DataSourceFactory getDataSourceFactory() {
    return database;
  }
}
