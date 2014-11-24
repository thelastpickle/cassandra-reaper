package com.spotify.reaper;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.hibernate.validator.constraints.NotEmpty;

import io.dropwizard.Configuration;

public class ReaperApplicationConfiguration extends Configuration {

  private String testingValue;

  @JsonProperty
  public String getTestingValue() {
    return testingValue;
  }

  @JsonProperty
  public void setTestingValue(String testingValue) {
    this.testingValue = testingValue;
  }
}
