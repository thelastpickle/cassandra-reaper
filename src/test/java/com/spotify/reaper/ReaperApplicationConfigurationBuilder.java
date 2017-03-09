package com.spotify.reaper;

import org.apache.cassandra.repair.RepairParallelism;

import java.time.Duration;
import java.util.Map;

public class ReaperApplicationConfigurationBuilder {

  private Integer segmentCount;
  private RepairParallelism repairParallelism;
  private Double repairIntensity;
  private Integer scheduleDaysBetween;
  private Integer repairRunThreadCount;
  private Integer hangingRepairTimeoutMins;
  private String storageType;
  private Boolean enableCrossOrigin;
  private Map<String, Integer> jmxPorts;
  private ReaperApplicationConfiguration.JmxCredentials jmxAuth;
  private ReaperApplicationConfiguration.AutoSchedulingConfiguration autoRepairScheduling;

  private ReaperApplicationConfigurationBuilder() {
  }

  public static ReaperApplicationConfigurationBuilder aReaperApplicationConfiguration() {
    return new ReaperApplicationConfigurationBuilder();
  }

  public ReaperApplicationConfigurationBuilder withSegmentCount(Integer segmentCount) {
    this.segmentCount = segmentCount;
    return this;
  }

  public ReaperApplicationConfigurationBuilder withRepairParallelism(RepairParallelism repairParallelism) {
    this.repairParallelism = repairParallelism;
    return this;
  }

  public ReaperApplicationConfigurationBuilder withRepairIntensity(Double repairIntensity) {
    this.repairIntensity = repairIntensity;
    return this;
  }

  public ReaperApplicationConfigurationBuilder withScheduleDaysBetween(Integer scheduleDaysBetween) {
    this.scheduleDaysBetween = scheduleDaysBetween;
    return this;
  }

  public ReaperApplicationConfigurationBuilder withRepairRunThreadCount(Integer repairRunThreadCount) {
    this.repairRunThreadCount = repairRunThreadCount;
    return this;
  }

  public ReaperApplicationConfigurationBuilder withHangingRepairTimeoutMins(Integer hangingRepairTimeoutMins) {
    this.hangingRepairTimeoutMins = hangingRepairTimeoutMins;
    return this;
  }

  public ReaperApplicationConfigurationBuilder withStorageType(String storageType) {
    this.storageType = storageType;
    return this;
  }

  public ReaperApplicationConfigurationBuilder withEnableCrossOrigin(Boolean enableCrossOrigin) {
    this.enableCrossOrigin = enableCrossOrigin;
    return this;
  }

  public ReaperApplicationConfigurationBuilder withJmxPorts(Map<String, Integer> jmxPorts) {
    this.jmxPorts = jmxPorts;
    return this;
  }

  public ReaperApplicationConfigurationBuilder withJmxAuth(ReaperApplicationConfiguration.JmxCredentials jmxAuth) {
    this.jmxAuth = jmxAuth;
    return this;
  }

  public ReaperApplicationConfigurationBuilder withAutoScheduling(ReaperApplicationConfiguration.AutoSchedulingConfiguration autoRepairScheduling) {
    this.autoRepairScheduling = autoRepairScheduling;
    return this;
  }

  public ReaperApplicationConfiguration build() {
    ReaperApplicationConfiguration reaperApplicationConfiguration = new ReaperApplicationConfiguration();
    reaperApplicationConfiguration.setSegmentCount(segmentCount);
    reaperApplicationConfiguration.setRepairParallelism(repairParallelism);
    reaperApplicationConfiguration.setRepairIntensity(repairIntensity);
    reaperApplicationConfiguration.setScheduleDaysBetween(scheduleDaysBetween);
    reaperApplicationConfiguration.setRepairRunThreadCount(repairRunThreadCount);
    reaperApplicationConfiguration.setHangingRepairTimeoutMins(hangingRepairTimeoutMins);
    reaperApplicationConfiguration.setStorageType(storageType);
    reaperApplicationConfiguration.setEnableCrossOrigin(enableCrossOrigin.toString());
    reaperApplicationConfiguration.setJmxPorts(jmxPorts);
    reaperApplicationConfiguration.setJmxAuth(jmxAuth);
    reaperApplicationConfiguration.setAutoScheduling(autoRepairScheduling);
    return reaperApplicationConfiguration;
  }

  public static class AutoSchedulingConfigurationBuilder {
    private Boolean enabled;
    private Duration initialDelayPeriod;
    private Duration periodBetweenPolls;
    private Duration timeBeforeFirstSchedule;
    private Duration scheduleSpreadPeriod;

    public ReaperApplicationConfiguration.AutoSchedulingConfiguration build() {
      ReaperApplicationConfiguration.AutoSchedulingConfiguration autoSchedulingConfig = new ReaperApplicationConfiguration.AutoSchedulingConfiguration();
      autoSchedulingConfig.setEnabled(enabled);
      autoSchedulingConfig.setInitialDelayPeriod(initialDelayPeriod);
      autoSchedulingConfig.setPeriodBetweenPolls(periodBetweenPolls);
      autoSchedulingConfig.setTimeBeforeFirstSchedule(timeBeforeFirstSchedule);
      autoSchedulingConfig.setScheduleSpreadPeriod(scheduleSpreadPeriod);
      return autoSchedulingConfig;
    }

    public AutoSchedulingConfigurationBuilder thatIsEnabled() {
      this.enabled = true;
      return this;
    }

    public AutoSchedulingConfigurationBuilder thatIsDisabled() {
      this.enabled = false;
      return this;
    }

    public AutoSchedulingConfigurationBuilder withInitialDelay(Duration delay) {
      this.initialDelayPeriod = delay;
      return this;
    }

    public AutoSchedulingConfigurationBuilder withPeriodBetweenPolls(Duration period) {
      this.periodBetweenPolls = period;
      return this;
    }

    public AutoSchedulingConfigurationBuilder withTimeBeforeFirstSchedule(Duration period) {
      this.timeBeforeFirstSchedule = period;
      return this;
    }

    public AutoSchedulingConfigurationBuilder withScheduleSpreadPeriod(Duration scheduleSpreadPeriod) {
      this.scheduleSpreadPeriod = scheduleSpreadPeriod;
      return this;
    }
  }

}
