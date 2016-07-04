package com.spotify.reaper.unit.service;

import com.spotify.reaper.ReaperApplicationConfiguration;
import com.spotify.reaper.ReaperApplicationConfigurationBuilder;
import org.apache.cassandra.repair.RepairParallelism;

public class TestRepairConfiguration {

  public static ReaperApplicationConfiguration defaultConfig() {
    return defaultConfigBuilder().build();
  }

  public static ReaperApplicationConfigurationBuilder defaultConfigBuilder() {
    return ReaperApplicationConfigurationBuilder.aReaperApplicationConfiguration()
        .withEnableCrossOrigin(true)
        .withHangingRepairTimeoutMins(30)
        .withRepairIntensity(0.9)
        .withRepairParallelism(RepairParallelism.DATACENTER_AWARE)
        .withRepairRunThreadCount(15)
        .withScheduleDaysBetween(1)
        .withSegmentCount(200)
        .withStorageType("memory")
        .withAutoScheduling(defaultAutoSchedulingConfigBuilder().build());
  }

  public static ReaperApplicationConfigurationBuilder.AutoSchedulingConfigurationBuilder defaultAutoSchedulingConfigBuilder() {
    return new ReaperApplicationConfigurationBuilder.AutoSchedulingConfigurationBuilder().thatIsDisabled();
  }
}
