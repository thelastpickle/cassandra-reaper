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

package io.cassandrareaper;

import io.cassandrareaper.core.JmxCredentials;
import io.cassandrareaper.crypto.CryptographFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.DecimalMin;
import javax.validation.constraints.Max;
import javax.validation.constraints.NotNull;
import javax.ws.rs.DefaultValue;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import io.dropwizard.client.HttpClientConfiguration;
import io.dropwizard.db.DataSourceFactory;
import org.apache.cassandra.repair.RepairParallelism;
import org.hibernate.validator.constraints.NotEmpty;
import org.secnod.dropwizard.shiro.ShiroConfiguration;
import systems.composable.dropwizard.cassandra.CassandraFactory;
import systems.composable.dropwizard.cassandra.network.AddressTranslatorFactory;

public final class ReaperApplicationConfiguration extends Configuration {

  private static final int DEFAULT_SEGMENT_COUNT_PER_NODE = 64;
  private static final Integer DEFAULT_MAX_PENDING_COMPACTIONS = 20;

  @JsonProperty
  private Integer maxPendingCompactions;

  @Deprecated
  @JsonProperty
  private Integer segmentCount;

  @JsonProperty private Integer segmentCountPerNode;

  @JsonProperty
  @NotNull
  private RepairParallelism repairParallelism;

  @JsonProperty
  @NotNull
  @DecimalMin(value = "0", inclusive = false)
  @Max(1)
  private Double repairIntensity;

  @JsonProperty
  @NotNull
  @DefaultValue("false")
  private Boolean incrementalRepair;

  @JsonProperty
  private Boolean blacklistTwcsTables;

  @DefaultValue("7")
  private Integer scheduleDaysBetween;

  @JsonProperty
  @DefaultValue("false")
  private Boolean useAddressTranslator;

  @Valid
  private Optional<AddressTranslatorFactory> jmxAddressTranslator = Optional.empty();

  @JsonProperty
  @NotNull
  private Integer repairRunThreadCount;

  @JsonProperty
  @Nullable
  private Integer maxParallelRepairs;

  @JsonProperty
  @NotNull
  private Integer hangingRepairTimeoutMins;

  @NotEmpty
  private String storageType;

  private String enableCrossOrigin;

  @JsonProperty
  private Map<String, Integer> jmxPorts;

  @JsonProperty
  private Jmxmp jmxmp = new Jmxmp();

  @JsonProperty
  private Map<String, JmxCredentials> jmxCredentials;

  @JsonProperty
  private JmxCredentials jmxAuth;

  @JsonProperty
  private AutoSchedulingConfiguration autoScheduling;

  @JsonProperty
  @DefaultValue("true")
  private Boolean enableDynamicSeedList;

  @JsonProperty
  private Integer repairManagerSchedulingIntervalSeconds;

  @JsonProperty
  @DefaultValue("false")
  private Boolean activateQueryLogger;

  @JsonProperty
  @DefaultValue("5")
  private Integer jmxConnectionTimeoutInSeconds;

  @JsonProperty
  @DefaultValue("7")
  private Integer clusterTimeoutInDays;

  @JsonProperty
  private DatacenterAvailability datacenterAvailability;

  @JsonProperty private AccessControlConfiguration accessControl;

  @JsonProperty
  private Integer repairThreadCount;
  /** If set to more than 0, defines how many days of run history should be kept. */
  @Nullable
  @JsonProperty
  private Integer purgeRecordsAfterInDays;

  /** If set to more than 0, defines how many runs to keep per repair unit. */
  @Nullable
  @JsonProperty
  private Integer numberOfRunsToKeepPerUnit;

  private CassandraFactory cassandra = new CassandraFactory();

  @Deprecated
  @JsonProperty
  private DataSourceFactory database;

  private DataSourceFactory relationalDb = new DataSourceFactory();

  @JsonProperty
  private Optional<String> enforcedLocalNode = Optional.empty();

  @JsonProperty
  private Optional<String> enforcedLocalClusterName = Optional.empty();

  @JsonProperty
  private Optional<String> enforcedLocalDatacenter = Optional.empty();

  @JsonProperty
  @DefaultValue("true")
  private Boolean enableConcurrentMigrations;

  private HttpClientConfiguration httpClient = new HttpClientConfiguration();

  @JsonProperty
  @Nullable
  private CryptographFactory cryptograph;

  public Jmxmp getJmxmp() {
    return jmxmp;
  }

  public void setJmxmp(Jmxmp jmxmp) {
    this.jmxmp = jmxmp;
  }

  public int getSegmentCount() {
    return segmentCount == null ? 0 : segmentCount;
  }

  public void setSegmentCount(int segmentCount) {
    this.segmentCount = segmentCount;
  }

  public int getSegmentCountPerNode() {
    return segmentCountPerNode == null ? DEFAULT_SEGMENT_COUNT_PER_NODE : segmentCountPerNode;
  }

  public void setSegmentCountPerNode(int segmentCountPerNode) {
    this.segmentCountPerNode = segmentCountPerNode;
  }

  public int getMaxPendingCompactions() {
    return maxPendingCompactions == null ? DEFAULT_MAX_PENDING_COMPACTIONS : maxPendingCompactions;
  }

  public void setMaxPendingCompactions(int maxPendingCompactions) {
    this.maxPendingCompactions = maxPendingCompactions;
  }

  public RepairParallelism getRepairParallelism() {
    return repairParallelism;
  }

  public void setRepairParallelism(RepairParallelism repairParallelism) {
    this.repairParallelism = repairParallelism;
  }

  public double getRepairIntensity() {
    return repairIntensity;
  }

  public void setRepairIntensity(double repairIntensity) {
    this.repairIntensity = repairIntensity;
  }

  public boolean getIncrementalRepair() {
    return incrementalRepair != null ? incrementalRepair : false;
  }

  public void setIncrementalRepair(boolean incrementalRepair) {
    this.incrementalRepair = incrementalRepair;
  }

  public boolean getBlacklistTwcsTables() {
    return blacklistTwcsTables != null ? blacklistTwcsTables : false;
  }

  public void setBlacklistTwcsTables(boolean blacklistTwcsTables) {
    this.blacklistTwcsTables = blacklistTwcsTables;
  }

  public Integer getScheduleDaysBetween() {
    return scheduleDaysBetween != null ? scheduleDaysBetween : 7;
  }

  public void setScheduleDaysBetween(int scheduleDaysBetween) {
    this.scheduleDaysBetween = scheduleDaysBetween;
  }

  public int getRepairRunThreadCount() {
    return repairRunThreadCount;
  }

  public void setRepairRunThreadCount(int repairRunThreadCount) {
    this.repairRunThreadCount = repairRunThreadCount;
  }

  public int getMaxParallelRepairs() {
    return maxParallelRepairs == null
        ? 2
        : maxParallelRepairs;
  }

  public void setMaxParallelRepairs(int maxParallelRepairs) {
    this.maxParallelRepairs = maxParallelRepairs;
  }

  public String getStorageType() {
    return storageType;
  }

  public void setEnableCrossOrigin(String enableCrossOrigin) {
    this.enableCrossOrigin = enableCrossOrigin;
  }

  public String getEnableCrossOrigin() {
    return this.enableCrossOrigin;
  }

  public boolean isEnableCrossOrigin() {
    return this.enableCrossOrigin != null && "true".equalsIgnoreCase(this.enableCrossOrigin);
  }

  public void setStorageType(String storageType) {
    this.storageType = storageType;
  }

  public DataSourceFactory getDataSourceFactory() {
    return database != null ? database : relationalDb;
  }

  @JsonProperty("h2")
  public DataSourceFactory getH2DataSourceFactory() {
    return relationalDb;
  }

  @JsonProperty("h2")
  public void setH2DataSourceFactory(DataSourceFactory h2) {
    this.relationalDb = h2;
  }

  @JsonProperty("postgres")
  public DataSourceFactory getPostgresDataSourceFactory() {
    return relationalDb;
  }

  @JsonProperty("postgres")
  public void setPostgresDataSourceFactory(DataSourceFactory postgres) {
    this.relationalDb = postgres;
  }

  public int getRepairManagerSchedulingIntervalSeconds() {
    return this.repairManagerSchedulingIntervalSeconds == null ? 30 : this.repairManagerSchedulingIntervalSeconds;
  }

  @JsonProperty
  public void setRepairManagerSchedulingIntervalSeconds(int repairManagerSchedulingIntervalSeconds) {
    this.repairManagerSchedulingIntervalSeconds = repairManagerSchedulingIntervalSeconds;
  }

  public Map<String, Integer> getJmxPorts() {
    return jmxPorts;
  }

  public void setJmxPorts(Map<String, Integer> jmxPorts) {
    this.jmxPorts = jmxPorts;
  }

  public JmxCredentials getJmxAuth() {
    return jmxAuth;
  }

  public void setJmxAuth(JmxCredentials jmxAuth) {
    this.jmxAuth = jmxAuth;
  }

  public Map<String, JmxCredentials> getJmxCredentials() {
    return jmxCredentials;
  }

  public void setJmxCredentials(Map<String, JmxCredentials> jmxCredentials) {
    this.jmxCredentials = jmxCredentials;
  }

  public boolean hasAutoSchedulingEnabled() {
    return autoScheduling != null && autoScheduling.isEnabled();
  }

  public AutoSchedulingConfiguration getAutoScheduling() {
    return autoScheduling;
  }

  public void setAutoScheduling(AutoSchedulingConfiguration autoRepairScheduling) {
    this.autoScheduling = autoRepairScheduling;
  }

  public void setEnableDynamicSeedList(boolean enableDynamicSeedList) {
    this.enableDynamicSeedList = enableDynamicSeedList;
  }

  public boolean getEnableDynamicSeedList() {
    return this.enableDynamicSeedList == null ? true : this.enableDynamicSeedList;
  }

  public void setActivateQueryLogger(boolean activateQueryLogger) {
    this.activateQueryLogger = activateQueryLogger;
  }

  public boolean getActivateQueryLogger() {
    return this.activateQueryLogger == null ? false : this.activateQueryLogger;
  }

  public void setUseAddressTranslator(boolean useAddressTranslator) {
    this.useAddressTranslator = useAddressTranslator;
  }

  public boolean useAddressTranslator() {
    return this.useAddressTranslator != null ? useAddressTranslator : false;
  }

  @JsonProperty("cassandra")
  public CassandraFactory getCassandraFactory() {
    return cassandra;
  }

  @JsonProperty("cassandra")
  public void setCassandraFactory(CassandraFactory cassandra) {
    this.cassandra = cassandra;
  }

  public int getHangingRepairTimeoutMins() {
    return hangingRepairTimeoutMins;
  }

  @JsonProperty
  public void setJmxConnectionTimeoutInSeconds(int jmxConnectionTimeoutInSeconds) {
    this.jmxConnectionTimeoutInSeconds = jmxConnectionTimeoutInSeconds;
  }

  public int getJmxConnectionTimeoutInSeconds() {
    return jmxConnectionTimeoutInSeconds != null ? jmxConnectionTimeoutInSeconds : 20;
  }

  @JsonProperty
  public void setClusterTimeoutInDays(int clusterTimeoutInDays) {
    this.clusterTimeoutInDays = clusterTimeoutInDays;
  }

  public int getClusterTimeoutInDays() {
    return clusterTimeoutInDays != null ? clusterTimeoutInDays : 7;
  }

  @JsonProperty
  public void setHangingRepairTimeoutMins(int hangingRepairTimeoutMins) {
    this.hangingRepairTimeoutMins = hangingRepairTimeoutMins;
  }

  public DatacenterAvailability getDatacenterAvailability() {
    return this.datacenterAvailability != null ? this.datacenterAvailability : DatacenterAvailability.ALL;
  }

  @JsonProperty("datacenterAvailability")
  public void setDatacenterAvailability(DatacenterAvailability datacenterAvailability) {
    this.datacenterAvailability = datacenterAvailability;
  }

  public AccessControlConfiguration getAccessControl() {
    return accessControl;
  }

  public void setAccessControl(AccessControlConfiguration accessControl) {
    this.accessControl = accessControl;
  }

  public boolean isAccessControlEnabled() {
    return getAccessControl() != null;
  }

  public int getRepairThreadCount() {
    return repairThreadCount != null ? repairThreadCount : 1;
  }

  public Integer getPurgeRecordsAfterInDays() {
    return purgeRecordsAfterInDays == null ? 0 : purgeRecordsAfterInDays;
  }

  @JsonProperty("purgeRecordsAfterInDays")
  public void setPurgeRecordsAfterInDays(Integer purgeRecordsAfterInDays) {
    this.purgeRecordsAfterInDays = purgeRecordsAfterInDays;
  }

  public Integer getNumberOfRunsToKeepPerUnit() {
    return numberOfRunsToKeepPerUnit == null ? 50 : numberOfRunsToKeepPerUnit;
  }

  @JsonProperty("numberOfRunsToKeepPerUnit")
  public void setNumberOfRunsToKeepPerUnit(Integer numberOfRunsToKeepPerUnit) {
    this.numberOfRunsToKeepPerUnit = numberOfRunsToKeepPerUnit;
  }

  public Boolean isInSidecarMode() {
    return datacenterAvailability == DatacenterAvailability.SIDECAR;
  }

  public Optional<String> getEnforcedLocalNode() {
    return enforcedLocalNode;
  }

  public Optional<String> getEnforcedLocalClusterName() {
    return enforcedLocalClusterName;
  }

  public Optional<String> getEnforcedLocalDatacenter() {
    return enforcedLocalDatacenter;
  }

  public void setEnforcedLocalNode(Optional<String> enforcedLocalNode) {
    this.enforcedLocalNode = enforcedLocalNode;
  }

  public void setEnforcedLocalClusterName(Optional<String> enforcedLocalClusterName) {
    this.enforcedLocalClusterName = enforcedLocalClusterName;
  }

  public void setEnforcedLocalDatacenter(Optional<String> enforcedLocalDatacenter) {
    this.enforcedLocalDatacenter = enforcedLocalDatacenter;
  }

  public boolean getEnableConcurrentMigrations() {
    return this.enableConcurrentMigrations == null ? true : this.enableConcurrentMigrations;
  }

  public void setEnableConcurrentMigrations(boolean enableConcurrentMigrations) {
    this.enableConcurrentMigrations = enableConcurrentMigrations;
  }

  public HttpClientConfiguration getHttpClientConfiguration() {
    return httpClient;
  }

  public void setHttpClientConfiguration(HttpClientConfiguration httpClient) {
    this.httpClient = httpClient;
  }

  @JsonProperty
  public Optional<AddressTranslatorFactory> getJmxAddressTranslator() {
    return jmxAddressTranslator;
  }

  @JsonProperty
  public void setJmxAddressTranslator(Optional<AddressTranslatorFactory> jmxAddressTranslator) {
    this.jmxAddressTranslator = jmxAddressTranslator;
  }

  @Nullable
  public CryptographFactory getCryptograph() {
    return cryptograph;
  }

  public void setCryptograph(@Nullable CryptographFactory cryptograph) {
    this.cryptograph = cryptograph;
  }

  public static final class AutoSchedulingConfiguration {

    @JsonProperty
    private Boolean enabled;

    @JsonProperty
    private Duration initialDelayPeriod;

    @JsonProperty
    private Duration periodBetweenPolls;

    @JsonProperty
    private Duration timeBeforeFirstSchedule;

    @JsonProperty
    private Duration scheduleSpreadPeriod;

    @JsonProperty
    private List<String> excludedKeyspaces = Collections.emptyList();

    @JsonProperty
    private List<String> excludedClusters = Collections.emptyList();

    public Boolean isEnabled() {
      return enabled;
    }

    public void setEnabled(Boolean enable) {
      this.enabled = enable;
    }

    public Duration getInitialDelayPeriod() {
      return initialDelayPeriod;
    }

    public void setInitialDelayPeriod(Duration initialDelayPeriod) {
      this.initialDelayPeriod = initialDelayPeriod;
    }

    public Duration getPeriodBetweenPolls() {
      return periodBetweenPolls;
    }

    public void setPeriodBetweenPolls(Duration periodBetweenPolls) {
      this.periodBetweenPolls = periodBetweenPolls;
    }

    public Duration getTimeBeforeFirstSchedule() {
      return timeBeforeFirstSchedule;
    }

    public void setTimeBeforeFirstSchedule(Duration timeBeforeFirstSchedule) {
      this.timeBeforeFirstSchedule = timeBeforeFirstSchedule;
    }

    public Duration getScheduleSpreadPeriod() {
      return scheduleSpreadPeriod;
    }

    public void setScheduleSpreadPeriod(Duration scheduleSpreadPeriod) {
      this.scheduleSpreadPeriod = scheduleSpreadPeriod;
    }

    public boolean hasScheduleSpreadPeriod() {
      return scheduleSpreadPeriod != null;
    }

    public void setExcludedKeyspaces(List<String> excludedKeyspaces) {
      this.excludedKeyspaces = null != excludedKeyspaces ? excludedKeyspaces : Collections.emptyList();
    }

    public List<String> getExcludedKeyspaces() {
      return excludedKeyspaces;
    }

    public void setExcludedClusters(List<String> excludedClusters) {
      this.excludedClusters = null != excludedClusters ? excludedClusters : Collections.emptyList();
    }

    public List<String> getExcludedClusters() {
      return excludedClusters;
    }

    @Override
    public String toString() {
      return "AutoSchedulingConfiguration{"
          + "enabled="
          + enabled
          + ", initialDelayPeriod="
          + initialDelayPeriod
          + ", periodBetweenPolls="
          + periodBetweenPolls
          + ", timeBeforeFirstSchedule="
          + timeBeforeFirstSchedule
          + ", scheduleSpreadPeriod="
          + scheduleSpreadPeriod
          + '}';
    }
  }

  public enum DatacenterAvailability {
    /* We require direct JMX access to all nodes across all datacenters */
    ALL,
    /* We require jmx access to all nodes in the local datacenter */
    LOCAL,
    /* Each datacenter requires at minimum one reaper instance that has jmx access to all nodes in that datacenter */
    EACH,
    /* Sets Reaper in sidecar mode where each Cassandra node has a collocated Reaper instance */
    SIDECAR;


    /**
     * Check if the current datacenter availability mode is to have collocation between Reaper and a DC/node.
     * @return true if we're in a collocated mode, false otherwise
     */
    public boolean isInCollocatedMode() {
      switch (this) {
        case LOCAL:
        case SIDECAR:
        case EACH:
          return true;
        default:
          return false;
      }
    }
  }

  public static final class AccessControlConfiguration {

    @JsonProperty private ShiroConfiguration shiro;
    @JsonProperty private Duration sessionTimeout;

    public ShiroConfiguration getShiroConfiguration() {
      return shiro;
    }

    public Duration getSessionTimeout() {
      return sessionTimeout != null ? sessionTimeout : Duration.ofMinutes(10);
    }


  }

  public static final class Jmxmp {

    @JsonProperty
    private Boolean ssl = false;

    @JsonProperty
    private Boolean enabled = false;

    public Boolean useSsl() {
      return ssl;
    }

    public Boolean isEnabled() {
      return enabled;
    }
  }
}
