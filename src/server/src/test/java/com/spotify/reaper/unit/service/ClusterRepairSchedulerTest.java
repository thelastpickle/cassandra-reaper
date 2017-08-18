package com.spotify.reaper.unit.service;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.spotify.reaper.AppContext;
import com.spotify.reaper.ReaperApplicationConfiguration;
import com.spotify.reaper.ReaperException;
import com.spotify.reaper.cassandra.JmxConnectionFactory;
import com.spotify.reaper.cassandra.JmxProxy;
import com.spotify.reaper.core.Cluster;
import com.spotify.reaper.core.RepairSchedule;
import com.spotify.reaper.core.RepairUnit;
import com.spotify.reaper.service.ClusterRepairScheduler;
import com.spotify.reaper.storage.MemoryStorage;
import org.apache.cassandra.repair.RepairParallelism;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;

import static java.lang.String.format;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClusterRepairSchedulerTest {

  private static final Cluster CLUSTER = new Cluster("cluster1", null, Collections.singleton("127.0.0.1"));
  private static final DateTime TWO_HOURS_AGO = DateTime.now().minusHours(2);
  private static final Duration DELAY_BEFORE_SCHEDULE = Duration.ofMinutes(4);
  private AppContext context;
  private ClusterRepairScheduler clusterRepairAuto;
  private JmxProxy jmxProxy;

  @Before
  public void setup() throws ReaperException {
    context = new AppContext();
    context.storage = new MemoryStorage();
    context.config = TestRepairConfiguration.defaultConfigBuilder()
        .withAutoScheduling(TestRepairConfiguration.defaultAutoSchedulingConfigBuilder()
            .thatIsEnabled()
            .withTimeBeforeFirstSchedule(DELAY_BEFORE_SCHEDULE)
            .build())
        .build();
    context.jmxConnectionFactory = mock(JmxConnectionFactory.class);
    clusterRepairAuto = new ClusterRepairScheduler(context);
    jmxProxy = mock(JmxProxy.class);
    when(context.jmxConnectionFactory.connectAny(CLUSTER, context.config.getJmxConnectionTimeoutInSeconds())).thenReturn(jmxProxy);
  }

  @Test
  public void schedulesRepairForAllKeyspacesInAllClusters() throws Exception {
    context.storage.addCluster(CLUSTER);
    when(jmxProxy.getKeyspaces()).thenReturn(Lists.newArrayList("keyspace1", "keyspace3"));
    when(jmxProxy.getTableNamesForKeyspace("keyspace1")).thenReturn(Sets.newHashSet("table1"));
    when(jmxProxy.getTableNamesForKeyspace("keyspace3")).thenReturn(Sets.newHashSet("table1"));

    clusterRepairAuto.scheduleRepairs(CLUSTER);

    assertThat(context.storage.getAllRepairSchedules()).hasSize(2);
    assertThatClusterRepairSchedules(context.storage.getRepairSchedulesForCluster(CLUSTER.getName()))
        .hasScheduleCount(2)
        .repairScheduleForKeyspace("keyspace1")
        .hasSameConfigItemsAs(context.config)
        .hasOwner("auto-scheduling")
        .hasNextActivationDateCloseTo(timeOfFirstSchedule())
        .andThen()
        .repairScheduleForKeyspace("keyspace3")
        .hasSameConfigItemsAs(context.config)
        .hasOwner("auto-scheduling")
        .hasNextActivationDateCloseTo(timeOfFirstSchedule());
  }

  @Test
  public void removeSchedulesForKeyspaceThatNoLongerExists() throws Exception {
    context.storage.addCluster(CLUSTER);
    context.storage.addRepairSchedule(aRepairSchedule(CLUSTER, "keyspace1", TWO_HOURS_AGO));
    context.storage.addRepairSchedule(aRepairSchedule(CLUSTER, "keyspace2", TWO_HOURS_AGO));

    when(jmxProxy.getKeyspaces()).thenReturn(Lists.newArrayList("keyspace1"));

    clusterRepairAuto.scheduleRepairs(CLUSTER);

    assertThat(context.storage.getAllRepairSchedules()).hasSize(1);
    assertThatClusterRepairSchedules(context.storage.getRepairSchedulesForCluster(CLUSTER.getName()))
        .hasScheduleCount(1)
        .repairScheduleForKeyspace("keyspace1").hasCreationTimeCloseTo(TWO_HOURS_AGO);
  }

  @Test
  public void addSchedulesForNewKeyspace() throws Exception {
    context.storage.addCluster(CLUSTER);
    context.storage.addRepairSchedule(aRepairSchedule(CLUSTER, "keyspace1", TWO_HOURS_AGO));

    when(jmxProxy.getKeyspaces()).thenReturn(Lists.newArrayList("keyspace1", "keyspace2"));
    when(jmxProxy.getTableNamesForKeyspace("keyspace1")).thenReturn(Sets.newHashSet("table1"));
    when(jmxProxy.getTableNamesForKeyspace("keyspace2")).thenReturn(Sets.newHashSet("table2"));

    clusterRepairAuto.scheduleRepairs(CLUSTER);

    assertThat(context.storage.getAllRepairSchedules()).hasSize(2);
    assertThatClusterRepairSchedules(context.storage.getRepairSchedulesForCluster(CLUSTER.getName()))
        .hasScheduleCount(2)
        .repairScheduleForKeyspace("keyspace1").hasCreationTime(TWO_HOURS_AGO)
        .andThen()
        .repairScheduleForKeyspace("keyspace2").hasCreationTimeCloseTo(DateTime.now());
  }

  @Test
  public void doesNotScheduleRepairForSystemKeyspaces() throws Exception {
    context.storage.addCluster(CLUSTER);
    when(jmxProxy.getKeyspaces()).thenReturn(Lists.newArrayList("system", "system_auth", "system_traces", "keyspace2"));
    when(jmxProxy.getTableNamesForKeyspace("keyspace2")).thenReturn(Sets.newHashSet("table1"));

    clusterRepairAuto.scheduleRepairs(CLUSTER);
    assertThat(context.storage.getAllRepairSchedules()).hasSize(1);
    assertThatClusterRepairSchedules(context.storage.getRepairSchedulesForCluster(CLUSTER.getName()))
        .hasScheduleCount(1)
        .repairScheduleForKeyspace("keyspace2");
  }

  @Test
  public void doesNotScheduleRepairWhenKeyspaceHasNoTable() throws Exception {
    context.storage.addCluster(CLUSTER);
    when(jmxProxy.getKeyspaces()).thenReturn(Lists.newArrayList("keyspace1"));
    when(jmxProxy.getTableNamesForKeyspace("keyspace1")).thenReturn(Sets.newHashSet());

    clusterRepairAuto.scheduleRepairs(CLUSTER);
    assertThat(context.storage.getAllRepairSchedules()).hasSize(0);
  }

  @Test
  public void spreadsKeyspaceScheduling() throws Exception {
    context.config = TestRepairConfiguration.defaultConfigBuilder()
        .withAutoScheduling(TestRepairConfiguration.defaultAutoSchedulingConfigBuilder()
            .thatIsEnabled()
            .withTimeBeforeFirstSchedule(DELAY_BEFORE_SCHEDULE)
            .withScheduleSpreadPeriod(Duration.ofHours(6))
            .build())
        .build();

    context.storage.addCluster(CLUSTER);
    when(jmxProxy.getKeyspaces()).thenReturn(Lists.newArrayList("keyspace1", "keyspace2", "keyspace3", "keyspace4"));
    when(jmxProxy.getTableNamesForKeyspace("keyspace1")).thenReturn(Sets.newHashSet("sometable"));
    when(jmxProxy.getTableNamesForKeyspace("keyspace2")).thenReturn(Sets.newHashSet("sometable"));
    when(jmxProxy.getTableNamesForKeyspace("keyspace4")).thenReturn(Sets.newHashSet("sometable"));

    clusterRepairAuto.scheduleRepairs(CLUSTER);
    assertThatClusterRepairSchedules(context.storage.getRepairSchedulesForCluster(CLUSTER.getName()))
        .hasScheduleCount(3)
        .repairScheduleForKeyspace("keyspace1").hasNextActivationDateCloseTo(timeOfFirstSchedule()).andThen()
        .repairScheduleForKeyspace("keyspace2").hasNextActivationDateCloseTo(timeOfFirstSchedule().plusHours(6)).andThen()
        .repairScheduleForKeyspace("keyspace4").hasNextActivationDateCloseTo(timeOfFirstSchedule().plusHours(12));
  }

  private DateTime timeOfFirstSchedule() {
    return DateTime.now().plus(DELAY_BEFORE_SCHEDULE.toMillis());
  }

  private RepairSchedule.Builder aRepairSchedule(Cluster cluster, String keyspace, DateTime creationTime) {
    RepairUnit repairUnit = context.storage.addRepairUnit(aRepair(cluster, keyspace));
    return new RepairSchedule.Builder(
        repairUnit.getId(),
        RepairSchedule.State.ACTIVE,
        1,
        DateTime.now(),
        ImmutableList.of(),
        10,
        RepairParallelism.DATACENTER_AWARE,
        0.9,
        creationTime
    );
  }

  private RepairUnit.Builder aRepair(Cluster cluster, String keyspace) {
    return new RepairUnit.Builder(cluster.getName(), keyspace, Collections.emptySet(), Boolean.FALSE);
  }

  private ClusterRepairScheduleAssertion assertThatClusterRepairSchedules(Collection<RepairSchedule> repairSchedules) {
    return new ClusterRepairScheduleAssertion(repairSchedules);
  }

  private class ClusterRepairScheduleAssertion {
    private final Collection<RepairSchedule> repairSchedules;

    public ClusterRepairScheduleAssertion(Collection<RepairSchedule> repairSchedules) {
      this.repairSchedules = repairSchedules;
    }

    public ClusterRepairScheduleAssertion hasScheduleCount(int size) {
      assertThat(size).isEqualTo(size);
      return this;
    }

    public ClusterRepairScheduleAssertion.RepairScheduleAssertion repairScheduleForKeyspace(String keyspace) {
      RepairSchedule keyspaceRepairSchedule = repairSchedules.stream()
          .filter(repairSchedule -> context.storage.getRepairUnit(repairSchedule.getRepairUnitId()).get().getKeyspaceName().equals(keyspace))
          .findFirst()
          .orElseThrow(() -> new AssertionError(format("No repair schedule found for keyspace %s", keyspace)));
      return new ClusterRepairScheduleAssertion.RepairScheduleAssertion(keyspaceRepairSchedule);
    }

    public class RepairScheduleAssertion {

      private final RepairSchedule repairSchedule;

      public RepairScheduleAssertion(RepairSchedule repairSchedule) {
        this.repairSchedule = repairSchedule;
      }

      public ClusterRepairScheduleAssertion.RepairScheduleAssertion hasSameConfigItemsAs(ReaperApplicationConfiguration config) {
        assertThat(repairSchedule.getDaysBetween()).isEqualTo(config.getScheduleDaysBetween());
        assertThat(repairSchedule.getIntensity()).isEqualTo(config.getRepairIntensity());
        assertThat(repairSchedule.getSegmentCount()).isEqualTo(config.getSegmentCount());
        assertThat(repairSchedule.getRepairParallelism()).isEqualTo(config.getRepairParallelism());
        return this;
      }

      public ClusterRepairScheduleAssertion.RepairScheduleAssertion hasNextActivationDateCloseTo(DateTime dateTime) {
        assertThat(repairSchedule.getNextActivation().toDate()).isCloseTo(dateTime.toDate(), Duration.ofSeconds(10).toMillis());
        return this;
      }

      public ClusterRepairScheduleAssertion.RepairScheduleAssertion hasOwner(String owner) {
        assertThat(repairSchedule.getOwner()).isEqualTo(owner);
        return this;
      }

      public ClusterRepairScheduleAssertion.RepairScheduleAssertion hasCreationTimeCloseTo(DateTime dateTime) {
        assertThat(repairSchedule.getCreationTime().toDate()).isCloseTo(dateTime.toDate(), Duration.ofSeconds(10).toMillis());
        return this;
      }

      public ClusterRepairScheduleAssertion.RepairScheduleAssertion hasCreationTime(DateTime dateTime) {
        assertThat(repairSchedule.getCreationTime()).isEqualTo(dateTime);
        return this;
      }

      public ClusterRepairScheduleAssertion andThen() {
        return ClusterRepairScheduleAssertion.this;
      }
    }
  }
}