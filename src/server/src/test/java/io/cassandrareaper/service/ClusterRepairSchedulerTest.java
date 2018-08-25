/*
 * Copyright 2017-2017 Spotify AB
 * Copyright 2017-2019 The Last Pickle Ltd
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

package io.cassandrareaper.service;

import io.cassandrareaper.AppContext;
import io.cassandrareaper.ReaperApplicationConfiguration;
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.RepairSchedule;
import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.core.Table;
import io.cassandrareaper.jmx.ClusterProxy;
import io.cassandrareaper.jmx.JmxConnectionFactory;
import io.cassandrareaper.jmx.JmxProxy;
import io.cassandrareaper.storage.MemoryStorage;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.cassandra.repair.RepairParallelism;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static java.lang.String.format;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class ClusterRepairSchedulerTest {

  private static final Cluster CLUSTER = new Cluster("cluster1", null, Collections.singleton("127.0.0.1"));
  private static final DateTime TWO_HOURS_AGO = DateTime.now().minusHours(2);
  private static final Duration DELAY_BEFORE_SCHEDULE = Duration.ofMinutes(4);
  private static final String STCS = "SizeTieredCompactionStrategy";

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

    when(context.jmxConnectionFactory.connectAny(CLUSTER))
        .thenReturn(jmxProxy);

    when(context.jmxConnectionFactory.connectAny(Mockito.anyCollection()))
        .thenReturn(jmxProxy);

    context.clusterProxy = ClusterProxy.create(context);
  }

  @Test
  public void schedulesRepairForAllKeyspacesInAllClusters() throws Exception {
    context.storage.addCluster(CLUSTER);
    when(jmxProxy.getKeyspaces()).thenReturn(Lists.newArrayList("keyspace1", "keyspace3"));
    when(jmxProxy.getTablesForKeyspace("keyspace1")).thenReturn(
            Sets.newHashSet(Table.builder().withName("table1").withCompactionStrategy(STCS).build()));
    when(jmxProxy.getTablesForKeyspace("keyspace3")).thenReturn(
            Sets.newHashSet(Table.builder().withName("table1").withCompactionStrategy(STCS).build()));

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
    when(jmxProxy.getTablesForKeyspace("keyspace1")).thenReturn(
            Sets.newHashSet(Table.builder().withName("table1").withCompactionStrategy(STCS).build()));
    when(jmxProxy.getTablesForKeyspace("keyspace2")).thenReturn(
            Sets.newHashSet(Table.builder().withName("table2").withCompactionStrategy(STCS).build()));

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
    when(jmxProxy.getTablesForKeyspace("keyspace2")).thenReturn(
            Sets.newHashSet(Table.builder().withName("table1").withCompactionStrategy(STCS).build()));

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
    when(jmxProxy.getTablesForKeyspace("keyspace1")).thenReturn(Sets.newHashSet());

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
    when(jmxProxy.getTablesForKeyspace("keyspace1")).thenReturn(
            Sets.newHashSet(Table.builder().withName("sometable").withCompactionStrategy(STCS).build()));
    when(jmxProxy.getTablesForKeyspace("keyspace2")).thenReturn(
            Sets.newHashSet(Table.builder().withName("sometable").withCompactionStrategy(STCS).build()));
    when(jmxProxy.getTablesForKeyspace("keyspace4")).thenReturn(
            Sets.newHashSet(Table.builder().withName("sometable").withCompactionStrategy(STCS).build()));

    clusterRepairAuto.scheduleRepairs(CLUSTER);
    assertThatClusterRepairSchedules(context.storage.getRepairSchedulesForCluster(CLUSTER.getName()))
        .hasScheduleCount(3)
        .repairScheduleForKeyspace("keyspace1")
        .hasNextActivationDateCloseTo(timeOfFirstSchedule())
        .andThen()
        .repairScheduleForKeyspace("keyspace2")
        .hasNextActivationDateCloseTo(timeOfFirstSchedule().plusHours(6))
        .andThen()
        .repairScheduleForKeyspace("keyspace4")
        .hasNextActivationDateCloseTo(timeOfFirstSchedule().plusHours(12));
  }

  private DateTime timeOfFirstSchedule() {
    return DateTime.now().plus(DELAY_BEFORE_SCHEDULE.toMillis());
  }

  private RepairSchedule.Builder aRepairSchedule(Cluster cluster, String keyspace, DateTime creationTime) {
    RepairUnit repairUnit = context.storage.addRepairUnit(aRepair(cluster, keyspace));

    return RepairSchedule.builder(repairUnit.getId())
        .creationTime(creationTime)
        .daysBetween(1)
        .nextActivation(DateTime.now())
        .repairParallelism(RepairParallelism.DATACENTER_AWARE)
        .intensity(0.9)
        .segmentCount(10)
        .segmentCountPerNode(0);

  }

  private RepairUnit.Builder aRepair(Cluster cluster, String keyspace) {
    return RepairUnit.builder()
        .clusterName(cluster.getName())
        .keyspaceName(keyspace)
        .incrementalRepair(Boolean.FALSE)
        .repairThreadCount(1);
  }

  private ClusterRepairScheduleAssertion assertThatClusterRepairSchedules(Collection<RepairSchedule> repairSchedules) {
    return new ClusterRepairScheduleAssertion(repairSchedules);
  }

  private class ClusterRepairScheduleAssertion {

    private final Collection<RepairSchedule> repairSchedules;

    ClusterRepairScheduleAssertion(Collection<RepairSchedule> repairSchedules) {
      this.repairSchedules = repairSchedules;
    }

    ClusterRepairScheduleAssertion hasScheduleCount(int size) {
      assertThat(size).isEqualTo(size);
      return this;
    }

    ClusterRepairScheduleAssertion.RepairScheduleAssertion repairScheduleForKeyspace(String keyspace) {
      RepairSchedule keyspaceRepairSchedule = repairSchedules.stream()
          .filter(repairSchedule
              -> context.storage.getRepairUnit(repairSchedule.getRepairUnitId()).getKeyspaceName().equals(keyspace))
          .findFirst()
          .orElseThrow(() -> new AssertionError(format("No repair schedule found for keyspace %s", keyspace)));
      return new ClusterRepairScheduleAssertion.RepairScheduleAssertion(keyspaceRepairSchedule);
    }

    private final class RepairScheduleAssertion {

      private final RepairSchedule repairSchedule;

      RepairScheduleAssertion(RepairSchedule repairSchedule) {
        this.repairSchedule = repairSchedule;
      }

      RepairScheduleAssertion hasSameConfigItemsAs(ReaperApplicationConfiguration config) {

        assertThat(repairSchedule.getDaysBetween()).isEqualTo(config.getScheduleDaysBetween());
        assertThat(repairSchedule.getIntensity()).isEqualTo(config.getRepairIntensity());
        assertThat(repairSchedule.getSegmentCount()).isEqualTo(0);
        assertThat(repairSchedule.getSegmentCountPerNode())
            .isEqualTo(config.getSegmentCountPerNode());
        assertThat(repairSchedule.getRepairParallelism()).isEqualTo(config.getRepairParallelism());
        return this;
      }

      RepairScheduleAssertion hasNextActivationDateCloseTo(DateTime dateTime) {

        assertThat(
            repairSchedule.getNextActivation().toDate()).isCloseTo(dateTime.toDate(),
            Duration.ofSeconds(10).toMillis());

        return this;
      }

      RepairScheduleAssertion hasOwner(String owner) {
        assertThat(repairSchedule.getOwner()).isEqualTo(owner);
        return this;
      }

      RepairScheduleAssertion hasCreationTimeCloseTo(DateTime dateTime) {

        assertThat(
            repairSchedule.getCreationTime().toDate()).isCloseTo(dateTime.toDate(),
            Duration.ofSeconds(10).toMillis());

        return this;
      }

      RepairScheduleAssertion hasCreationTime(DateTime dateTime) {
        assertThat(repairSchedule.getCreationTime()).isEqualTo(dateTime);
        return this;
      }

      ClusterRepairScheduleAssertion andThen() {
        return ClusterRepairScheduleAssertion.this;
      }
    }
  }
}
