/*
 *
 * Copyright 2019-2019 The Last Pickle Ltd
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
import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.core.Table;
import io.cassandrareaper.jmx.JmxConnectionFactory;
import io.cassandrareaper.jmx.JmxProxy;
import io.cassandrareaper.jmx.JmxProxyTest;

import java.net.UnknownHostException;
import java.util.Collection;

import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public final class RepairUnitServiceTest {

  private static final String STCS = "SizeTieredCompactionStrategy";
  private static final String TWCS = "TimeWindowCompactionStrategy";

  private AppContext context;
  private RepairUnitService service;

  private final Cluster cluster = Cluster.builder()
      .withName("testcluster_" + RandomStringUtils.randomAlphabetic(6))
      .withPartitioner("murmur3")
      .withSeedHosts(Sets.newHashSet("127.0.0.1"))
      .withJmxPort(7199)
      .build();

  @Before
  public void setUp() throws Exception {
    context = new AppContext();
    context.config = new ReaperApplicationConfiguration();
    context.config.setBlacklistTwcsTables(true);
    context.jmxConnectionFactory = mock(JmxConnectionFactory.class);
    service = RepairUnitService.create(context);
  }

  @Test
  public void getTablesToRepairRemoveOneTableTest() throws ReaperException, UnknownHostException {
    JmxProxy proxy = JmxProxyTest.mockJmxProxyImpl();
    when(proxy.getCassandraVersion()).thenReturn("3.11.4");
    when(context.jmxConnectionFactory.connectAny(Mockito.any(Collection.class))).thenReturn(proxy);

    when(proxy.getTablesForKeyspace(Mockito.anyString()))
            .thenReturn(Sets.newHashSet(
                    Table.builder().withName("table1").withCompactionStrategy(STCS).build(),
                    Table.builder().withName("table2").withCompactionStrategy(STCS).build(),
                    Table.builder().withName("table3").withCompactionStrategy(STCS).build()));

    RepairUnit unit = RepairUnit.builder()
        .clusterName(cluster.getName())
        .keyspaceName("test")
        .blacklistedTables(Sets.newHashSet("table1"))
        .incrementalRepair(false)
        .repairThreadCount(4)
        .build(UUIDs.timeBased());

    assertEquals(Sets.newHashSet("table2", "table3"), service.getTablesToRepair(cluster, unit));
  }

  @Test
  public void getTablesToRepairDefaultCompactionStrategyTable() throws ReaperException, UnknownHostException {
    JmxProxy proxy = JmxProxyTest.mockJmxProxyImpl();
    when(proxy.getCassandraVersion()).thenReturn("3.11.4");
    when(context.jmxConnectionFactory.connectAny(Mockito.any(Collection.class))).thenReturn(proxy);

    when(proxy.getTablesForKeyspace(Mockito.anyString()))
          .thenReturn(Sets.newHashSet(
                Table.builder().withName("table1").build(),
                Table.builder().withName("table2").build(),
                Table.builder().withName("table3").build()));

    RepairUnit unit = RepairUnit.builder()
          .clusterName(cluster.getName())
          .keyspaceName("test")
          .blacklistedTables(Sets.newHashSet("table1"))
          .incrementalRepair(false)
          .repairThreadCount(4)
          .build(UUIDs.timeBased());

    assertEquals(Sets.newHashSet("table2", "table3"), service.getTablesToRepair(cluster, unit));
  }

  @Test
  public void getTablesToRepairRemoveOneTableWithTwcsTest() throws ReaperException, UnknownHostException {
    JmxProxy proxy = JmxProxyTest.mockJmxProxyImpl();
    when(proxy.getCassandraVersion()).thenReturn("3.11.4");
    when(context.jmxConnectionFactory.connectAny(Mockito.any(Collection.class))).thenReturn(proxy);

    when(proxy.getTablesForKeyspace(Mockito.anyString()))
            .thenReturn(Sets.newHashSet(
                    Table.builder().withName("table1").withCompactionStrategy(TWCS).build(),
                    Table.builder().withName("table2").withCompactionStrategy(STCS).build(),
                    Table.builder().withName("table3").withCompactionStrategy(STCS).build()));

    RepairUnit unit = RepairUnit.builder()
        .clusterName(cluster.getName())
        .keyspaceName("test")
        .incrementalRepair(false)
        .repairThreadCount(4)
        .build(UUIDs.timeBased());

    assertEquals(Sets.newHashSet("table2", "table3"), service.getTablesToRepair(cluster, unit));
  }

  @Test
  public void getTablesToRepairRemoveTwoTablesTest() throws ReaperException, UnknownHostException {
    JmxProxy proxy = JmxProxyTest.mockJmxProxyImpl();
    when(proxy.getCassandraVersion()).thenReturn("3.11.4");
    when(context.jmxConnectionFactory.connectAny(Mockito.any(Collection.class))).thenReturn(proxy);

    when(proxy.getTablesForKeyspace(Mockito.anyString()))
            .thenReturn(Sets.newHashSet(
                    Table.builder().withName("table1").withCompactionStrategy(STCS).build(),
                    Table.builder().withName("table2").withCompactionStrategy(STCS).build(),
                    Table.builder().withName("table3").withCompactionStrategy(STCS).build()));

    RepairUnit unit = RepairUnit.builder()
        .clusterName(cluster.getName())
        .keyspaceName("test")
        .blacklistedTables(Sets.newHashSet("table1", "table3"))
        .incrementalRepair(false)
        .repairThreadCount(4)
        .build(UUIDs.timeBased());

    assertEquals(Sets.newHashSet("table2"), service.getTablesToRepair(cluster, unit));
  }

  @Test
  public void getTablesToRepairRemoveTwoTablesOneWithTwcsTest() throws ReaperException, UnknownHostException {
    JmxProxy proxy = JmxProxyTest.mockJmxProxyImpl();
    when(proxy.getCassandraVersion()).thenReturn("3.11.4");
    when(context.jmxConnectionFactory.connectAny(Mockito.any(Collection.class))).thenReturn(proxy);

    when(proxy.getTablesForKeyspace(Mockito.anyString()))
            .thenReturn(Sets.newHashSet(
                    Table.builder().withName("table1").withCompactionStrategy(STCS).build(),
                    Table.builder().withName("table2").withCompactionStrategy(STCS).build(),
                    Table.builder().withName("table3").withCompactionStrategy(TWCS).build()));

    RepairUnit unit = RepairUnit.builder()
        .clusterName(cluster.getName())
        .keyspaceName("test")
        .blacklistedTables(Sets.newHashSet("table1"))
        .incrementalRepair(false)
        .repairThreadCount(4)
        .build(UUIDs.timeBased());

    assertEquals(Sets.newHashSet("table2"), service.getTablesToRepair(cluster, unit));
  }

  @Test
  public void getTablesToRepairRemoveOneTableFromListTest() throws ReaperException, UnknownHostException {
    JmxProxy proxy = JmxProxyTest.mockJmxProxyImpl();
    when(proxy.getCassandraVersion()).thenReturn("3.11.4");
    when(context.jmxConnectionFactory.connectAny(Mockito.any(Collection.class))).thenReturn(proxy);

    when(proxy.getTablesForKeyspace(Mockito.anyString()))
            .thenReturn(Sets.newHashSet(
                    Table.builder().withName("table1").withCompactionStrategy(STCS).build(),
                    Table.builder().withName("table2").withCompactionStrategy(STCS).build(),
                    Table.builder().withName("table3").withCompactionStrategy(STCS).build()));

    RepairUnit unit = RepairUnit.builder()
        .clusterName(cluster.getName())
        .keyspaceName("test")
        .columnFamilies(Sets.newHashSet("table1", "table2"))
        .blacklistedTables(Sets.newHashSet("table1"))
        .incrementalRepair(false)
        .repairThreadCount(4)
        .build(UUIDs.timeBased());

    assertEquals(Sets.newHashSet("table2"), service.getTablesToRepair(cluster, unit));
  }

  @Test
  public void getTablesToRepairRemoveOneTableFromListOneWithTwcsTest() throws ReaperException, UnknownHostException {
    JmxProxy proxy = JmxProxyTest.mockJmxProxyImpl();
    when(proxy.getCassandraVersion()).thenReturn("3.11.4");
    when(context.jmxConnectionFactory.connectAny(Mockito.any(Collection.class))).thenReturn(proxy);

    when(proxy.getTablesForKeyspace(Mockito.anyString()))
            .thenReturn(Sets.newHashSet(
                    Table.builder().withName("table1").withCompactionStrategy(STCS).build(),
                    Table.builder().withName("table2").withCompactionStrategy(TWCS).build(),
                    Table.builder().withName("table3").withCompactionStrategy(STCS).build()));

    RepairUnit unit = RepairUnit.builder()
        .clusterName(cluster.getName())
        .keyspaceName("test")
        .columnFamilies(Sets.newHashSet("table1", "table2"))
        .blacklistedTables(Sets.newHashSet("table1"))
        .incrementalRepair(false)
        .repairThreadCount(4)
        .build(UUIDs.timeBased());

    assertEquals(Sets.newHashSet("table2"), service.getTablesToRepair(cluster, unit));
  }

  @Test(expected = IllegalStateException.class)
  public void getTablesToRepairRemoveAllFailingTest() throws ReaperException, UnknownHostException {
    JmxProxy proxy = JmxProxyTest.mockJmxProxyImpl();
    when(proxy.getCassandraVersion()).thenReturn("3.11.4");
    when(context.jmxConnectionFactory.connectAny(Mockito.any(Collection.class))).thenReturn(proxy);

    when(proxy.getTablesForKeyspace(Mockito.anyString()))
            .thenReturn(Sets.newHashSet(
                    Table.builder().withName("table1").withCompactionStrategy(STCS).build(),
                    Table.builder().withName("table2").withCompactionStrategy(STCS).build(),
                    Table.builder().withName("table3").withCompactionStrategy(STCS).build()));

    RepairUnit unit = RepairUnit.builder()
        .clusterName(cluster.getName())
        .keyspaceName("test")
        .blacklistedTables(Sets.newHashSet("table1", "table2", "table3"))
        .incrementalRepair(false)
        .repairThreadCount(4)
        .build(UUIDs.timeBased());

    service.getTablesToRepair(cluster, unit);
  }

  @Test(expected = IllegalStateException.class)
  public void getTablesToRepairRemoveAllFromListFailingTest() throws ReaperException, UnknownHostException {
    JmxProxy proxy = JmxProxyTest.mockJmxProxyImpl();
    when(proxy.getCassandraVersion()).thenReturn("3.11.4");
    when(context.jmxConnectionFactory.connectAny(Mockito.any(Collection.class))).thenReturn(proxy);

    when(proxy.getTablesForKeyspace(Mockito.anyString()))
        .thenReturn(Sets.newHashSet(
                Table.builder().withName("table1").withCompactionStrategy(STCS).build(),
                Table.builder().withName("table2").withCompactionStrategy(STCS).build(),
                Table.builder().withName("table3").withCompactionStrategy(STCS).build(),
                Table.builder().withName("table4").withCompactionStrategy(STCS).build()));

    RepairUnit unit = RepairUnit.builder()
        .clusterName(cluster.getName())
        .keyspaceName("test")
        .columnFamilies(Sets.newHashSet("table1", "table2", "table3"))
        .blacklistedTables(Sets.newHashSet("table1", "table2", "table3"))
        .incrementalRepair(false)
        .repairThreadCount(4)
        .build(UUIDs.timeBased());

    service.getTablesToRepair(cluster, unit);
  }
}
