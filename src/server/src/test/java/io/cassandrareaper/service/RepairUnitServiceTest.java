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
import io.cassandrareaper.core.Node;
import io.cassandrareaper.core.RepairSchedule;
import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.core.Table;
import io.cassandrareaper.management.ICassandraManagementProxy;
import io.cassandrareaper.management.jmx.CassandraManagementProxyTest;
import io.cassandrareaper.management.jmx.ClusterFacade;
import io.cassandrareaper.management.jmx.JmxManagementConnectionFactory;
import io.cassandrareaper.storage.IStorageDao;
import io.cassandrareaper.storage.repairschedule.IRepairScheduleDao;
import io.cassandrareaper.storage.repairunit.IRepairUnitDao;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;

import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.Sets;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.commons.lang3.RandomStringUtils;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
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
    IStorageDao storage = mock(IStorageDao.class);
    IRepairUnitDao mockedRepairUnitDao = mock(IRepairUnitDao.class);
    Mockito.when(storage.getRepairUnitDao()).thenReturn(mockedRepairUnitDao);
    when(mockedRepairUnitDao.getRepairUnit(any(RepairUnit.Builder.class))).thenReturn(Optional.empty());


    when(storage.getRepairUnitDao().addRepairUnit(any(RepairUnit.Builder.class))).thenReturn(mock(RepairUnit.class));
    context.storage = storage;
    context.managementConnectionFactory = mock(JmxManagementConnectionFactory.class);
    service = RepairUnitService.create(context);
  }

  @Test
  public void getTablesToRepairRemoveOneTableTest() throws ReaperException, UnknownHostException {
    ICassandraManagementProxy proxy = CassandraManagementProxyTest.mockJmxProxyImpl();
    when(proxy.getCassandraVersion()).thenReturn("3.11.4");
    when(context.managementConnectionFactory.connectAny(Mockito.any(Collection.class))).thenReturn(proxy);

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
        .timeout(30)
        .build(UUIDs.timeBased());

    assertEquals(Sets.newHashSet("table2", "table3"), service.getTablesToRepair(cluster, unit));
  }

  @Test
  public void getTablesToRepairDefaultCompactionStrategyTable() throws ReaperException, UnknownHostException {
    ICassandraManagementProxy proxy = CassandraManagementProxyTest.mockJmxProxyImpl();
    when(proxy.getCassandraVersion()).thenReturn("3.11.4");
    when(context.managementConnectionFactory.connectAny(Mockito.any(Collection.class))).thenReturn(proxy);

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
        .timeout(30)
        .build(UUIDs.timeBased());

    assertEquals(Sets.newHashSet("table2", "table3"), service.getTablesToRepair(cluster, unit));
  }

  @Test
  public void getTablesToRepairRemoveOneTableWithTwcsTest() throws ReaperException, UnknownHostException {
    ICassandraManagementProxy proxy = CassandraManagementProxyTest.mockJmxProxyImpl();
    when(proxy.getCassandraVersion()).thenReturn("3.11.4");
    when(context.managementConnectionFactory.connectAny(Mockito.any(Collection.class))).thenReturn(proxy);

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
        .timeout(30)
        .build(UUIDs.timeBased());

    assertEquals(Sets.newHashSet("table2", "table3"), service.getTablesToRepair(cluster, unit));
  }

  @Test
  public void getTablesToRepairRemoveTwoTablesTest() throws ReaperException, UnknownHostException {
    ICassandraManagementProxy proxy = CassandraManagementProxyTest.mockJmxProxyImpl();
    when(proxy.getCassandraVersion()).thenReturn("3.11.4");
    when(context.managementConnectionFactory.connectAny(Mockito.any(Collection.class))).thenReturn(proxy);

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
        .timeout(30)
        .build(UUIDs.timeBased());

    assertEquals(Sets.newHashSet("table2"), service.getTablesToRepair(cluster, unit));
  }

  @Test
  public void getTablesToRepairRemoveTwoTablesOneWithTwcsTest() throws ReaperException, UnknownHostException {
    ICassandraManagementProxy proxy = CassandraManagementProxyTest.mockJmxProxyImpl();
    when(proxy.getCassandraVersion()).thenReturn("3.11.4");
    when(context.managementConnectionFactory.connectAny(Mockito.any(Collection.class))).thenReturn(proxy);

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
        .timeout(30)
        .build(UUIDs.timeBased());

    assertEquals(Sets.newHashSet("table2"), service.getTablesToRepair(cluster, unit));
  }

  @Test
  public void getTablesToRepairRemoveOneTableFromListTest() throws ReaperException, UnknownHostException {
    ICassandraManagementProxy proxy = CassandraManagementProxyTest.mockJmxProxyImpl();
    when(proxy.getCassandraVersion()).thenReturn("3.11.4");
    when(context.managementConnectionFactory.connectAny(Mockito.any(Collection.class))).thenReturn(proxy);

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
        .timeout(30)
        .build(UUIDs.timeBased());

    assertEquals(Sets.newHashSet("table2"), service.getTablesToRepair(cluster, unit));
  }

  @Test
  public void getTablesToRepairRemoveOneTableFromListOneWithTwcsTest() throws ReaperException, UnknownHostException {
    ICassandraManagementProxy proxy = CassandraManagementProxyTest.mockJmxProxyImpl();
    when(proxy.getCassandraVersion()).thenReturn("3.11.4");
    when(context.managementConnectionFactory.connectAny(Mockito.any(Collection.class))).thenReturn(proxy);

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
        .timeout(30)
        .build(UUIDs.timeBased());

    assertEquals(Sets.newHashSet("table2"), service.getTablesToRepair(cluster, unit));
  }

  @Test(expected = IllegalStateException.class)
  public void getTablesToRepairRemoveAllFailingTest() throws ReaperException, UnknownHostException {
    ICassandraManagementProxy proxy = CassandraManagementProxyTest.mockJmxProxyImpl();
    when(proxy.getCassandraVersion()).thenReturn("3.11.4");
    when(context.managementConnectionFactory.connectAny(Mockito.any(Collection.class))).thenReturn(proxy);

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
        .timeout(30)
        .build(UUIDs.timeBased());

    service.getTablesToRepair(cluster, unit);
  }

  @Test(expected = IllegalStateException.class)
  public void getTablesToRepairRemoveAllFromListFailingTest() throws ReaperException, UnknownHostException {
    ICassandraManagementProxy proxy = CassandraManagementProxyTest.mockJmxProxyImpl();
    when(proxy.getCassandraVersion()).thenReturn("3.11.4");
    when(context.managementConnectionFactory.connectAny(Mockito.any(Collection.class))).thenReturn(proxy);

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
        .timeout(30)
        .build(UUIDs.timeBased());

    service.getTablesToRepair(cluster, unit);
  }

  @Test
  public void conflictingRepairUnitsTest() throws ReaperException, UnknownHostException {
    ICassandraManagementProxy proxy = CassandraManagementProxyTest.mockJmxProxyImpl();
    when(proxy.getCassandraVersion()).thenReturn("3.11.4");
    when(context.managementConnectionFactory.connectAny(Mockito.any(Collection.class))).thenReturn(proxy);

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
        .timeout(30)
        .build(UUIDs.timeBased());

    RepairUnit.Builder unitBuilder = RepairUnit.builder()
        .clusterName(cluster.getName())
        .keyspaceName("test")
        .columnFamilies(Sets.newHashSet("table1", "table2", "table3"))
        .blacklistedTables(Sets.newHashSet("table1", "table2", "table3"))
        .incrementalRepair(false)
        .repairThreadCount(4)
        .timeout(30);

    assertTrue("Units are not conflicting", service.identicalUnits(cluster, unit, unitBuilder));
  }

  @Test(expected = IllegalStateException.class)
  public void conflictingRepairUnitsDiffKSTest() throws ReaperException, UnknownHostException {
    ICassandraManagementProxy proxy = CassandraManagementProxyTest.mockJmxProxyImpl();
    when(proxy.getCassandraVersion()).thenReturn("3.11.4");
    when(context.managementConnectionFactory.connectAny(Mockito.any(Collection.class))).thenReturn(proxy);

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
        .timeout(30)
        .build(UUIDs.timeBased());

    RepairUnit.Builder unitBuilder = RepairUnit.builder()
        .clusterName(cluster.getName())
        .keyspaceName("test2")
        .columnFamilies(Sets.newHashSet("table1", "table2", "table3"))
        .blacklistedTables(Sets.newHashSet("table1", "table2", "table3"))
        .incrementalRepair(false)
        .repairThreadCount(4)
        .timeout(30);

    service.conflictingUnits(cluster, unit, unitBuilder);
  }

  @Test
  public void conflictingRepairUnitsNoTablesTest() throws ReaperException, UnknownHostException {
    ICassandraManagementProxy proxy = CassandraManagementProxyTest.mockJmxProxyImpl();
    when(proxy.getCassandraVersion()).thenReturn("3.11.4");
    when(context.managementConnectionFactory.connectAny(Mockito.any(Collection.class))).thenReturn(proxy);

    when(proxy.getTablesForKeyspace(Mockito.anyString()))
        .thenReturn(Sets.newHashSet(
            Table.builder().withName("table1").withCompactionStrategy(STCS).build(),
            Table.builder().withName("table2").withCompactionStrategy(STCS).build(),
            Table.builder().withName("table3").withCompactionStrategy(STCS).build(),
            Table.builder().withName("table4").withCompactionStrategy(STCS).build()));

    RepairUnit unit = RepairUnit.builder()
        .clusterName(cluster.getName())
        .keyspaceName("test")
        .incrementalRepair(false)
        .repairThreadCount(4)
        .timeout(30)
        .build(UUIDs.timeBased());

    RepairUnit.Builder unitBuilder = RepairUnit.builder()
        .clusterName(cluster.getName())
        .keyspaceName("test")
        .incrementalRepair(false)
        .repairThreadCount(3)
        .timeout(30);

    assertTrue("Units are not conflicting", service.conflictingUnits(cluster, unit, unitBuilder));
  }

  @Test
  public void notConflictingRepairUnitsTest() throws ReaperException, UnknownHostException {
    ICassandraManagementProxy proxy = CassandraManagementProxyTest.mockJmxProxyImpl();
    when(proxy.getCassandraVersion()).thenReturn("3.11.4");
    when(context.managementConnectionFactory.connectAny(Mockito.any(Collection.class))).thenReturn(proxy);

    when(proxy.getTablesForKeyspace(Mockito.anyString()))
        .thenReturn(Sets.newHashSet(
            Table.builder().withName("table1").withCompactionStrategy(STCS).build(),
            Table.builder().withName("table2").withCompactionStrategy(STCS).build(),
            Table.builder().withName("table3").withCompactionStrategy(STCS).build(),
            Table.builder().withName("table4").withCompactionStrategy(STCS).build()));

    RepairUnit unit = RepairUnit.builder()
        .clusterName(cluster.getName())
        .keyspaceName("test")
        .incrementalRepair(false)
        .repairThreadCount(4)
        .timeout(30)
        .build(UUIDs.timeBased());

    RepairUnit.Builder unitBuilder = RepairUnit.builder()
        .clusterName(cluster.getName())
        .keyspaceName("test")
        .incrementalRepair(false)
        .repairThreadCount(4)
        .timeout(30);

    assertTrue("Units are not conflicting", service.conflictingUnits(cluster, unit, unitBuilder));
  }

  @Test
  public void identicalRepairUnitsIncrFullTest() throws ReaperException, UnknownHostException {
    ICassandraManagementProxy proxy = CassandraManagementProxyTest.mockJmxProxyImpl();
    when(proxy.getCassandraVersion()).thenReturn("3.11.4");
    when(context.managementConnectionFactory.connectAny(Mockito.any(Collection.class))).thenReturn(proxy);

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
        .incrementalRepair(true)
        .repairThreadCount(4)
        .timeout(30)
        .build(UUIDs.timeBased());

    RepairUnit.Builder unitBuilder = RepairUnit.builder()
        .clusterName(cluster.getName())
        .keyspaceName("test")
        .columnFamilies(Sets.newHashSet("table1", "table2", "table3"))
        .blacklistedTables(Sets.newHashSet("table1", "table2", "table3"))
        .incrementalRepair(false)
        .repairThreadCount(4)
        .timeout(30);

    assertFalse("Units are identical", service.identicalUnits(cluster, unit, unitBuilder));
  }

  @Test
  public void identicalRepairUnitsDiffTablesTest() throws ReaperException, UnknownHostException {
    ICassandraManagementProxy proxy = CassandraManagementProxyTest.mockJmxProxyImpl();
    when(proxy.getCassandraVersion()).thenReturn("3.11.4");
    when(context.managementConnectionFactory.connectAny(Mockito.any(Collection.class))).thenReturn(proxy);

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
        .timeout(30)
        .build(UUIDs.timeBased());

    RepairUnit.Builder unitBuilder = RepairUnit.builder()
        .clusterName(cluster.getName())
        .keyspaceName("test")
        .columnFamilies(Sets.newHashSet("table1", "table2", "table4"))
        .blacklistedTables(Sets.newHashSet("table1", "table2", "table3"))
        .incrementalRepair(false)
        .repairThreadCount(4)
        .timeout(30);

    assertFalse("Units are identical", service.conflictingUnits(cluster, unit, unitBuilder));

    RepairUnit.Builder unitBuilder2 = RepairUnit.builder()
        .clusterName(cluster.getName())
        .keyspaceName("test")
        .blacklistedTables(Sets.newHashSet("table1", "table2", "table3"))
        .incrementalRepair(false)
        .repairThreadCount(4)
        .timeout(30);

    assertFalse("Units are identical", service.identicalUnits(cluster, unit, unitBuilder2));
  }

  @Test
  public void identicalRepairUnitsDiffNodesTest() throws ReaperException, UnknownHostException {
    ICassandraManagementProxy proxy = CassandraManagementProxyTest.mockJmxProxyImpl();
    when(proxy.getCassandraVersion()).thenReturn("3.11.4");
    when(context.managementConnectionFactory.connectAny(Mockito.any(Collection.class))).thenReturn(proxy);

    when(proxy.getTablesForKeyspace(Mockito.anyString()))
        .thenReturn(Sets.newHashSet(
            Table.builder().withName("table1").withCompactionStrategy(STCS).build(),
            Table.builder().withName("table2").withCompactionStrategy(STCS).build(),
            Table.builder().withName("table3").withCompactionStrategy(STCS).build(),
            Table.builder().withName("table4").withCompactionStrategy(STCS).build()));

    when(proxy.getLiveNodes()).thenReturn(Arrays.asList("node1", "node2"));

    RepairUnit unit = RepairUnit.builder()
        .clusterName(cluster.getName())
        .keyspaceName("test")
        .columnFamilies(Sets.newHashSet("table1", "table2", "table3"))
        .blacklistedTables(Sets.newHashSet("table1", "table2", "table3"))
        .nodes(Sets.newHashSet("node1"))
        .incrementalRepair(false)
        .repairThreadCount(4)
        .timeout(30)
        .build(UUIDs.timeBased());

    RepairUnit.Builder unitBuilder = RepairUnit.builder()
        .clusterName(cluster.getName())
        .keyspaceName("test")
        .columnFamilies(Sets.newHashSet("table1", "table2", "table3"))
        .blacklistedTables(Sets.newHashSet("table1", "table2", "table3"))
        .nodes(Sets.newHashSet("node2"))
        .incrementalRepair(false)
        .repairThreadCount(4)
        .timeout(30);

    assertFalse("Units are identical", service.identicalUnits(cluster, unit, unitBuilder));
  }

  @Test
  public void conflictingRepairUnitsSameDcsTest() throws ReaperException, UnknownHostException, InterruptedException {
    ICassandraManagementProxy proxy = CassandraManagementProxyTest.mockJmxProxyImpl();
    ClusterFacade clusterFacade = mock(ClusterFacade.class);
    when(proxy.getCassandraVersion()).thenReturn("3.11.4");
    when(context.managementConnectionFactory.connectAny(Mockito.any(Collection.class))).thenReturn(proxy);
    when(clusterFacade.getDatacenter(any())).thenReturn("dc1");
    RepairUnitService repairUnitService = RepairUnitService.create(context, () -> clusterFacade);

    when(proxy.getLiveNodes()).thenReturn(Arrays.asList("node1", "node2"));

    RepairUnit unit = RepairUnit.builder()
        .clusterName(cluster.getName())
        .keyspaceName("test")
        .columnFamilies(Sets.newHashSet("table1", "table2", "table3"))
        .blacklistedTables(Sets.newHashSet("table1", "table2", "table3"))
        .datacenters(Sets.newHashSet("dc1"))
        .nodes(Sets.newHashSet("node1"))
        .incrementalRepair(false)
        .repairThreadCount(4)
        .timeout(30)
        .build(UUIDs.timeBased());

    RepairUnit.Builder unitBuilder = RepairUnit.builder()
        .clusterName(cluster.getName())
        .keyspaceName("test")
        .columnFamilies(Sets.newHashSet("table1", "table2", "table3"))
        .blacklistedTables(Sets.newHashSet("table1", "table2", "table3"))
        .nodes(Sets.newHashSet("node1"))
        .incrementalRepair(false)
        .repairThreadCount(4)
        .timeout(30);

    assertTrue("Units are not identical", repairUnitService.identicalUnits(cluster, unit, unitBuilder));
  }

  @Test
  public void identicalRepairUnitsDifferentDcsTest()
      throws ReaperException, UnknownHostException, InterruptedException {
    ICassandraManagementProxy proxy = CassandraManagementProxyTest.mockJmxProxyImpl();
    ClusterFacade clusterFacade = mock(ClusterFacade.class);
    when(proxy.getCassandraVersion()).thenReturn("3.11.4");
    when(context.managementConnectionFactory.connectAny(Mockito.any(Collection.class))).thenReturn(proxy);
    when(clusterFacade.getDatacenter(any())).thenReturn("dc1");

    when(proxy.getTablesForKeyspace(Mockito.anyString()))
        .thenReturn(Sets.newHashSet(
            Table.builder().withName("table1").withCompactionStrategy(STCS).build(),
            Table.builder().withName("table2").withCompactionStrategy(STCS).build(),
            Table.builder().withName("table3").withCompactionStrategy(STCS).build(),
            Table.builder().withName("table4").withCompactionStrategy(STCS).build()));

    when(proxy.getLiveNodes()).thenReturn(Arrays.asList("node1", "node2"));

    RepairUnit unit = RepairUnit.builder()
        .clusterName(cluster.getName())
        .keyspaceName("test")
        .columnFamilies(Sets.newHashSet("table1", "table2", "table3"))
        .blacklistedTables(Sets.newHashSet("table1", "table2", "table3"))
        .datacenters(Sets.newHashSet("dc1"))
        .nodes(Sets.newHashSet("node1"))
        .incrementalRepair(false)
        .repairThreadCount(4)
        .timeout(30)
        .build(UUIDs.timeBased());

    RepairUnit.Builder unitBuilder = RepairUnit.builder()
        .clusterName(cluster.getName())
        .keyspaceName("test")
        .columnFamilies(Sets.newHashSet("table1", "table2", "table3"))
        .blacklistedTables(Sets.newHashSet("table1", "table2", "table3"))
        .datacenters(Sets.newHashSet("dc2"))
        .nodes(Sets.newHashSet("node1"))
        .incrementalRepair(false)
        .repairThreadCount(4)
        .timeout(30);

    assertFalse("Units are identical", service.identicalUnits(cluster, unit, unitBuilder));
  }

  @Test
  public void identicalRepairUnitsNonExistentNodesTest()
      throws ReaperException, UnknownHostException, InterruptedException {
    ICassandraManagementProxy proxy = CassandraManagementProxyTest.mockJmxProxyImpl();
    ClusterFacade clusterFacade = mock(ClusterFacade.class);
    when(proxy.getCassandraVersion()).thenReturn("3.11.4");
    when(context.managementConnectionFactory.connectAny(Mockito.any(Collection.class))).thenReturn(proxy);
    when(clusterFacade.getDatacenter(any(Node.class))).thenThrow(new ReaperException("fake exception"));
    when(clusterFacade.getLiveNodes(any())).thenReturn(Arrays.asList("node1", "node2"));
    RepairUnitService repairUnitService = RepairUnitService.create(context, () -> clusterFacade);

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
        .timeout(30)
        .build(UUIDs.timeBased());

    RepairUnit.Builder unitBuilder = RepairUnit.builder()
        .clusterName(cluster.getName())
        .keyspaceName("test")
        .columnFamilies(Sets.newHashSet("table1", "table2", "table3"))
        .blacklistedTables(Sets.newHashSet("table1", "table2", "table3"))
        .incrementalRepair(false)
        .repairThreadCount(3)
        .timeout(30);

    assertTrue("Units are not identical", repairUnitService.identicalUnits(cluster, unit, unitBuilder));
  }

  @Test()
  public void identicalRepairUnitsFailGetDcsTest() throws ReaperException, UnknownHostException {
    ICassandraManagementProxy proxy = CassandraManagementProxyTest.mockJmxProxyImpl();
    when(proxy.getCassandraVersion()).thenReturn("3.11.4");
    when(context.managementConnectionFactory.connectAny(Mockito.any(Collection.class))).thenReturn(proxy);

    when(proxy.getTablesForKeyspace(Mockito.anyString()))
        .thenReturn(Sets.newHashSet(
            Table.builder().withName("table1").withCompactionStrategy(STCS).build(),
            Table.builder().withName("table2").withCompactionStrategy(STCS).build(),
            Table.builder().withName("table3").withCompactionStrategy(STCS).build(),
            Table.builder().withName("table4").withCompactionStrategy(STCS).build()));

    when(proxy.getLiveNodes()).thenReturn(Arrays.asList("node1", "node2"));

    RepairUnit unit = RepairUnit.builder()
        .clusterName(cluster.getName())
        .keyspaceName("test")
        .columnFamilies(Sets.newHashSet("table1", "table2", "table3"))
        .blacklistedTables(Sets.newHashSet("table1", "table2", "table3"))
        .nodes(Sets.newHashSet("node1"))
        .incrementalRepair(false)
        .repairThreadCount(4)
        .timeout(30)
        .build(UUIDs.timeBased());

    RepairUnit.Builder unitBuilder = RepairUnit.builder()
        .clusterName(cluster.getName())
        .keyspaceName("test")
        .columnFamilies(Sets.newHashSet("table1", "table2", "table3"))
        .blacklistedTables(Sets.newHashSet("table1", "table2", "table3"))
        .nodes(Sets.newHashSet("node3"))
        .incrementalRepair(false)
        .repairThreadCount(4)
        .timeout(30);

    assertFalse("Units are identical", service.identicalUnits(cluster, unit, unitBuilder));
  }

  @Test
  public void unknownTablesTest() throws ReaperException, UnknownHostException {
    ICassandraManagementProxy proxy = CassandraManagementProxyTest.mockJmxProxyImpl();
    when(proxy.getCassandraVersion()).thenReturn("3.11.4");
    when(context.managementConnectionFactory.connectAny(Mockito.any(Collection.class))).thenReturn(proxy);

    when(proxy.getTablesForKeyspace(Mockito.anyString()))
        .thenThrow(new ReaperException("Fake failure"));

    when(proxy.getLiveNodes()).thenReturn(Arrays.asList("node1", "node2"));

    RepairUnit unit = RepairUnit.builder()
        .clusterName(cluster.getName())
        .keyspaceName("test")
        .incrementalRepair(false)
        .repairThreadCount(4)
        .timeout(30)
        .build(UUIDs.timeBased());

    RepairUnit.Builder unitBuilder = RepairUnit.builder()
        .clusterName(cluster.getName())
        .keyspaceName("test")
        .incrementalRepair(false)
        .repairThreadCount(3)
        .timeout(30);

    assertTrue("Units are not identical", service.identicalUnits(cluster, unit, unitBuilder));
  }

  @Test
  public void missingLiveNodesTest()
      throws ReaperException, UnknownHostException, InterruptedException {
    ICassandraManagementProxy proxy = CassandraManagementProxyTest.mockJmxProxyImpl();
    ClusterFacade clusterFacade = mock(ClusterFacade.class);
    when(proxy.getCassandraVersion()).thenReturn("3.11.4");
    when(context.managementConnectionFactory.connectAny(Mockito.any(Collection.class))).thenReturn(proxy);
    when(clusterFacade.getDatacenter(any(Node.class))).thenThrow(new ReaperException("fake exception"));
    when(clusterFacade.getLiveNodes(any())).thenThrow(new ReaperException("ouch"));
    RepairUnitService repairUnitService = RepairUnitService.create(context, () -> clusterFacade);

    RepairUnit unit = RepairUnit.builder()
        .clusterName(cluster.getName())
        .keyspaceName("test")
        .columnFamilies(Sets.newHashSet("table1", "table2", "table3"))
        .blacklistedTables(Sets.newHashSet("table1", "table2", "table3"))
        .incrementalRepair(false)
        .repairThreadCount(4)
        .timeout(30)
        .build(UUIDs.timeBased());

    RepairUnit.Builder unitBuilder = RepairUnit.builder()
        .clusterName(cluster.getName())
        .keyspaceName("test")
        .columnFamilies(Sets.newHashSet("table1", "table2", "table3"))
        .blacklistedTables(Sets.newHashSet("table1", "table2", "table3"))
        .incrementalRepair(false)
        .repairThreadCount(3)
        .timeout(30);

    assertTrue("Units are not identical", repairUnitService.identicalUnits(cluster, unit, unitBuilder));
  }

  @Test(expected = IllegalArgumentException.class)
  public void createRepairUnitIncrPrior21Test()
      throws ReaperException, UnknownHostException, InterruptedException {
    ClusterFacade clusterFacade = mock(ClusterFacade.class);
    when(clusterFacade.getCassandraVersion(any())).thenReturn("2.0");
    RepairUnitService repairUnitService = RepairUnitService.create(context, () -> clusterFacade);

    RepairUnit.Builder unitBuilder = RepairUnit.builder()
        .clusterName(cluster.getName())
        .keyspaceName("test")
        .columnFamilies(Sets.newHashSet("table1", "table2", "table3"))
        .blacklistedTables(Sets.newHashSet("table1", "table2", "table3"))
        .incrementalRepair(true)
        .repairThreadCount(4)
        .timeout(30);

    repairUnitService.getOrCreateRepairUnit(cluster, unitBuilder);
  }

  @Test
  public void createRepairUnitIncrUnknownVersionTest()
      throws ReaperException, UnknownHostException, InterruptedException {
    ClusterFacade clusterFacade = mock(ClusterFacade.class);
    when(clusterFacade.getCassandraVersion(any())).thenThrow(new ReaperException("ouch"));

    IRepairScheduleDao mockedRepairScheduleDao = Mockito.mock(IRepairScheduleDao.class);
    Mockito.when(context.storage.getRepairScheduleDao()).thenReturn(mockedRepairScheduleDao);

    RepairUnitService repairUnitService = RepairUnitService.create(context, () -> clusterFacade);

    RepairUnit.Builder unitBuilder = RepairUnit.builder()
        .clusterName(cluster.getName())
        .keyspaceName("test")
        .columnFamilies(Sets.newHashSet("table1", "table2", "table3"))
        .blacklistedTables(Sets.newHashSet("table1", "table2", "table3"))
        .incrementalRepair(true)
        .repairThreadCount(4)
        .timeout(30);

    repairUnitService.getOrCreateRepairUnit(cluster, unitBuilder);
  }

  @Test
  public void unitScheduleConflictsTest()
      throws ReaperException, UnknownHostException, InterruptedException {
    AppContext localContext = new AppContext();
    localContext.config = new ReaperApplicationConfiguration();
    localContext.config.setBlacklistTwcsTables(true);
    IStorageDao storage = mock(IStorageDao.class);
    IRepairUnitDao mockedRepairUnitDao = mock(IRepairUnitDao.class);
    Mockito.when(storage.getRepairUnitDao()).thenReturn(mockedRepairUnitDao);
    when(mockedRepairUnitDao.getRepairUnit(any(RepairUnit.Builder.class))).thenReturn(Optional.empty());


    localContext.storage = storage;
    localContext.managementConnectionFactory = mock(JmxManagementConnectionFactory.class);
    ClusterFacade clusterFacade = mock(ClusterFacade.class);
    when(clusterFacade.getCassandraVersion(any())).thenThrow(new ReaperException("ouch"));

    RepairUnit.Builder unitBuilder = RepairUnit.builder()
        .clusterName(cluster.getName())
        .keyspaceName("test")
        .columnFamilies(Sets.newHashSet("table1", "table2", "table3"))
        .blacklistedTables(Sets.newHashSet("table1", "table2", "table3"))
        .incrementalRepair(true)
        .repairThreadCount(4)
        .timeout(30);

    RepairUnit repairUnit = unitBuilder.build(UUIDs.timeBased());
    when(mockedRepairUnitDao.getRepairUnit(any(UUID.class))).thenReturn(repairUnit);

    RepairSchedule repairSchedule = RepairSchedule.builder(repairUnit.getId())
        .daysBetween(1)
        .nextActivation(DateTime.now())
        .repairParallelism(RepairParallelism.PARALLEL)
        .intensity(1)
        .segmentCountPerNode(10)
        .build(UUIDs.timeBased());

    IRepairScheduleDao mockedRepairScheduleDao = Mockito.mock(IRepairScheduleDao.class);
    Mockito.when(localContext.storage.getRepairScheduleDao()).thenReturn(mockedRepairScheduleDao);
    when(mockedRepairScheduleDao.getRepairSchedulesForClusterAndKeyspace(any(), any()))
        .thenReturn(Arrays.asList(repairSchedule));
    RepairUnitService repairUnitService = RepairUnitService.create(localContext, () -> clusterFacade);
    assertTrue("Unit is not conflicting with existing schedules",
        repairUnitService.unitConflicts(cluster, unitBuilder));
  }

  @Test
  public void findBlacklistedCompactionStrategyTablesTest()
      throws ReaperException, UnknownHostException, InterruptedException {
    AppContext localContext = new AppContext();
    localContext.config = new ReaperApplicationConfiguration();
    localContext.config.setBlacklistTwcsTables(false);
    ClusterFacade clusterFacade = mock(ClusterFacade.class);
    when(clusterFacade.getCassandraVersion(any())).thenReturn("2.0");
    RepairUnitService repairUnitService = RepairUnitService.create(context, () -> clusterFacade);

    assertTrue("Blacklisted tables list isn't empty",
        repairUnitService.findBlacklistedCompactionStrategyTables(cluster, Collections.emptySet()).isEmpty());
  }
}