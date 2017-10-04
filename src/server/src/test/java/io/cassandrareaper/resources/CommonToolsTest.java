/*
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

package io.cassandrareaper.resources;

import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.jmx.JmxProxy;
import io.cassandrareaper.service.RingRange;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import jersey.repackaged.com.google.common.collect.Maps;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class CommonToolsTest {

  @Test
  public void testDateTimeToISO8601() {
    DateTime dateTime = new DateTime(2015, 2, 20, 15, 24, 45, DateTimeZone.UTC);
    assertEquals("2015-02-20T15:24:45Z", CommonTools.dateTimeToIso8601(dateTime));
  }

  @Test
  public void testParseSeedHost() {
    String seedHostStringList = "127.0.0.1 , 127.0.0.2,  127.0.0.3";
    Set<String> seedHostSet = CommonTools.parseSeedHosts(seedHostStringList);
    Set<String> seedHostExpectedSet = Sets.newHashSet("127.0.0.2", "127.0.0.1", "127.0.0.3");

    assertEquals(seedHostSet, seedHostExpectedSet);
  }

  @Test
  public void buildEndpointToRangeMapTest() {
    Map<List<String>, List<String>> rangeToEndpoint = Maps.newHashMap();
    rangeToEndpoint.put(Arrays.asList("1", "2"), Arrays.asList("node1", "node2", "node3"));
    rangeToEndpoint.put(Arrays.asList("2", "4"), Arrays.asList("node1", "node2", "node3"));
    rangeToEndpoint.put(Arrays.asList("4", "6"), Arrays.asList("node1"));
    rangeToEndpoint.put(Arrays.asList("6", "8"), Arrays.asList("node1", "node2"));

    Map<String, List<RingRange>> endpointToRangeMap = CommonTools.buildEndpointToRangeMap(rangeToEndpoint);

    assertEquals(endpointToRangeMap.entrySet().size(), 3);
    assertEquals(endpointToRangeMap.get("node1").size(), 4);
    assertEquals(endpointToRangeMap.get("node2").size(), 3);
    assertEquals(endpointToRangeMap.get("node3").size(), 2);
  }

  @Test
  public void filterSegmentsByNodesTest() throws ReaperException {
    Map<List<String>, List<String>> rangeToEndpoint = Maps.newHashMap();
    rangeToEndpoint.put(Arrays.asList("1", "2"), Arrays.asList("node1", "node2", "node3"));
    rangeToEndpoint.put(Arrays.asList("2", "4"), Arrays.asList("node1", "node2", "node3"));
    rangeToEndpoint.put(Arrays.asList("4", "6"), Arrays.asList("node1"));
    rangeToEndpoint.put(Arrays.asList("6", "8"), Arrays.asList("node1", "node2"));

    List<RingRange> segments = Arrays.asList(new RingRange("1", "2"), new RingRange("2", "3"), new RingRange("3", "4"),
        new RingRange("4", "5"), new RingRange("5", "6"), new RingRange("6", "8"));

    final RepairUnit repairUnit1 = mock(RepairUnit.class);
    when(repairUnit1.getNodes()).thenReturn(new HashSet<String>(Arrays.asList("node3", "node2")));

    final RepairUnit repairUnit2 = mock(RepairUnit.class);
    when(repairUnit2.getNodes()).thenReturn(new HashSet<String>(Arrays.asList("node1")));

    final RepairUnit repairUnit3 = mock(RepairUnit.class);
    when(repairUnit3.getNodes()).thenReturn(new HashSet<String>(Arrays.asList("node3")));

    List<RingRange> filtered = CommonTools.filterSegmentsByNodes(segments, repairUnit1,
        CommonTools.buildEndpointToRangeMap(rangeToEndpoint));
    assertEquals(filtered.size(), 4);

    filtered = CommonTools.filterSegmentsByNodes(segments, repairUnit2,
        CommonTools.buildEndpointToRangeMap(rangeToEndpoint));
    assertEquals(filtered.size(), 6);

    filtered = CommonTools.filterSegmentsByNodes(segments, repairUnit3,
        CommonTools.buildEndpointToRangeMap(rangeToEndpoint));
    assertEquals(filtered.size(), 3);

    final RepairUnit repairUnitWithNoNodes = mock(RepairUnit.class);
    when(repairUnitWithNoNodes.getNodes()).thenReturn(new HashSet<String>());

    filtered = CommonTools.filterSegmentsByNodes(segments, repairUnitWithNoNodes,
        CommonTools.buildEndpointToRangeMap(rangeToEndpoint));
    assertEquals(filtered.size(), 6);

  }

  @Test
  public void getTablesToRepairRemoveOneTableTest() throws ReaperException {
    JmxProxy coord = mock(JmxProxy.class);
    when(coord.getTableNamesForKeyspace(Mockito.anyString()))
        .thenReturn(Sets.newHashSet("table1", "table2", "table3"));


    RepairUnit unit = mock(RepairUnit.class);
    when(unit.getBlacklistedTables()).thenReturn(Sets.newHashSet("table1"));
    when(unit.getColumnFamilies()).thenReturn(Sets.newHashSet());
    when(unit.getKeyspaceName()).thenReturn("test");

    assertEquals(Sets.newHashSet("table2", "table3"), CommonTools.getTablesToRepair(coord, unit));
  }

  @Test
  public void getTablesToRepairRemoveTwoTablesTest() throws ReaperException {
    JmxProxy coord = mock(JmxProxy.class);
    when(coord.getTableNamesForKeyspace(Mockito.anyString()))
        .thenReturn(Sets.newHashSet("table1", "table2", "table3"));

    RepairUnit unit = mock(RepairUnit.class);
    when(unit.getBlacklistedTables()).thenReturn(Sets.newHashSet("table1", "table3"));
    when(unit.getColumnFamilies()).thenReturn(Sets.newHashSet());
    when(unit.getKeyspaceName()).thenReturn("test");

    assertEquals(Sets.newHashSet("table2"), CommonTools.getTablesToRepair(coord, unit));
  }

  @Test
  public void getTablesToRepairRemoveOneTableFromListTest() throws ReaperException {
    JmxProxy coord = mock(JmxProxy.class);
    when(coord.getTableNamesForKeyspace(Mockito.anyString()))
        .thenReturn(Sets.newHashSet("table1", "table2", "table3"));

    RepairUnit unit = mock(RepairUnit.class);
    when(unit.getBlacklistedTables()).thenReturn(Sets.newHashSet("table1"));
    when(unit.getColumnFamilies()).thenReturn(Sets.newHashSet("table1", "table2"));
    when(unit.getKeyspaceName()).thenReturn("test");

    assertEquals(Sets.newHashSet("table2"), CommonTools.getTablesToRepair(coord, unit));
  }

  @Test(expected = IllegalStateException.class)
  public void getTablesToRepairRemoveAllFailingTest() throws ReaperException {
    JmxProxy coord = mock(JmxProxy.class);
    when(coord.getTableNamesForKeyspace(Mockito.anyString()))
        .thenReturn(Sets.newHashSet("table1", "table2", "table3"));

    RepairUnit unit = mock(RepairUnit.class);
    when(unit.getBlacklistedTables()).thenReturn(Sets.newHashSet("table1", "table2", "table3"));
    when(unit.getColumnFamilies()).thenReturn(Sets.newHashSet());
    when(unit.getKeyspaceName()).thenReturn("test");

    CommonTools.getTablesToRepair(coord, unit);
  }

  @Test(expected = IllegalStateException.class)
  public void getTablesToRepairRemoveAllFromListFailingTest() throws ReaperException {
    JmxProxy coord = mock(JmxProxy.class);
    when(coord.getTableNamesForKeyspace(Mockito.anyString()))
        .thenReturn(Sets.newHashSet("table1", "table2", "table3", "table4"));

    RepairUnit unit = mock(RepairUnit.class);
    when(unit.getBlacklistedTables()).thenReturn(Sets.newHashSet("table1", "table2", "table3"));
    when(unit.getColumnFamilies()).thenReturn(Sets.newHashSet("table1", "table2", "table3"));
    when(unit.getKeyspaceName()).thenReturn("test");

    CommonTools.getTablesToRepair(coord, unit);
  }
}
