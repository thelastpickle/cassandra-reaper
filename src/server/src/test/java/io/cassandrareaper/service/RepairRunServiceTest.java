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

package io.cassandrareaper.service;

import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.core.Segment;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class RepairRunServiceTest {

  @Test
  public void buildEndpointToRangeMapTest() {
    Map<List<String>, List<String>> rangeToEndpoint = Maps.newHashMap();
    rangeToEndpoint.put(Arrays.asList("1", "2"), Arrays.asList("node1", "node2", "node3"));
    rangeToEndpoint.put(Arrays.asList("2", "4"), Arrays.asList("node1", "node2", "node3"));
    rangeToEndpoint.put(Arrays.asList("4", "6"), Arrays.asList("node1"));
    rangeToEndpoint.put(Arrays.asList("6", "8"), Arrays.asList("node1", "node2"));

    Map<String, List<RingRange>> endpointToRangeMap = RepairRunService.buildEndpointToRangeMap(rangeToEndpoint);

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

    List<Segment> segments =
        Arrays.asList(
            Segment.builder().withTokenRange(new RingRange("1", "2")).build(),
            Segment.builder().withTokenRange(new RingRange("2", "3")).build(),
            Segment.builder().withTokenRange(new RingRange("3", "4")).build(),
            Segment.builder().withTokenRange(new RingRange("4", "5")).build(),
            Segment.builder().withTokenRange(new RingRange("5", "6")).build(),
            Segment.builder().withTokenRange(new RingRange("6", "8")).build());

    final RepairUnit repairUnit1 = mock(RepairUnit.class);
    when(repairUnit1.getNodes()).thenReturn(new HashSet<String>(Arrays.asList("node3", "node2")));

    final RepairUnit repairUnit2 = mock(RepairUnit.class);
    when(repairUnit2.getNodes()).thenReturn(new HashSet<String>(Arrays.asList("node1")));

    final RepairUnit repairUnit3 = mock(RepairUnit.class);
    when(repairUnit3.getNodes()).thenReturn(new HashSet<String>(Arrays.asList("node3")));

    List<Segment> filtered =
        RepairRunService.filterSegmentsByNodes(
            segments, repairUnit1, RepairRunService.buildEndpointToRangeMap(rangeToEndpoint));
    assertEquals(filtered.size(), 4);

    filtered = RepairRunService.filterSegmentsByNodes(segments, repairUnit2,
        RepairRunService.buildEndpointToRangeMap(rangeToEndpoint));
    assertEquals(filtered.size(), 6);

    filtered = RepairRunService.filterSegmentsByNodes(segments, repairUnit3,
        RepairRunService.buildEndpointToRangeMap(rangeToEndpoint));
    assertEquals(filtered.size(), 3);

    final RepairUnit repairUnitWithNoNodes = mock(RepairUnit.class);
    when(repairUnitWithNoNodes.getNodes()).thenReturn(new HashSet<String>());

    filtered = RepairRunService.filterSegmentsByNodes(segments, repairUnitWithNoNodes,
        RepairRunService.buildEndpointToRangeMap(rangeToEndpoint));
    assertEquals(filtered.size(), 6);

  }

  @Test
  public void computeGlobalSegmentCountSubdivisionOkTest() {

    Map<List<String>, List<String>> rangeToEndpoint = Maps.newHashMap();
    for (int i = 0; i < 10; i++) {
      rangeToEndpoint.put(
          Arrays.asList(i + "", (i + 1) + ""), Arrays.asList("node1", "node2", "node3"));
    }

    Map<String, List<RingRange>> endpointToRange = Maps.newHashMap();
    endpointToRange.put("node1", Lists.newArrayList());
    endpointToRange.put("node2", Lists.newArrayList());
    endpointToRange.put("node3", Lists.newArrayList());

    assertEquals(30, RepairRunService.computeGlobalSegmentCount(10, endpointToRange));
  }

  @Test
  public void computeGlobalSegmentCountSubdivisionNotOkTest() {

    Map<List<String>, List<String>> rangeToEndpoint = Maps.newHashMap();
    for (int i = 0; i < 60; i++) {
      rangeToEndpoint.put(
          Arrays.asList(i + "", (i + 1) + ""), Arrays.asList("node1", "node2", "node3"));
    }

    Map<String, List<RingRange>> endpointToRange = Maps.newHashMap();
    endpointToRange.put("node1", Lists.newArrayList());
    endpointToRange.put("node2", Lists.newArrayList());
    endpointToRange.put("node3", Lists.newArrayList());

    assertEquals(30, RepairRunService.computeGlobalSegmentCount(10, endpointToRange));
  }

  @Test
  public void computeGlobalSegmentCountSingleTokenPerNodeTest() {

    Map<List<String>, List<String>> rangeToEndpoint = Maps.newHashMap();
    for (int i = 0; i < 3; i++) {
      rangeToEndpoint.put(
          Arrays.asList(i + "", (i + 1) + ""), Arrays.asList("node1", "node2", "node3"));
    }

    Map<String, List<RingRange>> endpointToRange = Maps.newHashMap();
    endpointToRange.put("node1", Lists.newArrayList());
    endpointToRange.put("node2", Lists.newArrayList());
    endpointToRange.put("node3", Lists.newArrayList());

    assertEquals(48, RepairRunService.computeGlobalSegmentCount(0, endpointToRange));
  }

  @Test
  public void computeGlobalSegmentCount256TokenPerNodeTest() {

    Map<List<String>, List<String>> rangeToEndpoint = Maps.newHashMap();
    for (int i = 0; i < 768; i++) {
      rangeToEndpoint.put(
          Arrays.asList(i + "", (i + 1) + ""), Arrays.asList("node1", "node2", "node3"));
    }

    Map<String, List<RingRange>> endpointToRange = Maps.newHashMap();
    endpointToRange.put("node1", Lists.newArrayList());
    endpointToRange.put("node2", Lists.newArrayList());
    endpointToRange.put("node3", Lists.newArrayList());

    assertEquals(48, RepairRunService.computeGlobalSegmentCount(0, endpointToRange));
  }

  @Test
  public void buildReplicasToRangeMapTest() {
    Map<List<String>, List<String>> rangeToEndpoint = Maps.newHashMap();
    rangeToEndpoint.put(Arrays.asList("1", "2"), Arrays.asList("node1", "node2", "node3"));
    rangeToEndpoint.put(Arrays.asList("2", "4"), Arrays.asList("node1", "node2", "node3"));
    rangeToEndpoint.put(Arrays.asList("4", "6"), Arrays.asList("node1"));
    rangeToEndpoint.put(Arrays.asList("6", "8"), Arrays.asList("node1", "node2"));
    rangeToEndpoint.put(Arrays.asList("9", "10"), Arrays.asList("node1", "node2"));
    rangeToEndpoint.put(Arrays.asList("11", "12"), Arrays.asList("node2", "node3", "node1"));

    Map<List<String>, List<RingRange>> replicasToRangeMap =
        RepairRunService.buildReplicasToRangeMap(rangeToEndpoint);

    assertEquals(replicasToRangeMap.entrySet().size(), 3);
    assertEquals(replicasToRangeMap.get(Arrays.asList("node1", "node2", "node3")).size(), 3);
    assertEquals(replicasToRangeMap.get(Arrays.asList("node1")).size(), 1);
    assertEquals(replicasToRangeMap.get(Arrays.asList("node1", "node2")).size(), 2);
  }
}
