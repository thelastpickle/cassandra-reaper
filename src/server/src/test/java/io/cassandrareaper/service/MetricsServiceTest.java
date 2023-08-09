/*
 * Copyright 2018-2019 The Last Pickle Ltd
 *
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
import io.cassandrareaper.core.*;
import io.cassandrareaper.management.ClusterFacade;
import io.cassandrareaper.management.JmxMetricsProxy;
import io.cassandrareaper.management.jmx.JmxCassandraManagementProxy;
import io.cassandrareaper.management.jmx.JmxManagementConnectionFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.management.JMException;
import javax.management.ObjectName;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MetricsServiceTest {

  @Test
  public void testGetTpstats()
      throws InterruptedException, ReaperException, JMException, IOException, ClassNotFoundException {

    AppContext cxt = new AppContext();
    ClusterFacade clusterFacade = Mockito.mock(ClusterFacade.class);
    Mockito.doReturn(Collections.singletonList("")).when(clusterFacade).getTpStats(any());
    cxt.config = new ReaperApplicationConfiguration();
    cxt.config.setJmxConnectionTimeoutInSeconds(10);
    cxt.managementConnectionFactory = mock(JmxManagementConnectionFactory.class);
    JmxCassandraManagementProxy jmx = (JmxCassandraManagementProxy) mock(
        Class.forName("io.cassandrareaper.management.jmx.JmxCassandraManagementProxy"));
    when(((JmxManagementConnectionFactory) cxt.managementConnectionFactory).connectAny(any(Collection.class))).thenReturn(jmx);

    // @todo capture objectName and return valid set of objectNames,
    // to properly test MetricsProxy.collectMetrics(..) and MetricsService.convertToThreadPoolStats(..)
    when(jmx.queryNames(Mockito.any(ObjectName.class), Mockito.isNull())).thenReturn(Collections.emptySet());

    Node node = Node.builder().withHostname("127.0.0.1").build();
    MetricsService.create(cxt, () -> clusterFacade).getTpStats(node);
    Mockito.verify(clusterFacade, Mockito.times(1)).getTpStats(Mockito.any());
  }

  @Test
  public void testConvertToThreadPoolStats() {
    List<JmxStat> statList = Lists.newArrayList();
    statList.add(
        JmxStat.builder()
            .withScope("ReadStage")
            .withName("PendingTasks")
            .withAttribute("Value")
            .withValue(1.0)
            .build());
    statList.add(
        JmxStat.builder()
            .withScope("ReadStage")
            .withName("ActiveTasks")
            .withAttribute("Value")
            .withValue(2.0)
            .build());
    statList.add(
        JmxStat.builder()
            .withScope("ReadStage")
            .withName("CurrentlyBlockedTasks")
            .withAttribute("Value")
            .withValue(3.0)
            .build());
    statList.add(
        JmxStat.builder()
            .withScope("ReadStage")
            .withName("CompletedTasks")
            .withAttribute("Value")
            .withValue(4.0)
            .build());
    statList.add(
        JmxStat.builder()
            .withScope("ReadStage")
            .withName("MaxPoolSize")
            .withAttribute("Value")
            .withValue(5.0)
            .build());
    statList.add(
        JmxStat.builder()
            .withScope("ReadStage")
            .withName("TotalBlockedTasks")
            .withAttribute("Value")
            .withValue(6.0)
            .build());

    Map<String, List<JmxStat>> jmxStats = Maps.newHashMap();
    jmxStats.put("ReadStage", statList);
    Node node = Node.builder().withHostname("127.0.0.1").build();
    AppContext context = new AppContext();
    List<ThreadPoolStat> threadPoolStats
        = ClusterFacade.create(context).convertToThreadPoolStats(
        JmxMetricsProxy.convertToGenericMetrics(jmxStats, node));
    ThreadPoolStat tpstat = threadPoolStats.get(0);

    assertEquals(1, tpstat.getPendingTasks().intValue());
    assertEquals(2, tpstat.getActiveTasks().intValue());
    assertEquals(3, tpstat.getCurrentlyBlockedTasks().intValue());
    assertEquals(4, tpstat.getCompletedTasks().intValue());
    assertEquals(5, tpstat.getMaxPoolSize().intValue());
    assertEquals(6, tpstat.getTotalBlockedTasks().intValue());
  }

  @Test
  public void testGetDroppedMessages()
      throws InterruptedException, ReaperException, JMException, IOException, ClassNotFoundException {

    AppContext cxt = new AppContext();
    ClusterFacade clusterFacade = Mockito.mock(ClusterFacade.class);
    Mockito.doReturn(Collections.singletonList("")).when(clusterFacade).getDroppedMessages(any());
    cxt.config = new ReaperApplicationConfiguration();
    cxt.config.setJmxConnectionTimeoutInSeconds(10);
    cxt.managementConnectionFactory = mock(JmxManagementConnectionFactory.class);
    JmxCassandraManagementProxy jmx = (JmxCassandraManagementProxy) mock(
        Class.forName("io.cassandrareaper.management.jmx.JmxCassandraManagementProxy"));
    when(((JmxManagementConnectionFactory) cxt.managementConnectionFactory).connectAny(any(Collection.class))).thenReturn(jmx);

    // @todo capture objectName and return valid set of objectNames,
    // to properly test MetricsProxy.collectMetrics(..) and MetricsService.convertToDroppedMessages(..)
    when(jmx.queryNames(Mockito.any(ObjectName.class), Mockito.isNull())).thenReturn(Collections.emptySet());

    Node node = Node.builder().withHostname("127.0.0.1").build();
    MetricsService.create(cxt, () -> clusterFacade).getDroppedMessages(node);
    Mockito.verify(clusterFacade, Mockito.times(1)).getDroppedMessages(Mockito.any());
  }

  @Test
  public void testConvertToDroppedMessages() throws ReaperException, InterruptedException {
    AppContext context = new AppContext();
    context.config = new ReaperApplicationConfiguration();
    ClusterFacade clusterFacade = ClusterFacade.create(context);
    final MetricsService metricsGrabber = MetricsService.create(context, () -> clusterFacade);

    List<JmxStat> statList = Lists.newArrayList();
    statList.add(
        JmxStat.builder()
            .withScope("READ")
            .withName("Dropped")
            .withAttribute("MeanRate")
            .withValue(1.0)
            .build());
    statList.add(
        JmxStat.builder()
            .withScope("READ")
            .withName("Dropped")
            .withAttribute("OneMinuteRate")
            .withValue(2.0)
            .build());
    statList.add(
        JmxStat.builder()
            .withScope("READ")
            .withName("Dropped")
            .withAttribute("FiveMinuteRate")
            .withValue(3.0)
            .build());
    statList.add(
        JmxStat.builder()
            .withScope("READ")
            .withName("Dropped")
            .withAttribute("FifteenMinuteRate")
            .withValue(4.0)
            .build());
    statList.add(
        JmxStat.builder()
            .withScope("READ")
            .withName("Dropped")
            .withAttribute("Count")
            .withValue(5.0)
            .build());

    Map<String, List<JmxStat>> jmxStats = Maps.newHashMap();
    jmxStats.put("READ", statList);

    Node node = Node.builder().withHostname("127.0.0.1").build();
    List<DroppedMessages> droppedMessages
        = clusterFacade.convertToDroppedMessages(
        JmxMetricsProxy.convertToGenericMetrics(jmxStats, node));
    DroppedMessages dropped = droppedMessages.get(0);

    assertEquals(1, dropped.getMeanRate().intValue());
    assertEquals(2, dropped.getOneMinuteRate().intValue());
    assertEquals(3, dropped.getFiveMinuteRate().intValue());
    assertEquals(4, dropped.getFifteenMinuteRate().intValue());
    assertEquals(5, dropped.getCount().intValue());
  }

  @Test
  public void testGetClientRequestLatencies()
      throws InterruptedException, ReaperException, JMException, IOException, ClassNotFoundException {

    AppContext cxt = new AppContext();
    ClusterFacade clusterFacadeMock = Mockito.mock(ClusterFacade.class);
    Mockito.when(clusterFacadeMock.getClientRequestLatencies(any())).thenReturn(Collections.EMPTY_LIST);
    cxt.config = new ReaperApplicationConfiguration();
    cxt.config.setJmxConnectionTimeoutInSeconds(10);
    cxt.managementConnectionFactory = mock(JmxManagementConnectionFactory.class);
    JmxCassandraManagementProxy jmx = (JmxCassandraManagementProxy) mock(
        Class.forName("io.cassandrareaper.management.jmx.JmxCassandraManagementProxy"));
    when(((JmxManagementConnectionFactory) cxt.managementConnectionFactory).connectAny(any(Collection.class))).thenReturn(jmx);

    // @todo capture objectName and return valid set of objectNames,
    // to properly test MetricsProxy.collectMetrics(..) and MetricsService.convertToMetricsHistogram(..)
    when(jmx.queryNames(Mockito.any(ObjectName.class), Mockito.isNull())).thenReturn(Collections.emptySet());

    Node node = Node.builder().withHostname("127.0.0.1").build();
    MetricsService.create(cxt, () -> clusterFacadeMock).getClientRequestLatencies(node);
    Mockito.verify(clusterFacadeMock, Mockito.times(1)).getClientRequestLatencies(Mockito.any());
  }
}