/*
 * Copyright 2018-2018 The Last Pickle Ltd
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
import io.cassandrareaper.core.DroppedMessages;
import io.cassandrareaper.core.JmxStat;
import io.cassandrareaper.core.Node;
import io.cassandrareaper.core.ThreadPoolStat;
import io.cassandrareaper.jmx.JmxConnectionFactory;
import io.cassandrareaper.jmx.JmxProxy;
import io.cassandrareaper.jmx.JmxProxyTest;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.management.JMException;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MetricsGrabberTest {

  @Test
  public void testGetTpstats()
      throws InterruptedException, ReaperException, JMException, IOException, ClassNotFoundException {

    AppContext cxt = new AppContext();
    cxt.config = new ReaperApplicationConfiguration();
    cxt.config.setJmxConnectionTimeoutInSeconds(10);
    cxt.jmxConnectionFactory = mock(JmxConnectionFactory.class);
    JmxProxy jmx = (JmxProxy) mock(Class.forName("io.cassandrareaper.jmx.JmxProxyImpl"));
    when(cxt.jmxConnectionFactory.connect(Mockito.any(Node.class), Mockito.anyInt())).thenReturn(jmx);
    MBeanServerConnection serverConn = mock(MBeanServerConnection.class);
    JmxProxyTest.mockGetMBeanServerConnection(jmx, serverConn);

    // @todo capture objectName and return valid set of objectNames,
    // to properly test MetricsProxy.collectMetrics(..) and MetricsGrabber.convertToThreadPoolStats(..)
    when(serverConn.queryNames(Mockito.any(ObjectName.class), Mockito.isNull())).thenReturn(Collections.emptySet());

    Node node = Node.builder().withClusterName("test").withHostname("127.0.0.1").build();
    MetricsGrabber.create(cxt).getTpStats(node);
    Mockito.verify(serverConn, Mockito.times(2)).queryNames(Mockito.any(ObjectName.class), Mockito.isNull());
  }

  @Test
  public void testConvertToThreadPoolStats() {
    AppContext context = new AppContext();
    final MetricsGrabber metricsGrabber = MetricsGrabber.create(context);

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

    List<ThreadPoolStat> threadPoolStats = metricsGrabber.convertToThreadPoolStats(jmxStats);
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
    cxt.config = new ReaperApplicationConfiguration();
    cxt.config.setJmxConnectionTimeoutInSeconds(10);
    cxt.jmxConnectionFactory = mock(JmxConnectionFactory.class);
    JmxProxy jmx = (JmxProxy) mock(Class.forName("io.cassandrareaper.jmx.JmxProxyImpl"));
    when(cxt.jmxConnectionFactory.connect(Mockito.any(Node.class), Mockito.anyInt())).thenReturn(jmx);
    MBeanServerConnection serverConn = mock(MBeanServerConnection.class);
    JmxProxyTest.mockGetMBeanServerConnection(jmx, serverConn);

    // @todo capture objectName and return valid set of objectNames,
    // to properly test MetricsProxy.collectMetrics(..) and MetricsGrabber.convertToDroppedMessages(..)
    when(serverConn.queryNames(Mockito.any(ObjectName.class), Mockito.isNull())).thenReturn(Collections.emptySet());

    Node node = Node.builder().withClusterName("test").withHostname("127.0.0.1").build();
    MetricsGrabber.create(cxt).getDroppedMessages(node);
    Mockito.verify(serverConn, Mockito.times(1)).queryNames(Mockito.any(ObjectName.class), Mockito.isNull());
  }

  @Test
  public void testConvertToDroppedMessages() {
    AppContext context = new AppContext();
    final MetricsGrabber metricsGrabber = MetricsGrabber.create(context);

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

    List<DroppedMessages> droppedMessages = metricsGrabber.convertToDroppedMessages(jmxStats);
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
    cxt.config = new ReaperApplicationConfiguration();
    cxt.config.setJmxConnectionTimeoutInSeconds(10);
    cxt.jmxConnectionFactory = mock(JmxConnectionFactory.class);
    JmxProxy jmx = (JmxProxy) mock(Class.forName("io.cassandrareaper.jmx.JmxProxyImpl"));
    when(cxt.jmxConnectionFactory.connect(Mockito.any(Node.class), Mockito.anyInt())).thenReturn(jmx);
    MBeanServerConnection serverConn = mock(MBeanServerConnection.class);
    JmxProxyTest.mockGetMBeanServerConnection(jmx, serverConn);

    // @todo capture objectName and return valid set of objectNames,
    // to properly test MetricsProxy.collectMetrics(..) and MetricsGrabber.convertToMetricsHistogram(..)
    when(serverConn.queryNames(Mockito.any(ObjectName.class), Mockito.isNull())).thenReturn(Collections.emptySet());

    Node node = Node.builder().withClusterName("test").withHostname("127.0.0.1").build();
    MetricsGrabber.create(cxt).getClientRequestLatencies(node);
    Mockito.verify(serverConn, Mockito.times(1)).queryNames(Mockito.any(ObjectName.class), Mockito.isNull());
  }
}
