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

import io.cassandrareaper.AppContext;
import io.cassandrareaper.ReaperApplicationConfiguration;
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.DroppedMessages;
import io.cassandrareaper.core.JmxStat;
import io.cassandrareaper.core.Node;
import io.cassandrareaper.core.ThreadPoolStat;
import io.cassandrareaper.jmx.JmxConnectionFactory;
import io.cassandrareaper.jmx.JmxProxy;
import io.cassandrareaper.storage.IStorage;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;

import javax.management.JMException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MetricsGrabberTest {

  @Test
  public void testGetTpstats() throws InterruptedException, ReaperException, JMException, IOException {
    AppContext context = new AppContext();
    context.config = new ReaperApplicationConfiguration();
    context.config.setJmxConnectionTimeoutInSeconds(10);

    context.storage = mock(IStorage.class);
    Cluster cluster = new Cluster("testCluster", "murmur3", new TreeSet<>(Arrays.asList("127.0.0.1")));
    when(context.storage.getCluster(anyString())).thenReturn(Optional.of(cluster));

    final JmxProxy jmx = mock(JmxProxy.class);
    when(jmx.getLiveNodes()).thenReturn(Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3"));

    final MetricsGrabber metricsGrabber = MetricsGrabber.create(context);
    context.jmxConnectionFactory =
        new JmxConnectionFactory() {
          @Override
          public JmxProxy connect(Node host, int connectionTimeout)
              throws ReaperException, InterruptedException {
            return jmx;
          }

          @Override
          public JmxProxy connectAny(Cluster cluster, int connectionTimeout)
              throws ReaperException {
            return jmx;
          }
        };
    Node node = Node.builder().withClusterName("test").withHostname("127.0.0.1").build();
    metricsGrabber.getTpStats(node);
    verify(jmx, times(1)).collectTpStats();
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
  public void testGetDroppedMessages() throws InterruptedException, ReaperException, JMException, IOException {
    AppContext context = new AppContext();
    context.config = new ReaperApplicationConfiguration();
    context.config.setJmxConnectionTimeoutInSeconds(10);

    context.storage = mock(IStorage.class);
    Cluster cluster = new Cluster("testCluster", "murmur3", new TreeSet<>(Arrays.asList("127.0.0.1")));
    when(context.storage.getCluster(anyString())).thenReturn(Optional.of(cluster));

    final JmxProxy jmx = mock(JmxProxy.class);
    when(jmx.getLiveNodes()).thenReturn(Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3"));

    final MetricsGrabber metricsGrabber = MetricsGrabber.create(context);
    context.jmxConnectionFactory =
        new JmxConnectionFactory() {
          @Override
          public JmxProxy connect(Node host, int connectionTimeout)
              throws ReaperException, InterruptedException {
            return jmx;
          }

          @Override
          public JmxProxy connectAny(Cluster cluster, int connectionTimeout)
              throws ReaperException {
            return jmx;
          }
        };
    Node node = Node.builder().withClusterName("test").withHostname("127.0.0.1").build();
    metricsGrabber.getDroppedMessages(node);
    verify(jmx, times(1)).collectDroppedMessages();
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

}
