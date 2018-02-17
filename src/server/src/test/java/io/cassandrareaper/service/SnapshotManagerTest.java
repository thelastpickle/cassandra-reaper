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
import io.cassandrareaper.core.Node;
import io.cassandrareaper.jmx.JmxConnectionFactory;
import io.cassandrareaper.jmx.JmxProxy;
import io.cassandrareaper.storage.IStorage;

import java.util.Arrays;
import java.util.TreeSet;

import com.google.common.base.Optional;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public final class SnapshotManagerTest {

  private static final Logger LOG = LoggerFactory.getLogger(SnapshotManagerTest.class);

  @Test
  public void testTakeSnapshot() throws InterruptedException, ReaperException {
    AppContext context = new AppContext();
    context.config = new ReaperApplicationConfiguration();
    context.config.setJmxConnectionTimeoutInSeconds(10);
    final JmxProxy jmx = mock(JmxProxy.class);

    SnapshotManager snapshotManager = SnapshotManager.create(context);
    context.jmxConnectionFactory =
        new JmxConnectionFactory() {
          @Override
          public JmxProxy connect(Node host, int connectionTimeout) throws ReaperException {
            return jmx;
          }
        };

    snapshotManager.takeSnapshot(
        "Test", Node.builder().withClusterName("test").withHostname("127.0.0.1").build());
    verify(jmx, times(1)).takeSnapshot(anyString());
  }

  @Test
  public void testTakeSnapshotForKeyspaces() throws InterruptedException, ReaperException {
    AppContext context = new AppContext();
    context.config = new ReaperApplicationConfiguration();
    context.config.setJmxConnectionTimeoutInSeconds(10);
    final JmxProxy jmx = mock(JmxProxy.class);

    SnapshotManager snapshotManager = SnapshotManager.create(context);
    context.jmxConnectionFactory =
        new JmxConnectionFactory() {
          @Override
          public JmxProxy connect(Node host, int connectionTimeout)
              throws ReaperException, InterruptedException {
            return jmx;
          }
        };

    snapshotManager.takeSnapshotForKeyspaces(
        "Test",
        Node.builder().withClusterName("test").withHostname("127.0.0.1").build(),
        "keyspace1",
        "keyspace2");
    verify(jmx, times(1)).takeSnapshot(anyString(), matches("keyspace1"), matches("keyspace2"));
  }

  @Test
  public void testListSnapshot() throws InterruptedException, ReaperException {
    AppContext context = new AppContext();
    context.config = new ReaperApplicationConfiguration();
    context.config.setJmxConnectionTimeoutInSeconds(10);
    final JmxProxy jmx = mock(JmxProxy.class);

    SnapshotManager snapshotManager = SnapshotManager.create(context);
    context.jmxConnectionFactory =
        new JmxConnectionFactory() {
          @Override
          public JmxProxy connect(Node host, int connectionTimeout)
              throws ReaperException, InterruptedException {
            return jmx;
          }
        };

    snapshotManager.listSnapshots(
        Node.builder().withClusterName("Test").withHostname("127.0.0.1").build());
    verify(jmx, times(1)).listSnapshots();
  }

  @Test
  public void testClearSnapshot() throws InterruptedException, ReaperException {
    AppContext context = new AppContext();
    context.config = new ReaperApplicationConfiguration();
    context.config.setJmxConnectionTimeoutInSeconds(10);
    final JmxProxy jmx = mock(JmxProxy.class);

    SnapshotManager snapshotManager = SnapshotManager.create(context);
    context.jmxConnectionFactory =
        new JmxConnectionFactory() {
          @Override
          public JmxProxy connect(Node host, int connectionTimeout)
              throws ReaperException, InterruptedException {
            return jmx;
          }
        };

    snapshotManager.clearSnapshot(
        "test", Node.builder().withClusterName("Test").withHostname("127.0.0.1").build());
    verify(jmx, times(1)).clearSnapshot(matches("test"));
  }

  @Test
  public void testClearSnapshotClusterWide() throws InterruptedException, ReaperException {
    AppContext context = new AppContext();
    context.config = new ReaperApplicationConfiguration();
    context.config.setJmxConnectionTimeoutInSeconds(10);

    context.storage = mock(IStorage.class);
    Cluster cluster =
        new Cluster("testCluster", "murmur3", new TreeSet<String>(Arrays.asList("127.0.0.1")));
    when(context.storage.getCluster(anyString())).thenReturn(Optional.of(cluster));

    final JmxProxy jmx = mock(JmxProxy.class);
    when(jmx.getLiveNodes()).thenReturn(Arrays.asList("127.0.0.1", "127.0.0.2"));

    SnapshotManager snapshotManager = SnapshotManager.create(context);
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

    snapshotManager.clearSnapshotClusterWide("snapshot", "testCluster");
    verify(jmx, times(2)).clearSnapshot(matches("snapshot"));
  }

  @Test
  public void testTakeSnapshotClusterWide() throws InterruptedException, ReaperException {
    AppContext context = new AppContext();
    context.config = new ReaperApplicationConfiguration();
    context.config.setJmxConnectionTimeoutInSeconds(10);

    context.storage = mock(IStorage.class);
    Cluster cluster =
        new Cluster("testCluster", "murmur3", new TreeSet<String>(Arrays.asList("127.0.0.1")));
    when(context.storage.getCluster(anyString())).thenReturn(Optional.of(cluster));

    final JmxProxy jmx = mock(JmxProxy.class);
    when(jmx.getLiveNodes()).thenReturn(Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3"));

    SnapshotManager snapshotManager = SnapshotManager.create(context);
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

    snapshotManager.takeSnapshotClusterWide("snapshot", "testCluster", "test", "test");
    verify(jmx, times(3)).takeSnapshot(matches("snapshot"));
  }
}
