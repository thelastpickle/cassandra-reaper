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
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.Node;
import io.cassandrareaper.core.Snapshot;
import io.cassandrareaper.jmx.ClusterFacade;
import io.cassandrareaper.jmx.JmxConnectionFactory;
import io.cassandrareaper.jmx.JmxProxy;
import io.cassandrareaper.jmx.JmxProxyTest;
import io.cassandrareaper.storage.IStorage;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.collect.ImmutableSet;
import org.apache.cassandra.service.StorageServiceMBean;
import org.apache.commons.lang3.tuple.Pair;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public final class SnapshotServiceTest {

  private static final ExecutorService SNAPSHOT_MANAGER_EXECUTOR = Executors.newFixedThreadPool(2);

  @Test
  public void testTakeSnapshot() throws InterruptedException, ReaperException, ClassNotFoundException, IOException {

    JmxProxy proxy = (JmxProxy) mock(Class.forName("io.cassandrareaper.jmx.JmxProxyImpl"));
    StorageServiceMBean storageMBean = Mockito.mock(StorageServiceMBean.class);
    JmxProxyTest.mockGetStorageServiceMBean(proxy, storageMBean);

    AppContext cxt = new AppContext();
    cxt.config = TestRepairConfiguration.defaultConfig();
    cxt.jmxConnectionFactory = mock(JmxConnectionFactory.class);
    when(cxt.jmxConnectionFactory.connectAny(any(Collection.class))).thenReturn(proxy);

    Pair<Node,String> result = SnapshotService
        .create(cxt, SNAPSHOT_MANAGER_EXECUTOR)
        .takeSnapshot("Test", Node.builder().withHostname("127.0.0.1").build());

    Assertions.assertThat(result.getLeft().getHostname()).isEqualTo("127.0.0.1");
    Assertions.assertThat(result.getRight()).isEqualTo("Test");
    verify(storageMBean, times(1)).takeSnapshot("Test");
  }

  @Test
  public void testTakeSnapshotForKeyspaces()
      throws InterruptedException, ReaperException, ClassNotFoundException, IOException {

    JmxProxy proxy = (JmxProxy) mock(Class.forName("io.cassandrareaper.jmx.JmxProxyImpl"));
    StorageServiceMBean storageMBean = Mockito.mock(StorageServiceMBean.class);
    JmxProxyTest.mockGetStorageServiceMBean(proxy, storageMBean);

    AppContext cxt = new AppContext();
    cxt.config = TestRepairConfiguration.defaultConfig();
    cxt.jmxConnectionFactory = mock(JmxConnectionFactory.class);
    when(cxt.jmxConnectionFactory.connectAny(any(Collection.class))).thenReturn(proxy);
    Node host = Node.builder().withHostname("127.0.0.1").build();

    Pair<Node,String> result = SnapshotService
        .create(cxt, SNAPSHOT_MANAGER_EXECUTOR)
        .takeSnapshot("Test", host, "keyspace1", "keyspace2");

    Assertions.assertThat(result.getLeft().getHostname()).isEqualTo("127.0.0.1");
    Assertions.assertThat(result.getRight()).isEqualTo("Test");
    verify(storageMBean, times(1)).takeSnapshot("Test", "keyspace1", "keyspace2");
  }

  @Test
  public void testListSnapshot() throws InterruptedException, ReaperException, ClassNotFoundException {

    JmxProxy proxy = (JmxProxy) mock(Class.forName("io.cassandrareaper.jmx.JmxProxyImpl"));
    when(proxy.getCassandraVersion()).thenReturn("2.1.0");
    StorageServiceMBean storageMBean = Mockito.mock(StorageServiceMBean.class);
    JmxProxyTest.mockGetStorageServiceMBean(proxy, storageMBean);
    when(storageMBean.getSnapshotDetails()).thenReturn(Collections.emptyMap());

    AppContext cxt = new AppContext();
    cxt.config = TestRepairConfiguration.defaultConfig();
    cxt.jmxConnectionFactory = mock(JmxConnectionFactory.class);
    when(cxt.jmxConnectionFactory.connectAny(any(Collection.class))).thenReturn(proxy);

    List<Snapshot> result = SnapshotService
        .create(cxt, SNAPSHOT_MANAGER_EXECUTOR)
        .listSnapshots(Node.builder().withHostname("127.0.0.1").build());

    Assertions.assertThat(result).isEmpty();
    verify(storageMBean, times(0)).getSnapshotDetails();
  }

  @Test
  public void testClearSnapshot() throws InterruptedException, ReaperException, ClassNotFoundException, IOException {

    JmxProxy proxy = (JmxProxy) mock(Class.forName("io.cassandrareaper.jmx.JmxProxyImpl"));
    StorageServiceMBean storageMBean = Mockito.mock(StorageServiceMBean.class);
    JmxProxyTest.mockGetStorageServiceMBean(proxy, storageMBean);

    AppContext cxt = new AppContext();
    cxt.config = TestRepairConfiguration.defaultConfig();
    cxt.jmxConnectionFactory = mock(JmxConnectionFactory.class);
    when(cxt.jmxConnectionFactory.connectAny(any(Collection.class))).thenReturn(proxy);

    SnapshotService
        .create(cxt, SNAPSHOT_MANAGER_EXECUTOR)
        .clearSnapshot("test", Node.builder().withHostname("127.0.0.1").build());

    verify(storageMBean, times(1)).clearSnapshot("test");
  }

  @Test
  public void testClearSnapshotClusterWide()
      throws InterruptedException, ReaperException, ClassNotFoundException, IOException {

    JmxProxy proxy = (JmxProxy) mock(Class.forName("io.cassandrareaper.jmx.JmxProxyImpl"));
    StorageServiceMBean storageMBean = Mockito.mock(StorageServiceMBean.class);
    JmxProxyTest.mockGetStorageServiceMBean(proxy, storageMBean);

    AppContext cxt = new AppContext();
    cxt.config = TestRepairConfiguration.defaultConfig();
    cxt.jmxConnectionFactory = mock(JmxConnectionFactory.class);
    when(cxt.jmxConnectionFactory.connectAny(any(Collection.class))).thenReturn(proxy);
    ClusterFacade clusterFacadeSpy = Mockito.spy(ClusterFacade.create(cxt));
    Mockito.doReturn(Arrays.asList("127.0.0.1", "127.0.0.2")).when(clusterFacadeSpy).getLiveNodes(any());
    Mockito.doReturn(Arrays.asList("127.0.0.1", "127.0.0.2"))
        .when(clusterFacadeSpy)
        .getLiveNodes(any(), any());
    cxt.storage = mock(IStorage.class);

    Cluster cluster = Cluster.builder()
        .withName("testCluster")
        .withPartitioner("murmur3")
        .withSeedHosts(ImmutableSet.of("127.0.0.1"))
        .build();

    when(cxt.storage.getCluster(anyString())).thenReturn(cluster);

    SnapshotService
        .create(cxt, SNAPSHOT_MANAGER_EXECUTOR, () -> clusterFacadeSpy)
        .clearSnapshotClusterWide("snapshot", "testCluster");

    verify(storageMBean, times(2)).clearSnapshot("snapshot");
  }

  @Test
  public void testTakeSnapshotClusterWide()
      throws InterruptedException, ReaperException, ClassNotFoundException, IOException {

    JmxProxy proxy = (JmxProxy) mock(Class.forName("io.cassandrareaper.jmx.JmxProxyImpl"));
    StorageServiceMBean storageMBean = Mockito.mock(StorageServiceMBean.class);
    JmxProxyTest.mockGetStorageServiceMBean(proxy, storageMBean);

    AppContext cxt = new AppContext();
    cxt.config = TestRepairConfiguration.defaultConfig();
    cxt.jmxConnectionFactory = mock(JmxConnectionFactory.class);
    when(cxt.jmxConnectionFactory.connectAny(any(Collection.class))).thenReturn(proxy);
    ClusterFacade clusterFacadeSpy = Mockito.spy(ClusterFacade.create(cxt));
    Mockito.doReturn(Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3")).when(clusterFacadeSpy).getLiveNodes(any());
    Mockito.doReturn(Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3"))
        .when(clusterFacadeSpy)
        .getLiveNodes(any(), any());

    cxt.storage = mock(IStorage.class);

    Cluster cluster = Cluster.builder()
        .withName("testCluster")
        .withPartitioner("murmur3")
        .withSeedHosts(ImmutableSet.of("127.0.0.1"))
        .build();

    when(cxt.storage.getCluster(anyString())).thenReturn(cluster);

    List<Pair<Node,String>> result = SnapshotService
        .create(cxt, SNAPSHOT_MANAGER_EXECUTOR, () -> clusterFacadeSpy)
        .takeSnapshotClusterWide("snapshot", "testCluster", "testOwner", "testCause");

    Assertions.assertThat(result.size()).isEqualTo(3);
    for (int i = 0 ; i < 3 ; ++i) {
      Assertions.assertThat(result.get(i).getLeft().getClusterName()).isEqualTo("testcluster");
      Assertions.assertThat(result.get(i).getLeft().getHostname()).isEqualTo("127.0.0." + (i + 1));
      Assertions.assertThat(result.get(i).getRight()).isEqualTo("snapshot");
    }
    verify(storageMBean, times(3)).takeSnapshot("snapshot");
  }
}
