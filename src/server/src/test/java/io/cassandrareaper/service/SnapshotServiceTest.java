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
import io.cassandrareaper.management.ClusterFacade;
import io.cassandrareaper.management.HostConnectionCounters;
import io.cassandrareaper.management.ICassandraManagementProxy;
import io.cassandrareaper.management.jmx.JmxManagementConnectionFactory;
import io.cassandrareaper.storage.IStorageDao;
import io.cassandrareaper.storage.cluster.IClusterDao;
import io.cassandrareaper.storage.snapshot.ISnapshotDao;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.tuple.Pair;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.Mockito;

public final class SnapshotServiceTest {

  private static final ExecutorService SNAPSHOT_MANAGER_EXECUTOR = Executors.newFixedThreadPool(2);

  @Test
  public void testTakeSnapshot()
      throws InterruptedException, ReaperException, ClassNotFoundException, IOException {

    ICassandraManagementProxy proxy =
        (ICassandraManagementProxy)
            mock(Class.forName("io.cassandrareaper.management.jmx.JmxCassandraManagementProxy"));

    AppContext cxt = new AppContext();
    cxt.config = TestRepairConfiguration.defaultConfig();
    cxt.managementConnectionFactory = mock(JmxManagementConnectionFactory.class);
    when(cxt.managementConnectionFactory.connectAny(any(Collection.class))).thenReturn(proxy);

    ISnapshotDao mockSnapshotDao = mock(ISnapshotDao.class);

    Pair<Node, String> result =
        SnapshotService.create(cxt, SNAPSHOT_MANAGER_EXECUTOR, mockSnapshotDao)
            .takeSnapshot("Test", Node.builder().withHostname("127.0.0.1").build());

    Assertions.assertThat(result.getLeft().getHostname()).isEqualTo("127.0.0.1");
    Assertions.assertThat(result.getRight()).isEqualTo("Test");
    verify(proxy, times(1)).takeSnapshot("Test");
  }

  @Test
  public void testTakeSnapshotForKeyspaces()
      throws InterruptedException, ReaperException, ClassNotFoundException, IOException {

    ICassandraManagementProxy proxy =
        (ICassandraManagementProxy)
            mock(Class.forName("io.cassandrareaper.management.jmx.JmxCassandraManagementProxy"));

    AppContext cxt = new AppContext();
    cxt.config = TestRepairConfiguration.defaultConfig();
    cxt.managementConnectionFactory = mock(JmxManagementConnectionFactory.class);
    when(cxt.managementConnectionFactory.connectAny(any(Collection.class))).thenReturn(proxy);
    Node host = Node.builder().withHostname("127.0.0.1").build();
    ISnapshotDao mockSnapshotDao = mock(ISnapshotDao.class);
    Pair<Node, String> result =
        SnapshotService.create(cxt, SNAPSHOT_MANAGER_EXECUTOR, mockSnapshotDao)
            .takeSnapshot("Test", host, "keyspace1", "keyspace2");

    Assertions.assertThat(result.getLeft().getHostname()).isEqualTo("127.0.0.1");
    Assertions.assertThat(result.getRight()).isEqualTo("Test");
    verify(proxy, times(1)).takeSnapshot("Test", "keyspace1", "keyspace2");
  }

  @Test
  public void testListSnapshot()
      throws InterruptedException, ReaperException, ClassNotFoundException {

    ICassandraManagementProxy proxy =
        (ICassandraManagementProxy)
            mock(Class.forName("io.cassandrareaper.management.jmx.JmxCassandraManagementProxy"));
    when(proxy.getCassandraVersion()).thenReturn("2.1.0");
    when(proxy.listSnapshots()).thenReturn(Collections.emptyList());

    AppContext cxt = new AppContext();
    cxt.config = TestRepairConfiguration.defaultConfig();
    cxt.managementConnectionFactory = mock(JmxManagementConnectionFactory.class);
    HostConnectionCounters connectionCounters = mock(HostConnectionCounters.class);
    when(cxt.managementConnectionFactory.connectAny(any(Collection.class))).thenReturn(proxy);
    when(cxt.managementConnectionFactory.getHostConnectionCounters())
        .thenReturn(connectionCounters);
    ISnapshotDao mockSnapshotDao = mock(ISnapshotDao.class);
    List<Snapshot> result =
        SnapshotService.create(cxt, SNAPSHOT_MANAGER_EXECUTOR, mockSnapshotDao)
            .listSnapshots(Node.builder().withHostname("127.0.0.1").build());

    Assertions.assertThat(result).isEmpty();
    verify(proxy, times(1)).listSnapshots();
  }

  @Test
  public void testClearSnapshot()
      throws InterruptedException, ReaperException, ClassNotFoundException, IOException {

    ICassandraManagementProxy proxy =
        (ICassandraManagementProxy)
            mock(Class.forName("io.cassandrareaper.management.jmx.JmxCassandraManagementProxy"));

    AppContext cxt = new AppContext();
    cxt.config = TestRepairConfiguration.defaultConfig();
    cxt.managementConnectionFactory = mock(JmxManagementConnectionFactory.class);
    when(cxt.managementConnectionFactory.connectAny(any(Collection.class))).thenReturn(proxy);
    ISnapshotDao mockSnapshotDao = mock(ISnapshotDao.class);
    SnapshotService.create(cxt, SNAPSHOT_MANAGER_EXECUTOR, mockSnapshotDao)
        .clearSnapshot("test", Node.builder().withHostname("127.0.0.1").build());

    verify(proxy, times(1)).clearSnapshot("test");
  }

  @Test
  public void testClearSnapshotClusterWide()
      throws InterruptedException, ReaperException, ClassNotFoundException, IOException {

    ICassandraManagementProxy proxy =
        (ICassandraManagementProxy)
            mock(Class.forName("io.cassandrareaper.management.jmx.JmxCassandraManagementProxy"));

    AppContext cxt = new AppContext();
    cxt.config = TestRepairConfiguration.defaultConfig();
    cxt.managementConnectionFactory = mock(JmxManagementConnectionFactory.class);
    when(cxt.managementConnectionFactory.connectAny(any(Collection.class))).thenReturn(proxy);
    ClusterFacade clusterFacadeSpy = Mockito.spy(ClusterFacade.create(cxt));
    Mockito.doReturn(Arrays.asList("127.0.0.1", "127.0.0.2"))
        .when(clusterFacadeSpy)
        .getLiveNodes(any());
    Mockito.doReturn(Arrays.asList("127.0.0.1", "127.0.0.2"))
        .when(clusterFacadeSpy)
        .getLiveNodes(any(), any());
    cxt.storage = mock(IStorageDao.class);

    Cluster cluster =
        Cluster.builder()
            .withName("testCluster")
            .withPartitioner("murmur3")
            .withSeedHosts(ImmutableSet.of("127.0.0.1"))
            .build();
    IClusterDao mockedClusterDao = Mockito.mock(IClusterDao.class);
    Mockito.when(cxt.storage.getClusterDao()).thenReturn(mockedClusterDao);
    Mockito.when(mockedClusterDao.getCluster(any())).thenReturn(cluster);
    when(mockedClusterDao.getCluster(anyString())).thenReturn(cluster);

    ISnapshotDao mockSnapshotDao = mock(ISnapshotDao.class);

    SnapshotService.create(cxt, SNAPSHOT_MANAGER_EXECUTOR, () -> clusterFacadeSpy, mockSnapshotDao)
        .clearSnapshotClusterWide("snapshot", "testCluster");

    verify(proxy, times(2)).clearSnapshot("snapshot");
  }

  @Test
  public void testTakeSnapshotClusterWide()
      throws InterruptedException, ReaperException, ClassNotFoundException, IOException {

    ICassandraManagementProxy proxy =
        (ICassandraManagementProxy)
            mock(Class.forName("io.cassandrareaper.management.jmx.JmxCassandraManagementProxy"));

    AppContext cxt = new AppContext();
    cxt.config = TestRepairConfiguration.defaultConfig();
    cxt.managementConnectionFactory = mock(JmxManagementConnectionFactory.class);
    when(cxt.managementConnectionFactory.connectAny(any(Collection.class))).thenReturn(proxy);
    ClusterFacade clusterFacadeSpy = Mockito.spy(ClusterFacade.create(cxt));
    Mockito.doReturn(Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3"))
        .when(clusterFacadeSpy)
        .getLiveNodes(any());
    Mockito.doReturn(Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3"))
        .when(clusterFacadeSpy)
        .getLiveNodes(any(), any());

    cxt.storage = mock(IStorageDao.class);

    Cluster cluster =
        Cluster.builder()
            .withName("testCluster")
            .withPartitioner("murmur3")
            .withSeedHosts(ImmutableSet.of("127.0.0.1"))
            .build();

    IClusterDao mockedClusterDao = Mockito.mock(IClusterDao.class);
    Mockito.when(cxt.storage.getClusterDao()).thenReturn(mockedClusterDao);
    Mockito.when(mockedClusterDao.getCluster(any())).thenReturn(cluster);

    when(mockedClusterDao.getCluster(anyString())).thenReturn(cluster);
    ISnapshotDao mockSnapshotDao = mock(ISnapshotDao.class);

    List<Pair<Node, String>> result =
        SnapshotService.create(
                cxt, SNAPSHOT_MANAGER_EXECUTOR, () -> clusterFacadeSpy, mockSnapshotDao)
            .takeSnapshotClusterWide("snapshot", "testCluster", "testOwner", "testCause");

    Assertions.assertThat(result.size()).isEqualTo(3);
    for (int i = 0; i < 3; ++i) {
      Assertions.assertThat(result.get(i).getLeft().getClusterName()).isEqualTo("testcluster");
      Assertions.assertThat(result.get(i).getLeft().getHostname()).isEqualTo("127.0.0." + (i + 1));
      Assertions.assertThat(result.get(i).getRight()).isEqualTo("snapshot");
    }
    verify(proxy, times(3)).takeSnapshot("snapshot");
  }
}
