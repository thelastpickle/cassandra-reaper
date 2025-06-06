/*
 * Copyright 2017-2017 Spotify AB
 * Copyright 2017-2019 The Last Pickle Ltd
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

package io.cassandrareaper.management.jmx;

import io.cassandrareaper.AppContext;
import io.cassandrareaper.ReaperApplicationConfiguration;
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.Node;
import io.cassandrareaper.crypto.Cryptograph;
import io.cassandrareaper.management.HostConnectionCounters;
import io.cassandrareaper.storage.IStorageDao;
import io.cassandrareaper.storage.cassandra.CassandraStorageFacade;
import io.cassandrareaper.storage.cluster.IClusterDao;

import java.net.UnknownHostException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import org.junit.Test;
import org.mockito.Mockito;

public final class JmxCustomPortTest {

  /*
   * Test that the custom JMX port is correctly used by the connection factory
   *
   * @throws ReaperException
   */
  @Test
  public void customJmxPortTest()
      throws ReaperException, InterruptedException, UnknownHostException {
    AppContext context = new AppContext();
    context.config = new ReaperApplicationConfiguration();
    final Cryptograph cryptographMock = mock(Cryptograph.class);
    final JmxCassandraManagementProxy cassandraManagementProxyMock =
        CassandraManagementProxyTest.mockJmxProxyImpl();
    final AtomicInteger port = new AtomicInteger(0);
    HostConnectionCounters hostConnectionCounters = mock(HostConnectionCounters.class);
    when(hostConnectionCounters.getSuccessfulConnections(any())).thenReturn(1);
    context.storage = Mockito.mock(IStorageDao.class);
    Mockito.when(context.storage.getClusterDao()).thenReturn(Mockito.mock(IClusterDao.class));

    context.managementConnectionFactory =
        new JmxManagementConnectionFactory(context, cryptographMock) {
          @Override
          protected JmxCassandraManagementProxy connectImpl(Node node) throws ReaperException {
            final JmxCassandraManagementProxy jmx = cassandraManagementProxyMock;
            port.set(node.getJmxPort());
            return jmx;
          }

          @Override
          public HostConnectionCounters getHostConnectionCounters() {
            return hostConnectionCounters;
          }
        };

    context.config = new ReaperApplicationConfiguration();
    context.storage = mock(CassandraStorageFacade.class);

    Cluster cluster =
        Cluster.builder()
            .withName("test")
            .withPartitioner("murmur3partitioner")
            .withSeedHosts(ImmutableSet.of("127.0.0.1", "127.0.0.2"))
            .withJmxPort(7188)
            .build();

    ((JmxManagementConnectionFactory) context.managementConnectionFactory)
        .connectAny(
            Collections.singleton(
                Node.builder().withCluster(cluster).withHostname("127.0.0.1").build()));

    assertEquals(7188, port.get());

    Cluster cluster2 =
        Cluster.builder()
            .withName("test")
            .withPartitioner("murmur3partitioner")
            .withSeedHosts(ImmutableSet.of("127.0.0.1", "127.0.0.2"))
            .withJmxPort(7198)
            .build();

    ((JmxManagementConnectionFactory) context.managementConnectionFactory)
        .connectAny(
            Collections.singleton(
                Node.builder().withCluster(cluster2).withHostname("127.0.0.3").build()));

    assertEquals(7198, port.get());
  }
}
