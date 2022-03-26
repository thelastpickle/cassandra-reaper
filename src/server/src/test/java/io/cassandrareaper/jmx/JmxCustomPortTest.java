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

package io.cassandrareaper.jmx;

import io.cassandrareaper.AppContext;
import io.cassandrareaper.ReaperApplicationConfiguration;
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.Node;
import io.cassandrareaper.crypto.Cryptograph;
import io.cassandrareaper.storage.CassandraStorage;

import java.net.UnknownHostException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class JmxCustomPortTest {

  /*
   * Test that the custom JMX port is correctly used by the connection factory
   *
   * @throws ReaperException
   */
  @Test
  public void customJmxPortTest() throws ReaperException, InterruptedException, UnknownHostException {
    AppContext context = new AppContext();
    final Cryptograph cryptographMock = mock(Cryptograph.class);
    final JmxProxy jmxProxyMock = JmxProxyTest.mockJmxProxyImpl();
    final AtomicInteger port = new AtomicInteger(0);
    HostConnectionCounters hostConnectionCounters = mock(HostConnectionCounters.class);
    when(hostConnectionCounters.getSuccessfulConnections(any())).thenReturn(1);


    context.jmxConnectionFactory = new JmxConnectionFactory(context, cryptographMock) {
          @Override
          protected JmxProxy connectImpl(Node node) throws ReaperException {
            final JmxProxy jmx = jmxProxyMock;
            port.set(node.getJmxPort());
            return jmx;
          }

          @Override
          public HostConnectionCounters getHostConnectionCounters() {
            return hostConnectionCounters;
          }
        };

    context.config = new ReaperApplicationConfiguration();
    context.storage = mock(CassandraStorage.class);

    Cluster cluster = Cluster.builder()
            .withName("test")
            .withPartitioner("murmur3partitioner")
            .withSeedHosts(ImmutableSet.of("127.0.0.1", "127.0.0.2"))
            .withJmxPort(7188)
            .build();

    context.jmxConnectionFactory
        .connectAny(Collections.singleton(Node.builder().withCluster(cluster).withHostname("127.0.0.1").build()));

    assertEquals(7188, port.get());

    Cluster cluster2 = Cluster.builder()
            .withName("test")
            .withPartitioner("murmur3partitioner")
            .withSeedHosts(ImmutableSet.of("127.0.0.1", "127.0.0.2"))
            .withJmxPort(7198)
            .build();

    context.jmxConnectionFactory
        .connectAny(Collections.singleton(Node.builder().withCluster(cluster2).withHostname("127.0.0.3").build()));

    assertEquals(7198, port.get());
  }
}
