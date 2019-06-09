/*
 * Copyright 2017-2017 Spotify AB
 * Copyright 2017-2018 The Last Pickle Ltd
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
import io.cassandrareaper.core.ClusterProperties;
import io.cassandrareaper.core.Node;
import io.cassandrareaper.storage.CassandraStorage;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class JmxCustomPortTest {

  /*
   * Test that the custom JMX port is correctly used by the connection factory
   *
   * @throws ReaperException
   */
  @Test
  public void customJmxPortTest() throws ReaperException, InterruptedException {
    AppContext context = new AppContext();
    final JmxProxy jmxProxyMock = mock(JmxProxy.class);
    final AtomicInteger port = new AtomicInteger(0);

    context.jmxConnectionFactory
        = new JmxConnectionFactory(context) {
          @Override
          protected JmxProxy connectImpl(Node node) throws ReaperException {
            final JmxProxy jmx = jmxProxyMock;
            port.set(node.getCluster().getProperties().getJmxPort());
            return jmx;
          }
        };

    context.config = new ReaperApplicationConfiguration();
    context.storage = mock(CassandraStorage.class);

    Cluster cluster
        = new Cluster(
            "test",
            Optional.of("murmur3partitioner"),
            new LinkedHashSet<>(Arrays.asList("127.0.0.1", "127.0.0.2")),
            ClusterProperties.builder().withJmxPort(7188).build());

    context.jmxConnectionFactory.connectAny(cluster);
    assertEquals(7188, port.get());

    Cluster cluster2
        = new Cluster(
            "test",
            Optional.of("murmur3partitioner"),
            new LinkedHashSet<>(Arrays.asList("127.0.0.1", "127.0.0.2")),
            ClusterProperties.builder().withJmxPort(7198).build());

    context.jmxConnectionFactory.connect(
        Node.builder().withCluster(cluster2).withHostname("127.0.0.3").build());
    assertEquals(7198, port.get());
  }
}
