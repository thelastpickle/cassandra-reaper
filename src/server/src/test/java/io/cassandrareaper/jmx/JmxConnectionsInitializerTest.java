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

package io.cassandrareaper.jmx;

import io.cassandrareaper.AppContext;
import io.cassandrareaper.ReaperApplicationConfiguration;
import io.cassandrareaper.ReaperApplicationConfiguration.DatacenterAvailability;
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.Node;
import io.cassandrareaper.crypto.Cryptograph;
import io.cassandrareaper.storage.CassandraStorage;

import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class JmxConnectionsInitializerTest {


  /*
   * JMX connections initialization should happen only if storage is using Cassandra as backend and
   * DatacenterAvailability is set to either LOCAL or EACH.
   *
   * @throws ReaperException
   */
  @Test
  public void initializerDatacenterAvailabilityEachTest() throws ReaperException, UnknownHostException {
    AppContext context = new AppContext();
    final Cryptograph cryptographMock = mock(Cryptograph.class);
    final JmxProxy jmxProxyMock = JmxProxyTest.mockJmxProxyImpl();
    final AtomicInteger connectionAttempts = new AtomicInteger(0);

    context.jmxConnectionFactory = new JmxConnectionFactory(context, cryptographMock) {
          @Override
          protected JmxProxy connectImpl(Node node) throws ReaperException {
            final JmxProxy jmx = jmxProxyMock;
            connectionAttempts.incrementAndGet();
            return jmx;
          }
        };

    context.config = new ReaperApplicationConfiguration();
    context.config.setDatacenterAvailability(DatacenterAvailability.EACH);
    context.storage = mock(CassandraStorage.class);

    Cluster cluster = Cluster.builder()
            .withName("test")
            .withPartitioner("murmur3partitioner")
            .withSeedHosts(ImmutableSet.of("127.0.0.1", "127.0.0.2"))
            .build();

    JmxConnectionsInitializer initializer = JmxConnectionsInitializer.create(context);
    initializer.on(cluster);

    assertEquals(2, connectionAttempts.get());
  }

  /*
   * JMX connections initialization should happen only if storage is using Cassandra as backend and
   * DatacenterAvailability is set to either LOCAL or EACH.
   *
   * @throws ReaperException
   */
  @Test
  public void initializerDatacenterAvailabilityLocalTest() throws ReaperException, UnknownHostException {
    AppContext context = new AppContext();
    final Cryptograph cryptographMock = mock(Cryptograph.class);
    final JmxProxy jmxProxyMock = JmxProxyTest.mockJmxProxyImpl();
    final AtomicInteger connectionAttempts = new AtomicInteger(0);

    context.jmxConnectionFactory = new JmxConnectionFactory(context, cryptographMock) {
          @Override
          protected JmxProxy connectImpl(Node node) throws ReaperException {
            final JmxProxy jmx = jmxProxyMock;
            connectionAttempts.incrementAndGet();
            return jmx;
          }
        };

    context.config = new ReaperApplicationConfiguration();
    context.config.setDatacenterAvailability(DatacenterAvailability.LOCAL);
    context.storage = mock(CassandraStorage.class);

    Cluster cluster = Cluster.builder()
            .withName("test")
            .withPartitioner("murmur3partitioner")
            .withSeedHosts(ImmutableSet.of("127.0.0.1", "127.0.0.2", "127.0.0.3"))
            .build();

    JmxConnectionsInitializer initializer = JmxConnectionsInitializer.create(context);
    initializer.on(cluster);

    assertEquals(3, connectionAttempts.get());
  }

  /*
   * JMX connections initialization should happen only if storage is using Cassandra as backend and
   * DatacenterAvailability is set to either LOCAL or EACH.
   *
   * @throws ReaperException
   */
  @Test
  public void initializerDatacenterAvailabilityAllTest() throws ReaperException {
    AppContext context = new AppContext();
    final Cryptograph cryptographMock = mock(Cryptograph.class);
    final JmxProxy jmxProxyMock = mock(JmxProxy.class);
    final AtomicInteger connectionAttempts = new AtomicInteger(0);

    context.jmxConnectionFactory = new JmxConnectionFactory(context, cryptographMock) {
          @Override
          protected JmxProxy connectImpl(Node node) throws ReaperException {
            final JmxProxy jmx = jmxProxyMock;
            connectionAttempts.incrementAndGet();
            return jmx;
          }
        };

    context.config = new ReaperApplicationConfiguration();
    context.config.setDatacenterAvailability(DatacenterAvailability.ALL);
    context.storage = mock(CassandraStorage.class);

    Cluster cluster = Cluster.builder()
            .withName("test")
            .withPartitioner("murmur3partitioner")
            .withSeedHosts(ImmutableSet.of("127.0.0.1", "127.0.0.2", "127.0.0.3"))
            .build();

    JmxConnectionsInitializer initializer = JmxConnectionsInitializer.create(context);
    initializer.on(cluster);

    assertEquals(0, connectionAttempts.get());
  }
}
