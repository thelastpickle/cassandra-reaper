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

package io.cassandrareaper.jmx;

import io.cassandrareaper.AppContext;
import io.cassandrareaper.ReaperApplicationConfiguration;
import io.cassandrareaper.ReaperApplicationConfiguration.DatacenterAvailability;
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.Node;
import io.cassandrareaper.storage.CassandraStorage;
import io.cassandrareaper.storage.PostgresStorage;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Optional;
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
  public void initializerDatacenterAvailabilityEachTest() throws ReaperException {
    AppContext context = new AppContext();
    final JmxProxy jmxProxyMock = mock(JmxProxy.class);
    final AtomicInteger connectionAttempts = new AtomicInteger(0);

    context.jmxConnectionFactory =
        new JmxConnectionFactory() {

          @Override
          protected JmxProxy connect(
              Optional<RepairStatusHandler> handler, Node node, int connectionTimeout)
              throws ReaperException, InterruptedException {

            final JmxProxy jmx = jmxProxyMock;
            connectionAttempts.incrementAndGet();
            return jmx;
          }
        };

    context.config = new ReaperApplicationConfiguration();
    context.config.setDatacenterAvailability(DatacenterAvailability.EACH);
    context.storage = mock(CassandraStorage.class);

    Cluster cluster =
        new Cluster(
            "test",
            "murmur3partitioner",
            new LinkedHashSet<String>(Arrays.asList("127.0.0.1", "127.0.0.2")));

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
  public void initializerDatacenterAvailabilityLocalTest() throws ReaperException {
    AppContext context = new AppContext();
    final JmxProxy jmxProxyMock = mock(JmxProxy.class);
    final AtomicInteger connectionAttempts = new AtomicInteger(0);

    context.jmxConnectionFactory =
        new JmxConnectionFactory() {

          @Override
          protected JmxProxy connect(
              Optional<RepairStatusHandler> handler, Node node, int connectionTimeout)
              throws ReaperException, InterruptedException {

            final JmxProxy jmx = jmxProxyMock;
            connectionAttempts.incrementAndGet();
            return jmx;
          }
        };

    context.config = new ReaperApplicationConfiguration();
    context.config.setDatacenterAvailability(DatacenterAvailability.LOCAL);
    context.storage = mock(CassandraStorage.class);

    Cluster cluster =
        new Cluster(
            "test",
            "murmur3partitioner",
            new LinkedHashSet<String>(Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3")));

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
    final JmxProxy jmxProxyMock = mock(JmxProxy.class);
    final AtomicInteger connectionAttempts = new AtomicInteger(0);

    context.jmxConnectionFactory =
        new JmxConnectionFactory() {

          @Override
          protected JmxProxy connect(
              Optional<RepairStatusHandler> handler, Node node, int connectionTimeout)
              throws ReaperException, InterruptedException {

            final JmxProxy jmx = jmxProxyMock;
            connectionAttempts.incrementAndGet();
            return jmx;
          }
        };

    context.config = new ReaperApplicationConfiguration();
    context.config.setDatacenterAvailability(DatacenterAvailability.ALL);
    context.storage = mock(CassandraStorage.class);

    Cluster cluster =
        new Cluster(
            "test",
            "murmur3partitioner",
            new LinkedHashSet<String>(Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3")));

    JmxConnectionsInitializer initializer = JmxConnectionsInitializer.create(context);
    initializer.on(cluster);

    assertEquals(0, connectionAttempts.get());
  }

  /*
   * JMX connections initialization should happen only if storage is using Cassandra as backend and
   * DatacenterAvailability is set to either LOCAL or EACH.
   *
   * @throws ReaperException
   */
  @Test
  public void initializerPostgresTest() throws ReaperException {
    AppContext context = new AppContext();
    final JmxProxy jmxProxyMock = mock(JmxProxy.class);
    final AtomicInteger connectionAttempts = new AtomicInteger(0);

    context.jmxConnectionFactory =
        new JmxConnectionFactory() {

          @Override
          protected JmxProxy connect(
              Optional<RepairStatusHandler> handler, Node node, int connectionTimeout)
              throws ReaperException, InterruptedException {

            final JmxProxy jmx = jmxProxyMock;
            connectionAttempts.incrementAndGet();
            return jmx;
          }
        };

    context.config = new ReaperApplicationConfiguration();
    context.config.setDatacenterAvailability(DatacenterAvailability.ALL);
    context.storage = mock(PostgresStorage.class);

    Cluster cluster =
        new Cluster(
            "test",
            "murmur3partitioner",
            new LinkedHashSet<String>(Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3")));

    JmxConnectionsInitializer initializer = JmxConnectionsInitializer.create(context);
    initializer.on(cluster);

    assertEquals(0, connectionAttempts.get());
  }

}
