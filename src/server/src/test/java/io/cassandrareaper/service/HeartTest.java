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
import io.cassandrareaper.core.NodeMetrics;
import io.cassandrareaper.jmx.HostConnectionCounters;
import io.cassandrareaper.jmx.JmxConnectionFactory;
import io.cassandrareaper.jmx.JmxProxy;
import io.cassandrareaper.storage.CassandraStorage;
import io.cassandrareaper.storage.MemoryStorage;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.management.JMException;

import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;

public final class HeartTest {

  @Test
  public void testBeat_nullStorage() {
    AppContext context = new AppContext();
    try (Heart heart = Heart.create(context)) {
      heart.beat();
      Assertions.assertThat(heart.isCurrentlyUpdatingNodeMetrics().get()).isFalse();
    }
  }

  @Test
  public void testBeat_memoryStorage() {
    AppContext context = new AppContext();
    context.storage = new MemoryStorage();
    try (Heart heart = Heart.create(context)) {
      heart.beat();
      Assertions.assertThat(heart.isCurrentlyUpdatingNodeMetrics().get()).isFalse();
    }
  }

  @Test
  public void testBeat_distributedStorage_noDatacenterAvailability() throws InterruptedException {

    AppContext context = new AppContext();
    context.config = new ReaperApplicationConfiguration();
    context.storage = Mockito.mock(CassandraStorage.class);

    try (Heart heart = Heart.create(context)) {
      heart.beat();
      Awaitility.await().until(() -> {
        try {
          Mockito.verify((CassandraStorage)context.storage, Mockito.times(1)).saveHeartbeat();
          return true;
        } catch (AssertionError ex) {
          return false;
        }
      });
      Assertions.assertThat(heart.isCurrentlyUpdatingNodeMetrics().get()).isFalse();
      Thread.sleep(500);
    }

    Mockito.verify(context.storage, Mockito.times(0)).getClusters();
    Mockito.verify((CassandraStorage)context.storage, Mockito.times(0)).storeNodeMetrics(any(), any());
  }

  @Test
  public void testBeat_distributedStorage_allDatacenterAvailability() throws InterruptedException {

    AppContext context = new AppContext();
    context.config = new ReaperApplicationConfiguration();
    context.config.setDatacenterAvailability(ReaperApplicationConfiguration.DatacenterAvailability.ALL);
    context.storage = Mockito.mock(CassandraStorage.class);

    try (Heart heart = Heart.create(context)) {
      heart.beat();
      Awaitility.await().until(() -> {
        try {
          Mockito.verify((CassandraStorage)context.storage, Mockito.times(1)).saveHeartbeat();
          return true;
        } catch (AssertionError ex) {
          return false;
        }
      });
      Assertions.assertThat(heart.isCurrentlyUpdatingNodeMetrics().get()).isFalse();
      Thread.sleep(500);
    }
    Mockito.verify((CassandraStorage)context.storage, Mockito.times(1)).saveHeartbeat();
    Mockito.verify(context.storage, Mockito.times(0)).getClusters();
    Mockito.verify((CassandraStorage)context.storage, Mockito.times(0)).storeNodeMetrics(any(), any());
  }

  @Test
  public void testBeat_distributedStorage_eachDatacenterAvailability() throws InterruptedException {

    AppContext context = new AppContext();
    context.config = new ReaperApplicationConfiguration();
    context.config.setDatacenterAvailability(ReaperApplicationConfiguration.DatacenterAvailability.EACH);
    context.storage = Mockito.mock(CassandraStorage.class);
    context.jmxConnectionFactory = new JmxConnectionFactory(context.metricRegistry);

    try (Heart heart = Heart.create(context)) {
      heart.beat();
      Thread.sleep(500);
    }
    Mockito.verify((CassandraStorage)context.storage, Mockito.times(1)).saveHeartbeat();
    Mockito.verify((CassandraStorage)context.storage, Mockito.times(0)).storeNodeMetrics(any(), any());
  }

  @Test
  public void testBeat_distributedStorage_eachDatacenterAvailability_repairs() throws InterruptedException {

    AppContext context = new AppContext();
    context.config = new ReaperApplicationConfiguration();
    context.config.setDatacenterAvailability(ReaperApplicationConfiguration.DatacenterAvailability.EACH);

    context.repairManager = RepairManager.create(context);
    context.repairManager.repairRunners.put(UUID.randomUUID(), Mockito.mock(RepairRunner.class));
    context.repairManager.repairRunners.put(UUID.randomUUID(), Mockito.mock(RepairRunner.class));

    context.storage = Mockito.mock(CassandraStorage.class);
    context.jmxConnectionFactory = new JmxConnectionFactory(context.metricRegistry);

    try (Heart heart = Heart.create(context)) {
      heart.beat();
      Thread.sleep(500);
    }
    Mockito.verify((CassandraStorage)context.storage, Mockito.times(1)).saveHeartbeat();
    Mockito.verify((CassandraStorage)context.storage, Mockito.times(0)).storeNodeMetrics(any(), any());
  }

  @Test
  public void testBeat_distributedStorage_eachDatacenterAvailability_repairs_noMetrics()
      throws InterruptedException, ReaperException {

    AppContext context = new AppContext();
    context.config = new ReaperApplicationConfiguration();
    context.config.setDatacenterAvailability(ReaperApplicationConfiguration.DatacenterAvailability.EACH);

    context.repairManager = RepairManager.create(context);
    context.repairManager.repairRunners.put(UUID.randomUUID(), Mockito.mock(RepairRunner.class));
    context.repairManager.repairRunners.put(UUID.randomUUID(), Mockito.mock(RepairRunner.class));

    context.storage = Mockito.mock(CassandraStorage.class);
    context.jmxConnectionFactory = Mockito.mock(JmxConnectionFactory.class);

    Mockito
        .when(((CassandraStorage)context.storage).getNodeMetrics(any()))
        .thenReturn(Collections.emptyList());

    try (Heart heart = Heart.create(context)) {
      heart.beat();
      Thread.sleep(500);
    }

    Mockito.verify((CassandraStorage)context.storage, Mockito.times(1)).saveHeartbeat();
    Mockito.verify((CassandraStorage)context.storage, Mockito.times(2)).getNodeMetrics(any());
    Mockito.verify(context.jmxConnectionFactory, Mockito.times(0)).connect(any(), anyInt());
    Mockito.verify((CassandraStorage)context.storage, Mockito.times(0)).storeNodeMetrics(any(), any());
  }

  @Test
  public void testBeat_distributedStorage_eachDatacenterAvailability_repairs_noRequests()
      throws InterruptedException, ReaperException {

    AppContext context = new AppContext();
    context.config = new ReaperApplicationConfiguration();
    context.config.setDatacenterAvailability(ReaperApplicationConfiguration.DatacenterAvailability.EACH);
    context.storage = Mockito.mock(CassandraStorage.class);

    context.repairManager = RepairManager.create(context);
    context.repairManager.repairRunners.put(UUID.randomUUID(), Mockito.mock(RepairRunner.class));
    context.repairManager.repairRunners.put(UUID.randomUUID(), Mockito.mock(RepairRunner.class));

    context.storage = Mockito.mock(CassandraStorage.class);
    context.jmxConnectionFactory = Mockito.mock(JmxConnectionFactory.class);

    Mockito
        .when(((CassandraStorage)context.storage).getNodeMetrics(any()))
        .thenReturn(
            Collections.singleton(
                NodeMetrics.builder()
                    .withNode("test")
                    .withDatacenter("dc1")
                    .withCluster("cluster1")
            .build()));

    JmxProxy nodeProxy = Mockito.mock(JmxProxy.class);

    Mockito
        .when(context.jmxConnectionFactory.connect(any(), eq(context.config.getJmxConnectionTimeoutInSeconds())))
        .thenReturn(nodeProxy);

    HostConnectionCounters hostConnectionCounters = Mockito.mock(HostConnectionCounters.class);
    Mockito.when(context.jmxConnectionFactory.getHostConnectionCounters()).thenReturn(hostConnectionCounters);

    Mockito
      .when(context.jmxConnectionFactory.connect(any(), eq(context.config.getJmxConnectionTimeoutInSeconds())))
      .thenThrow(InterruptedException.class);

    try (Heart heart = Heart.create(context)) {
      heart.beat();
      Thread.sleep(500);
    }

    Mockito.verify((CassandraStorage)context.storage, Mockito.times(1)).saveHeartbeat();
    Mockito.verify((CassandraStorage)context.storage, Mockito.times(2)).getNodeMetrics(any());
    Mockito.verify(context.jmxConnectionFactory, Mockito.times(0)).connect(any(), anyInt());
    Mockito.verify((CassandraStorage)context.storage, Mockito.times(0)).storeNodeMetrics(any(), any());
  }

  @Test
  public void testBeat_distributedStorage_eachDatacenterAvailability_repairs_requests()
      throws InterruptedException, ReaperException {

    AppContext context = new AppContext();
    context.config = new ReaperApplicationConfiguration();
    context.config.setDatacenterAvailability(ReaperApplicationConfiguration.DatacenterAvailability.EACH);
    context.storage = Mockito.mock(CassandraStorage.class);

    context.repairManager = RepairManager.create(context);
    context.repairManager.repairRunners.put(UUID.randomUUID(), Mockito.mock(RepairRunner.class));
    context.repairManager.repairRunners.put(UUID.randomUUID(), Mockito.mock(RepairRunner.class));

    context.storage = Mockito.mock(CassandraStorage.class);
    context.jmxConnectionFactory = Mockito.mock(JmxConnectionFactory.class);

    Mockito
        .when(((CassandraStorage)context.storage).getNodeMetrics(any()))
        .thenReturn(
            Collections.singleton(
                NodeMetrics.builder()
                    .withNode("test")
                    .withDatacenter("dc1")
                    .withCluster("cluster1")
                    .withRequested(true)
            .build()));

    JmxProxy nodeProxy = Mockito.mock(JmxProxy.class);

    Mockito.when(context.jmxConnectionFactory.connect(any(), anyInt())).thenReturn(nodeProxy);

    try (Heart heart = Heart.create(context)) {
      heart.beat();
      Thread.sleep(500);
    }

    Mockito.verify((CassandraStorage)context.storage, Mockito.times(1)).saveHeartbeat();
    Mockito.verify((CassandraStorage)context.storage, Mockito.times(2)).getNodeMetrics(any());
    Mockito.verify(context.jmxConnectionFactory, Mockito.times(2)).connect(any(), anyInt());
    Mockito.verify((CassandraStorage)context.storage, Mockito.times(2)).storeNodeMetrics(any(), any());
  }

  @Test
  public void testBeat_distributedStorage_eachDatacenterAvailability_repairs_requests_queued()
      throws InterruptedException, ReaperException, JMException {

    AppContext context = new AppContext();
    context.config = new ReaperApplicationConfiguration();
    context.config.setDatacenterAvailability(ReaperApplicationConfiguration.DatacenterAvailability.EACH);
    context.storage = Mockito.mock(CassandraStorage.class);

    context.repairManager = RepairManager.create(context);
    context.repairManager.repairRunners.put(UUID.randomUUID(), Mockito.mock(RepairRunner.class));
    context.repairManager.repairRunners.put(UUID.randomUUID(), Mockito.mock(RepairRunner.class));

    context.storage = Mockito.mock(CassandraStorage.class);
    context.jmxConnectionFactory = Mockito.mock(JmxConnectionFactory.class);

    Mockito
        .when(((CassandraStorage)context.storage).getNodeMetrics(any()))
        .thenReturn(
            Collections.singleton(
                NodeMetrics.builder()
                    .withNode("test")
                    .withDatacenter("dc1")
                    .withCluster("cluster1")
                    .withRequested(true)
            .build()));

    JmxProxy nodeProxy = Mockito.mock(JmxProxy.class);
    Mockito.when(context.jmxConnectionFactory.connect(any(), anyInt())).thenReturn(nodeProxy);

    Mockito.when(nodeProxy.getPendingCompactions())
        .then(a -> {
          // delay the call so force the forkJoinPool queue
          Thread.sleep(2501);
          return 10;
        });

    try (Heart heart = Heart.create(context, TimeUnit.SECONDS.toMillis(2))) {
      heart.beat();
      Assertions.assertThat(heart.isCurrentlyUpdatingNodeMetrics().get()).isTrue();
      Thread.sleep(2100);
      heart.beat();
      Thread.sleep(500);
    }

    Mockito.verify((CassandraStorage)context.storage, Mockito.times(2)).saveHeartbeat();
    Mockito.verify((CassandraStorage)context.storage, Mockito.times(2)).getNodeMetrics(any());
    Mockito.verify(context.jmxConnectionFactory, Mockito.times(2)).connect(any(), anyInt());
    Mockito.verify((CassandraStorage)context.storage, Mockito.times(2)).storeNodeMetrics(any(), any());
  }
}
