/*
 * Copyright 2023-2023 DataStax, Inc.
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

package io.cassandrareaper.management.http;

import io.cassandrareaper.core.Snapshot;
import io.cassandrareaper.management.RepairStatusHandler;
import io.cassandrareaper.management.http.models.JobStatusTracker;

import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;

import com.codahale.metrics.MetricRegistry;
import com.datastax.mgmtapi.client.api.DefaultApi;
import com.datastax.mgmtapi.client.model.SnapshotDetails;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.commons.lang3.concurrent.ConcurrentUtils;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class HttpCassandraManagementProxyTest {

  @Test
  public void testConvertSnapshots() throws Exception {
    DefaultApi mockClient = Mockito.mock(DefaultApi.class);
    ScheduledExecutorService executorService = Mockito.mock(ScheduledExecutorService.class);
    Mockito.doReturn(ConcurrentUtils.constantFuture(null)).when(executorService).submit(any(Callable.class));
    HttpCassandraManagementProxy proxy = new HttpCassandraManagementProxy(
        null, "/", InetSocketAddress.createUnresolved("localhost", 8080), executorService, mockClient);
    SnapshotDetails details = jsonFromResourceFile("example_snapshot_details.json", SnapshotDetails.class);
    List<Snapshot> snapshots = proxy.convertSnapshots(details);
    assertEquals(3, snapshots.size());
    // we have 3 sets of snapshot data
    Snapshot snapshot0 = null;
    Snapshot snapshot1 = null;
    Snapshot snapshot2 = null;
    for (int i = 0; i < snapshots.size(); ++i) {
      Snapshot snapshot = snapshots.get(i);
      switch (snapshot.getTable()) {
        case "booya0":
          snapshot0 = snapshot;
          break;
        case "booya1":
          snapshot1 = snapshot;
          break;
        case "booya2":
          snapshot2 = snapshot;
          break;
        default:
          fail("Unexpected Table name for snapshot: " + snapshot.getTable());
          break;
      }
    }
    // make sure all 3 snapshots have been found
    assertNotNull(snapshot0);
    assertNotNull(snapshot1);
    assertNotNull(snapshot2);
    // assert common values
    for (Snapshot snapshot : snapshots) {
      assertEquals("localhost", snapshot.getHost());
      assertEquals("testSnapshot", snapshot.getName());
      assertEquals("booya", snapshot.getKeyspace());
    }
    // assert sizes
    assertEquals("Size on disk mismatch", 122122.24, snapshot0.getSizeOnDisk(), 0d);
    assertEquals("Size on disk mismatch", 8488.96, snapshot1.getSizeOnDisk(), 0d);
    assertEquals("Size on disk mismatch", 8468.48, snapshot2.getSizeOnDisk(), 0d);
    assertEquals("True size mismatch", 413d, snapshot0.getTrueSize(), 0d);
    assertEquals("True size mismatch", 256d, snapshot1.getTrueSize(), 0d);
    assertEquals("True size mismatch", 10d, snapshot2.getTrueSize(), 0d);
  }

  private <T extends Object> T jsonFromResourceFile(String filename, Class<T> clazz) throws Exception {
    return new ObjectMapper().readValue(
        this.getClass().getResource(filename).openStream(), clazz);
  }

  @Test
  public void testRepairHandlers() throws Exception {
    HttpManagementConnectionFactory connectionFactory = Mockito.mock(HttpManagementConnectionFactory.class);
    DefaultApi mockClient = Mockito.mock(DefaultApi.class);
    Mockito.doReturn("repair-123456789").when(mockClient).repair1(any());
    ScheduledExecutorService executorService = Mockito.mock(ScheduledExecutorService.class);
    Mockito.doReturn(ConcurrentUtils.constantFuture(null)).when(executorService).submit(any(Callable.class));

    HttpCassandraManagementProxy httpCassandraManagementProxy = new HttpCassandraManagementProxy(
        Mockito.mock(MetricRegistry.class), "",
        Mockito.mock(InetSocketAddress.class), executorService, mockClient);
    when(connectionFactory.connectAny(any())).thenReturn(httpCassandraManagementProxy);

    RepairStatusHandler repairStatusHandler = Mockito.mock(RepairStatusHandler.class);

    int repairNo = httpCassandraManagementProxy.triggerRepair(BigInteger.ZERO, BigInteger.ONE, "ks",
        RepairParallelism.PARALLEL,
        Collections.singleton("table"), true, Collections.emptyList(), repairStatusHandler, Collections.emptyList(), 1);

    assertEquals(123456789, repairNo);
    assertEquals(1, httpCassandraManagementProxy.jobTracker.size());
    String jobId = String.format("repair-%d", repairNo);
    assertTrue(httpCassandraManagementProxy.jobTracker.containsKey(jobId));
    JobStatusTracker jobStatus = httpCassandraManagementProxy.jobTracker.get(jobId);
    assertEquals(0, jobStatus.latestNotificationCount.get());
    assertEquals(1, httpCassandraManagementProxy.repairStatusExecutors.size());
    assertEquals(1, httpCassandraManagementProxy.repairStatusHandlers.size());
    assertTrue(httpCassandraManagementProxy.repairStatusHandlers.containsKey(repairNo));

    httpCassandraManagementProxy.removeRepairStatusHandler(repairNo);
    assertEquals(0, httpCassandraManagementProxy.jobTracker.size());
    assertEquals(0, httpCassandraManagementProxy.repairStatusExecutors.size());
    assertEquals(0, httpCassandraManagementProxy.repairStatusHandlers.size());
  }
}
