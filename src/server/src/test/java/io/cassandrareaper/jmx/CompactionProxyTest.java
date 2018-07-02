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

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.service.StorageServiceMBean;
import org.awaitility.Awaitility;
import org.junit.Test;
import org.mockito.Mockito;




public final class CompactionProxyTest {


  @Test
  public void testForceCompaction() throws IOException, ExecutionException, InterruptedException {
    AppContext context = new AppContext();
    JmxProxyImpl proxy = Mockito.mock(JmxProxyImpl.class);
    StorageServiceMBean storageMBean = Mockito.mock(StorageServiceMBean.class);
    Mockito.when(proxy.getStorageServiceMBean()).thenReturn(storageMBean);

    CompactionProxy compactionProxy = CompactionProxy.create(proxy, context.metricRegistry);
    compactionProxy.forceCompaction("test_keyspace", "test_table1", "test_table2");

    Awaitility.await().with().atMost(5, TimeUnit.SECONDS).until(() -> {
      try {
        Mockito.verify(proxy, Mockito.atLeastOnce()).getStorageServiceMBean();
        Mockito.verify(storageMBean).forceKeyspaceCompaction(false, "test_keyspace", "test_table1", "test_table2");
        return true;
      } catch (AssertionError ex) {
        return false;
      }
    });

    Mockito.verify(proxy, Mockito.atLeastOnce()).getStorageServiceMBean();
    Mockito.verify(storageMBean).forceKeyspaceCompaction(false, "test_keyspace", "test_table1", "test_table2");
  }

}
