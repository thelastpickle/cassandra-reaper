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

import io.cassandrareaper.ReaperException;

import java.net.UnknownHostException;
import java.util.Optional;
import java.util.Random;

import javax.management.MBeanServerConnection;

import com.google.common.base.Preconditions;
import org.apache.cassandra.db.compaction.CompactionManagerMBean;
import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.service.StorageServiceMBean;
import org.apache.cassandra.streaming.StreamManagerMBean;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;

public final class JmxProxyTest {

  public static JmxProxy mockJmxProxyImpl() throws UnknownHostException {
    JmxProxyImpl impl = Mockito.mock(JmxProxyImpl.class);
    Mockito.when(impl.getUntranslatedHost()).thenReturn("test-host-" + new Random().nextInt());
    EndpointSnitchInfoMBean endpointSnitchInfoMBean = Mockito.mock(EndpointSnitchInfoMBean.class);
    Mockito.when(endpointSnitchInfoMBean.getDatacenter(any())).thenReturn("dc1");
    Mockito.when(impl.getEndpointSnitchInfoMBean()).thenReturn(endpointSnitchInfoMBean);
    return impl;
  }

  public static void mockGetMBeanServerConnection(JmxProxy proxy, MBeanServerConnection serverConnection) {
    Preconditions.checkArgument(proxy instanceof JmxProxyImpl, "only JmxProxyImpl is supported");
    Mockito.when(((JmxProxyImpl)proxy).getMBeanServerConnection()).thenReturn(serverConnection);
  }

  public static void mockGetStorageServiceMBean(JmxProxy proxy, StorageServiceMBean storageMBean) {
    Preconditions.checkArgument(proxy instanceof JmxProxyImpl, "only JmxProxyImpl is supported");
    Mockito.when(((JmxProxyImpl)proxy).getStorageServiceMBean()).thenReturn(storageMBean);
  }

  public static void mockGetStreamManagerMBean(JmxProxy proxy, StreamManagerMBean streamingManagerMBean) {
    Preconditions.checkArgument(proxy instanceof JmxProxyImpl, "only JmxProxyImpl is supported");
    Mockito.when(((JmxProxyImpl)proxy).getStreamManagerMBean()).thenReturn(Optional.of(streamingManagerMBean));
  }

  public static void mockGetEndpointSnitchInfoMBean(JmxProxy proxy, EndpointSnitchInfoMBean endpointSnitchInfoMBean) {
    Preconditions.checkArgument(proxy instanceof JmxProxyImpl, "only JmxProxyImpl is supported");
    Mockito.when(((JmxProxyImpl)proxy).getEndpointSnitchInfoMBean()).thenReturn(endpointSnitchInfoMBean);
  }

  public static void mockGetCompactionManagerMBean(JmxProxy proxy, CompactionManagerMBean compactionManagerMBean) {
    Preconditions.checkArgument(proxy instanceof JmxProxyImpl, "only JmxProxyImpl is supported");
    Mockito.when(((JmxProxyImpl)proxy).getCompactionManagerMBean()).thenReturn(compactionManagerMBean);
  }

  @Test
  public void testVersionCompare() throws ReaperException {
    assertEquals(Integer.valueOf(0), JmxProxyImpl.versionCompare("1.0", "1.0"));
    assertEquals(Integer.valueOf(0), JmxProxyImpl.versionCompare("1000.999", "1000.999"));
    assertEquals(Integer.valueOf(-1), JmxProxyImpl.versionCompare("1.0", "1.1"));
    assertEquals(Integer.valueOf(-1), JmxProxyImpl.versionCompare("1.2", "2.1"));
    assertEquals(Integer.valueOf(1), JmxProxyImpl.versionCompare("10.0.0", "1.0"));
    assertEquals(Integer.valueOf(1), JmxProxyImpl.versionCompare("99.0.0", "9.0"));
    assertEquals(Integer.valueOf(1), JmxProxyImpl.versionCompare("99.0.10", "99.0.1"));
    assertEquals(Integer.valueOf(-1), JmxProxyImpl.versionCompare("99.0.10~1", "99.0.10~2"));
    assertEquals(Integer.valueOf(-1), JmxProxyImpl.versionCompare("2.0.17", "2.1.1"));
  }

}
