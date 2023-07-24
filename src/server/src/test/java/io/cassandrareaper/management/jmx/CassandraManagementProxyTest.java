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

import io.cassandrareaper.ReaperException;
import io.cassandrareaper.management.ICassandraManagementProxy;

import java.net.UnknownHostException;
import java.util.Optional;
import java.util.Random;

import org.apache.cassandra.db.compaction.CompactionManagerMBean;
import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.streaming.StreamManagerMBean;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;

public final class CassandraManagementProxyTest {

  public static ICassandraManagementProxy mockJmxProxyImpl() throws UnknownHostException {
    JmxCassandraManagementProxy impl = Mockito.mock(JmxCassandraManagementProxy.class);
    Mockito.when(impl.getUntranslatedHost()).thenReturn("test-host-" + new Random().nextInt());
    EndpointSnitchInfoMBean endpointSnitchInfoMBean = Mockito.mock(EndpointSnitchInfoMBean.class);
    Mockito.when(endpointSnitchInfoMBean.getDatacenter(any())).thenReturn("dc1");
    Mockito.when(impl.getEndpointSnitchInfoMBean()).thenReturn(endpointSnitchInfoMBean);
    return impl;
  }

  public static void mockGetStreamManagerMBean(ICassandraManagementProxy proxy,
                                               StreamManagerMBean streamingManagerMBean) {
    Mockito.when(((JmxCassandraManagementProxy) proxy).getStreamManagerMBean())
        .thenReturn(Optional.of(streamingManagerMBean));
  }

  public static void mockGetEndpointSnitchInfoMBean(ICassandraManagementProxy proxy,
                                                    EndpointSnitchInfoMBean endpointSnitchInfoMBean) {

    Mockito.when(((JmxCassandraManagementProxy) proxy).getEndpointSnitchInfoMBean())
        .thenReturn(endpointSnitchInfoMBean);
  }

  public static void mockGetCompactionManagerMBean(ICassandraManagementProxy proxy,
                                                   CompactionManagerMBean compactionManagerMBean) {

    Mockito.when(((JmxCassandraManagementProxy) proxy).getCompactionManagerMBean()).thenReturn(compactionManagerMBean);
  }

  @Test
  public void testVersionCompare() throws ReaperException {
    assertEquals(Integer.valueOf(0), JmxCassandraManagementProxy.versionCompare("1.0", "1.0"));
    assertEquals(Integer.valueOf(0), JmxCassandraManagementProxy.versionCompare("1000.999", "1000.999"));
    assertEquals(Integer.valueOf(-1), JmxCassandraManagementProxy.versionCompare("1.0", "1.1"));
    assertEquals(Integer.valueOf(-1), JmxCassandraManagementProxy.versionCompare("1.2", "2.1"));
    assertEquals(Integer.valueOf(1), JmxCassandraManagementProxy.versionCompare("10.0.0", "1.0"));
    assertEquals(Integer.valueOf(1), JmxCassandraManagementProxy.versionCompare("99.0.0", "9.0"));
    assertEquals(Integer.valueOf(1), JmxCassandraManagementProxy.versionCompare("99.0.10", "99.0.1"));
    assertEquals(Integer.valueOf(-1), JmxCassandraManagementProxy.versionCompare("99.0.10~1", "99.0.10~2"));
    assertEquals(Integer.valueOf(-1), JmxCassandraManagementProxy.versionCompare("2.0.17", "2.1.1"));
  }

}