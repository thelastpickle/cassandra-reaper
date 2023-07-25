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
import java.util.Random;

import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;

public final class CassandraManagementProxyTest {

  public static ICassandraManagementProxy mockJmxProxyImpl() throws UnknownHostException {
    JmxCassandraManagementProxy impl = Mockito.mock(JmxCassandraManagementProxy.class);
    Mockito.when(impl.getUntranslatedHost()).thenReturn("test-host-" + new Random().nextInt());
    Mockito.when(impl.getDatacenter(any())).thenReturn("dc1");
    return impl;
  }

  @Test
  public void testVersionCompare() throws ReaperException {
    assertEquals(Integer.valueOf(0), ICassandraManagementProxy.versionCompare("1.0", "1.0"));
    assertEquals(Integer.valueOf(0), ICassandraManagementProxy.versionCompare("1000.999", "1000.999"));
    assertEquals(Integer.valueOf(-1), ICassandraManagementProxy.versionCompare("1.0", "1.1"));
    assertEquals(Integer.valueOf(-1), ICassandraManagementProxy.versionCompare("1.2", "2.1"));
    assertEquals(Integer.valueOf(1), ICassandraManagementProxy.versionCompare("10.0.0", "1.0"));
    assertEquals(Integer.valueOf(1), ICassandraManagementProxy.versionCompare("99.0.0", "9.0"));
    assertEquals(Integer.valueOf(1), ICassandraManagementProxy.versionCompare("99.0.10", "99.0.1"));
    assertEquals(Integer.valueOf(-1), ICassandraManagementProxy.versionCompare("99.0.10~1", "99.0.10~2"));
    assertEquals(Integer.valueOf(-1), ICassandraManagementProxy.versionCompare("2.0.17", "2.1.1"));
  }

}