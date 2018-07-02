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


import java.lang.reflect.Field;
import javax.management.MBeanServerConnection;

import com.google.common.collect.ImmutableMap;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.fest.assertions.api.Assertions;
import org.junit.Test;
import org.mockito.Mockito;


public final class ColumnFamilyStoreProxyTest {

  @Test
  public void testCompactionStrategy() throws Exception {

    JmxProxyImpl proxy = Mockito.mock(JmxProxyImpl.class);
    MBeanServerConnection mbeanServerConnection = Mockito.mock(MBeanServerConnection.class);
    ColumnFamilyStoreMBean mbean = Mockito.mock(ColumnFamilyStoreMBean.class);
    Mockito.when(proxy.getMBeanServerConnection()).thenReturn(mbeanServerConnection);
    Mockito.when(mbean.getCompactionParameters()).thenReturn(ImmutableMap.of("class", "test-compaction"));

    ColumnFamilyStoreProxy cfsProxy = ColumnFamilyStoreProxy.create("test_keyspace", "test_table1", proxy);
    setColumnFamilyStoreMBean(cfsProxy, mbean);
    Assertions.assertThat(cfsProxy.getCompactionStrategyClass()).isEqualTo("test-compaction");
  }

  public static void setColumnFamilyStoreMBean(ColumnFamilyStoreProxy proxy, ColumnFamilyStoreMBean mbean)
      throws ReflectiveOperationException, SecurityException {

    Field field = ColumnFamilyStoreProxy.class.getDeclaredField("mbean");
    field.setAccessible(true);
    field.set(proxy, mbean);
  }
}
