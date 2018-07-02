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

import javax.management.JMX;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import com.google.common.base.Preconditions;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;


public final class ColumnFamilyStoreProxy {

  private final ColumnFamilyStoreMBean mbean;

  private ColumnFamilyStoreProxy(JmxProxyImpl proxy, String keyspace, String table) {
    try {
      String name = String.format("org.apache.cassandra.db:type=Tables,keyspace=%s,table=%s", keyspace, table);
      mbean = JMX.newMBeanProxy(proxy.getMBeanServerConnection(), new ObjectName(name), ColumnFamilyStoreMBean.class);
    } catch (MalformedObjectNameException ex) {
      throw new IllegalStateException(String.format("failed on ColumnFamilyStoreMBean(%s.%s)", keyspace, table), ex);
    }
  }

  public static ColumnFamilyStoreProxy create(String keyspaceName, String tableName, JmxProxy proxy) {
    Preconditions.checkArgument(proxy instanceof JmxProxyImpl, "only JmxProxyImpl is supported");
    return new ColumnFamilyStoreProxy((JmxProxyImpl)proxy, keyspaceName, tableName);
  }

  public String getCompactionStrategyClass() {
    return mbean.getCompactionParameters().get("class");
  }

}
