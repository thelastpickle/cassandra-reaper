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

package com.spotify.reaper.cassandra;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.cassandra.db.ColumnFamilyStoreMBean;

/**
 * This code is copied and adjusted from from NodeProbe.java from Cassandra source.
 */
final class ColumnFamilyStoreMBeanIterator implements Iterator<Map.Entry<String, ColumnFamilyStoreMBean>> {

  private final Iterator<ObjectName> resIter;
  private final MBeanServerConnection mbeanServerConn;

  ColumnFamilyStoreMBeanIterator(MBeanServerConnection mbeanServerConn)
      throws MalformedObjectNameException, NullPointerException, IOException {

    ObjectName query = new ObjectName("org.apache.cassandra.db:type=ColumnFamilies,*");
    resIter = mbeanServerConn.queryNames(query, null).iterator();
    this.mbeanServerConn = mbeanServerConn;
  }

  static Iterator<Map.Entry<String, ColumnFamilyStoreMBean>> getColumnFamilyStoreMBeanProxies(
      MBeanServerConnection mbeanServerConn) throws IOException, MalformedObjectNameException {

    return new ColumnFamilyStoreMBeanIterator(mbeanServerConn);
  }

  @Override
  public boolean hasNext() {
    return resIter.hasNext();
  }

  @Override
  public Map.Entry<String, ColumnFamilyStoreMBean> next() {
    ObjectName objectName = resIter.next();
    String keyspaceName = objectName.getKeyProperty("keyspace");
    ColumnFamilyStoreMBean cfsProxy = JMX.newMBeanProxy(mbeanServerConn, objectName, ColumnFamilyStoreMBean.class);
    return new AbstractMap.SimpleImmutableEntry<>(keyspaceName, cfsProxy);
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

}
