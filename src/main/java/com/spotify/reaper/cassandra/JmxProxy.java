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

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;

import com.spotify.reaper.ReaperException;
import com.spotify.reaper.core.Cluster;
import com.spotify.reaper.service.RingRange;

import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.CompactionManagerMBean;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageServiceMBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;
import javax.management.InstanceNotFoundException;
import javax.management.JMX;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import static com.google.common.base.Preconditions.checkNotNull;

public class JmxProxy implements NotificationListener, AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(JmxProxy.class);

  private static final int JMX_PORT = 7199;
  private static final String JMX_URL = "service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi";
  private static final String SS_OBJECT_NAME = "org.apache.cassandra.db:type=StorageService";
  private static final String AES_OBJECT_NAME =
      "org.apache.cassandra.internal:type=AntiEntropySessions";

  private final JMXConnector jmxConnector;
  private final ObjectName ssMbeanName;
  private final MBeanServerConnection mbeanServer;
  private final CompactionManagerMBean cmProxy;
  private final StorageServiceMBean ssProxy;
  private final Optional<RepairStatusHandler> repairStatusHandler;
  private final String host;
  private final String clusterName;

  private JmxProxy(Optional<RepairStatusHandler> handler, String host, JMXConnector jmxConnector,
                   StorageServiceMBean ssProxy, ObjectName ssMbeanName,
                   MBeanServerConnection mbeanServer, CompactionManagerMBean cmProxy) {
    this.host = host;
    this.jmxConnector = jmxConnector;
    this.ssMbeanName = ssMbeanName;
    this.mbeanServer = mbeanServer;
    this.ssProxy = ssProxy;
    this.repairStatusHandler = handler;
    this.cmProxy = cmProxy;
    this.clusterName = Cluster.toSymbolicName(ssProxy.getClusterName());
  }

  /**
   * Connect to JMX interface on the given host and default JMX port.
   *
   * @param handler Implementation of {@link RepairStatusHandler} to process incoming notifications
   *                of repair events.
   */
  public static JmxProxy connect(Optional<RepairStatusHandler> handler, String host)
      throws ReaperException {
    assert null != host : "null host given to JmxProxy.connect()";
    String[] parts = host.split(":");
    if (parts.length == 2) {
      return connect(handler, parts[0], Integer.valueOf(parts[1]));
    } else {
      return connect(handler, host, JMX_PORT);
    }
  }

  /**
   * Connect to JMX interface on the given host and port.
   *
   * @param handler Implementation of {@link RepairStatusHandler} to process incoming notifications
   *                of repair events.
   */
  public static JmxProxy connect(Optional<RepairStatusHandler> handler, String host, int port)
      throws ReaperException {
    JMXServiceURL jmxUrl;
    ObjectName ssMbeanName;
    ObjectName cmMbeanName;
    try {
      jmxUrl = new JMXServiceURL(String.format(JMX_URL, host, port));
      ssMbeanName = new ObjectName(SS_OBJECT_NAME);
      cmMbeanName = new ObjectName(CompactionManager.MBEAN_OBJECT_NAME);
    } catch (MalformedURLException | MalformedObjectNameException e) {
      LOG.error(String.format("Failed to prepare the JMX connection to %s:%s", host, port));
      throw new ReaperException("Failure during preparations for JMX connection", e);
    }
    try {
      JMXConnector jmxConn = JMXConnectorFactory.connect(jmxUrl);
      MBeanServerConnection mbeanServerConn = jmxConn.getMBeanServerConnection();
      StorageServiceMBean ssProxy =
          JMX.newMBeanProxy(mbeanServerConn, ssMbeanName, StorageServiceMBean.class);
      CompactionManagerMBean cmProxy =
          JMX.newMBeanProxy(mbeanServerConn, cmMbeanName, CompactionManagerMBean.class);
      JmxProxy proxy =
          new JmxProxy(handler, host, jmxConn, ssProxy, ssMbeanName, mbeanServerConn, cmProxy);
      // registering a listener throws bunch of exceptions, so we do it here rather than in the
      // constructor
      mbeanServerConn.addNotificationListener(ssMbeanName, proxy, null, null);
      LOG.debug(String.format("JMX connection to %s properly connected.", host));
      return proxy;
    } catch (IOException | InstanceNotFoundException e) {
      LOG.error(String.format("Failed to establish JMX connection to %s:%s", host, port));
      throw new ReaperException("Failure when establishing JMX connection", e);
    }
  }

  public String getHost() {
    return host;
  }

  /**
   * @return list of tokens in the cluster
   */
  public List<BigInteger> getTokens() {
    checkNotNull(ssProxy, "Looks like the proxy is not connected");
    return Lists.transform(
        Lists.newArrayList(ssProxy.getTokenToEndpointMap().keySet()),
        new Function<String, BigInteger>() {
          @Override
          public BigInteger apply(String s) {
            return new BigInteger(s);
          }
        });
  }

  /**
   * @return all hosts owning a range of tokens
   */
  public List<String> tokenRangeToEndpoint(String keyspace, RingRange tokenRange) {
    checkNotNull(ssProxy, "Looks like the proxy is not connected");
    Set<Map.Entry<List<String>, List<String>>> entries =
        ssProxy.getRangeToEndpointMap(keyspace).entrySet();
    for (Map.Entry<List<String>, List<String>> entry : entries) {
      BigInteger rangeStart = new BigInteger(entry.getKey().get(0));
      BigInteger rangeEnd = new BigInteger(entry.getKey().get(1));
      if (new RingRange(rangeStart, rangeEnd).encloses(tokenRange)) {
        return entry.getValue();
      }
    }
    return Lists.newArrayList();
  }

  /**
   * @return full class name of Cassandra's partitioner.
   */
  public String getPartitioner() {
    checkNotNull(ssProxy, "Looks like the proxy is not connected");
    return ssProxy.getPartitionerName();
  }

  /**
   * @return Cassandra cluster name.
   */
  public String getClusterName() {
    checkNotNull(ssProxy, "Looks like the proxy is not connected");
    return ssProxy.getClusterName();
  }

  /**
   * @return list of available keyspaces
   */
  public List<String> getKeyspaces() {
    checkNotNull(ssProxy, "Looks like the proxy is not connected");
    return ssProxy.getKeyspaces();
  }

  public Set<String> getTableNamesForKeyspace(String keyspace) throws ReaperException {
    Set<String> tableNames = new HashSet<>();
    Iterator<Map.Entry<String, ColumnFamilyStoreMBean>> proxies;
    try {
      proxies = ColumnFamilyStoreMBeanIterator.getColumnFamilyStoreMBeanProxies(mbeanServer);
    } catch (IOException | MalformedObjectNameException e) {
      e.printStackTrace();
      throw new ReaperException("failed to get ColumnFamilyStoreMBean instances from JMX");
    }
    while (proxies.hasNext()) {
      Map.Entry<String, ColumnFamilyStoreMBean> proxyEntry = proxies.next();
      String keyspaceName = proxyEntry.getKey();
      if (keyspace.equalsIgnoreCase(keyspaceName)) {
        ColumnFamilyStoreMBean columnFamilyMBean = proxyEntry.getValue();
        tableNames.add(columnFamilyMBean.getColumnFamilyName());
      }
    }
    return tableNames;
  }

  /**
   * @return number of pending compactions on the node this proxy is connected to
   */
  public int getPendingCompactions() {
    checkNotNull(cmProxy, "Looks like the proxy is not connected");
    return cmProxy.getPendingTasks();
  }

  /**
   * @return true if any repairs are running on the node.
   */
  public boolean isRepairRunning() {
    // Check if AntiEntropySession is actually running on the node
    try {
      ObjectName name = new ObjectName(AES_OBJECT_NAME);
      int activeCount = (Integer) mbeanServer.getAttribute(name, "ActiveCount");
      long pendingCount = (Long) mbeanServer.getAttribute(name, "PendingTasks");
      return activeCount + pendingCount != 0;
    } catch (IOException ignored) {
      LOG.warn("Failed to connect to " + host + " using JMX");
    } catch (MalformedObjectNameException ignored) {
      LOG.error("Internal error, malformed name");
    } catch (InstanceNotFoundException e) {
      // This happens if no repair has yet been run on the node
      // The AntiEntropySessions object is created on the first repair
      return false;
    } catch (Exception e) {
      LOG.error("Error getting attribute from JMX", e);
    }
    // If uncertain, assume it's running
    return true;
  }

  /**
   * Terminates all ongoing repairs on the node this proxy is connected to
   */
  public void cancelAllRepairs() {
    checkNotNull(ssProxy, "Looks like the proxy is not connected");
    ssProxy.forceTerminateAllRepairSessions();
  }

  /**
   * Checks if table exists in the cluster by instantiating a MBean for that table.
   *
   * @throws ReaperException if the query fails, not when the table doesn't exist
   */
  public boolean tableExists(String ks, String cf) throws ReaperException {
    try {
      String type = cf.contains(".") ? "IndexColumnFamilies" : "ColumnFamilies";
      String nameStr = String.format("org.apache.cassandra.db:type=*%s,keyspace=%s,columnfamily=%s",
                                     type, ks, cf);
      Set<ObjectName> beans = mbeanServer.queryNames(new ObjectName(nameStr), null);
      if (beans.isEmpty() || beans.size() != 1) {
        return false;
      }
      ObjectName bean = beans.iterator().next();
      JMX.newMBeanProxy(mbeanServer, bean, ColumnFamilyStoreMBean.class);
    } catch (MalformedObjectNameException | IOException e) {
      String errMsg = String.format("ColumnFamilyStore for %s/%s not found: %s", ks, cf,
                                    e.getMessage());
      LOG.warn(errMsg);
      return false;
    }
    return true;
  }

  /**
   * Triggers a repair of range (beginToken, endToken] for given keyspace and column family.
   * The repair is triggered by {@link org.apache.cassandra.service.StorageServiceMBean#forceRepairRangeAsync}
   * For time being, we don't allow local nor snapshot repairs.
   *
   * @return Repair command number, or 0 if nothing to repair
   */
  public int triggerRepair(BigInteger beginToken, BigInteger endToken, String keyspace,
                           RepairParallelism repairParallelism, Collection<String> columnFamilies) {
    checkNotNull(ssProxy, "Looks like the proxy is not connected");
    String cassandraVersion = ssProxy.getReleaseVersion();
    boolean canUseDatacenterAware = false;
    try {
      canUseDatacenterAware = versionCompare(cassandraVersion, "2.0.12") >= 0;
    } catch (ReaperException e) {
      LOG.warn("failed on version comparison, not using dc aware repairs by default");
    }
    String msg = String.format("Triggering repair of range (%s,%s] for keyspace \"%s\" on "
                               + "host %s, with repair parallelism %s, in cluster with Cassandra "
                               + "version '%s' (can use DATACENTER_AWARE '%s'), "
                               + "for column families: %s",
                               beginToken.toString(), endToken.toString(), keyspace, this.host,
                               repairParallelism, cassandraVersion, canUseDatacenterAware,
                               columnFamilies);
    LOG.info(msg);
    if (repairParallelism.equals(RepairParallelism.DATACENTER_AWARE)) {
      if (canUseDatacenterAware) {
        return ssProxy.forceRepairRangeAsync(beginToken.toString(), endToken.toString(), keyspace,
                                             repairParallelism.ordinal(), null, null,
                                             columnFamilies
                                                 .toArray(new String[columnFamilies.size()]));
      } else {
        LOG.info("Cannot use DATACENTER_AWARE repair policy for Cassandra cluster with version {},"
                 + " falling back to SEQUENTIAL repair.",
                 cassandraVersion);
        repairParallelism = RepairParallelism.SEQUENTIAL;
      }
    }
    boolean snapshotRepair = repairParallelism.equals(RepairParallelism.SEQUENTIAL);
    return ssProxy.forceRepairRangeAsync(beginToken.toString(), endToken.toString(), keyspace,
                                         snapshotRepair, false,
                                         columnFamilies.toArray(new String[columnFamilies.size()]));
  }

  /**
   * Invoked when the MBean this class listens to publishes an event.
   * We're only interested in repair-related events.
   * Their format is explained at {@link org.apache.cassandra.service.StorageServiceMBean#forceRepairAsync}
   * The format is: notification type: "repair" notification userData: int array of length 2 where
   * [0] = command number [1] = ordinal of AntiEntropyService.Status
   */
  @Override
  public void handleNotification(Notification notification, Object handback) {
    Thread.currentThread().setName(clusterName);
    // we're interested in "repair"
    String type = notification.getType();
    LOG.debug("Received notification: {}", notification.toString());
    if (repairStatusHandler.isPresent() && type.equals("repair")) {
      int[] data = (int[]) notification.getUserData();
      // get the repair sequence number
      int repairNo = data[0];
      // get the repair status
      ActiveRepairService.Status status = ActiveRepairService.Status.values()[data[1]];
      // this is some text message like "Starting repair...", "Finished repair...", etc.
      String message = notification.getMessage();
      // let the handler process the event
      repairStatusHandler.get().handle(repairNo, status, message);
    }
  }

  public String getConnectionId() throws IOException {
    return jmxConnector.getConnectionId();
  }

  public boolean isConnectionAlive() {
    try {
      String connectionId = getConnectionId();
      return null != connectionId && connectionId.length() > 0;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return false;
  }

  /**
   * Cleanly shut down by un-registering the listener and closing the JMX connection.
   */
  @Override
  public void close() throws ReaperException {
    try {
      mbeanServer.removeNotificationListener(ssMbeanName, this);
      jmxConnector.close();
    } catch (IOException | InstanceNotFoundException | ListenerNotFoundException e) {
      throw new ReaperException(e);
    }
  }

  /**
   * NOTICE: This code is loosely based on StackOverflow answer:
   * http://stackoverflow.com/questions/6701948/efficient-way-to-compare-version-strings-in-java
   *
   * Compares two version strings.
   *
   * Use this instead of String.compareTo() for a non-lexicographical
   * comparison that works for version strings. e.g. "1.10".compareTo("1.6").
   *
   * @param str1 a string of ordinal numbers separated by decimal points.
   * @param str2 a string of ordinal numbers separated by decimal points.
   * @return The result is a negative integer if str1 is _numerically_ less than str2.
   * The result is a positive integer if str1 is _numerically_ greater than str2.
   * The result is zero if the strings are _numerically_ equal.
   * It does not work if "1.10" is supposed to be equal to "1.10.0".
   */
  public static Integer versionCompare(String str1, String str2) throws ReaperException {
    try {
      str1 = str1.split(" ")[0].replaceAll("[-_~]", ".");
      str2 = str2.split(" ")[0].replaceAll("[-_~]", ".");
      String[] parts1 = str1.split("\\.");
      String[] parts2 = str2.split("\\.");
      int i = 0;
      // set index to first non-equal ordinal or length of shortest version string
      while (i < parts1.length && i < parts2.length) {
        try {
          Integer.parseInt(parts1[i]);
          Integer.parseInt(parts2[i]);
        } catch (NumberFormatException ex) {
          if (i == 0) {
            throw ex; // just comparing two non-version strings should fail
          }
          // first non integer part, so let's just stop comparison here and ignore the rest
          i--;
          break;
        }
        if (parts1[i].equals(parts2[i])) {
          i++;
          continue;
        }
        break;
      }
      // compare first non-equal ordinal number
      if (i < parts1.length && i < parts2.length) {
        int diff = Integer.valueOf(parts1[i]).compareTo(Integer.valueOf(parts2[i]));
        return Integer.signum(diff);
      }
      // the strings are equal or one string is a substring of the other
      // e.g. "1.2.3" = "1.2.3" or "1.2.3" < "1.2.3.4"
      else {
        return Integer.signum(parts1.length - parts2.length);
      }
    } catch (Exception ex) {
      LOG.error("failed comparing strings for versions: '{}' '{}'", str1, str2);
      throw new ReaperException(ex);
    }
  }
}


/**
 * This code is copied and adjusted from from NodeProbe.java from Cassandra source.
 */
class ColumnFamilyStoreMBeanIterator
    implements Iterator<Map.Entry<String, ColumnFamilyStoreMBean>> {

  private Iterator<ObjectName> resIter;
  private MBeanServerConnection mbeanServerConn;

  public ColumnFamilyStoreMBeanIterator(MBeanServerConnection mbeanServerConn)
      throws MalformedObjectNameException, NullPointerException, IOException {
    ObjectName query = new ObjectName("org.apache.cassandra.db:type=ColumnFamilies,*");
    resIter = mbeanServerConn.queryNames(query, null).iterator();
    this.mbeanServerConn = mbeanServerConn;
  }

  static Iterator<Map.Entry<String, ColumnFamilyStoreMBean>> getColumnFamilyStoreMBeanProxies(
      MBeanServerConnection mbeanServerConn)
      throws IOException, MalformedObjectNameException {
    return new ColumnFamilyStoreMBeanIterator(mbeanServerConn);
  }

  public boolean hasNext() {
    return resIter.hasNext();
  }

  public Map.Entry<String, ColumnFamilyStoreMBean> next() {
    ObjectName objectName = resIter.next();
    String keyspaceName = objectName.getKeyProperty("keyspace");
    ColumnFamilyStoreMBean cfsProxy =
        JMX.newMBeanProxy(mbeanServerConn, objectName, ColumnFamilyStoreMBean.class);
    return new AbstractMap.SimpleImmutableEntry<>(keyspaceName, cfsProxy);
  }

  public void remove() {
    throw new UnsupportedOperationException();
  }
}
