/*
 * Copyright 2018-2019 The Last Pickle Ltd
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

package io.cassandrareaper.management.jmx;

import io.cassandrareaper.core.GenericMetric;
import io.cassandrareaper.core.JmxStat;
import io.cassandrareaper.core.Node;
import io.cassandrareaper.management.MetricsProxy;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.management.JMException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.ObjectName;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JmxMetricsProxy implements MetricsProxy {

  private static final Logger LOG = LoggerFactory.getLogger(JmxMetricsProxy.class);

  private static final String[] GENERIC_METRICS = {
    "org.apache.cassandra.metrics:type=ThreadPools,path=request,*",
    "org.apache.cassandra.metrics:type=ThreadPools,path=internal,*",
    "org.apache.cassandra.metrics:type=ClientRequest,*",
    "org.apache.cassandra.metrics:type=DroppedMessage,*"
  };

  private static final String PERCENT_REPAIRED_METRICS =
      "org.apache.cassandra.metrics:type=ColumnFamily,keyspace=%s,scope=*,name=PercentRepaired";

  private final JmxCassandraManagementProxy proxy;
  private final Node node;

  private JmxMetricsProxy(JmxCassandraManagementProxy proxy, Node node) {
    this.proxy = proxy;
    this.node = node;
  }

  public static JmxMetricsProxy create(JmxCassandraManagementProxy proxy, Node node) {
    return new JmxMetricsProxy(proxy, node);
  }

  @Override
  public List<GenericMetric> collectTpStats() throws JMException, IOException {
    return MetricsProxy.convertToGenericMetrics(
        collectMetrics(
            "org.apache.cassandra.metrics:type=ThreadPools,path=request,*",
            "org.apache.cassandra.metrics:type=ThreadPools,path=internal,*"),
        node);
  }

  @Override
  public List<GenericMetric> collectDroppedMessages() throws JMException, IOException {
    return MetricsProxy.convertToGenericMetrics(
        collectMetrics("org.apache.cassandra.metrics:type=DroppedMessage,*"), node);
  }

  @Override
  public List<GenericMetric> collectLatencyMetrics() throws JMException, IOException {
    return MetricsProxy.convertToGenericMetrics(
        collectMetrics("org.apache.cassandra.metrics:type=ClientRequest,*"), node);
  }

  /**
   * Collects all attributes for a given set of JMX beans.
   *
   * @param beans the list of beans to collect through JMX
   * @return a map with a key for each bean and a list of jmx stat in generic format.
   */
  public Map<String, List<JmxStat>> collectMetrics(String... beans)
      throws JMException, IOException {
    List<List<JmxStat>> allStats = Lists.newArrayList();
    Set<ObjectName> beanSet = Sets.newLinkedHashSet();
    for (String bean : beans) {
      beanSet.addAll(proxy.queryNames(new ObjectName(bean), null));
    }

    beanSet.stream()
        .map((objName) -> scrapeBean(objName))
        .forEachOrdered(attributes -> allStats.add(attributes));

    List<JmxStat> flatStatList =
        allStats.stream().flatMap(attr -> attr.stream()).collect(Collectors.toList());

    // Group the stats by scope to ease displaying/manipulating the data
    Map<String, List<JmxStat>> groupedStatList =
        flatStatList.stream().collect(Collectors.groupingBy(JmxStat::getMbeanName));

    return groupedStatList;
  }

  @Override
  public List<GenericMetric> collectGenericMetrics() throws JMException, IOException {
    return MetricsProxy.convertToGenericMetrics(collectMetrics(GENERIC_METRICS), node);
  }

  @Override
  public List<GenericMetric> collectPercentRepairedMetrics(String keyspaceName)
      throws JMException, IOException {
    return MetricsProxy.convertToGenericMetrics(
        collectMetrics(String.format(PERCENT_REPAIRED_METRICS, keyspaceName)), node);
  }

  private List<JmxStat> scrapeBean(ObjectName mbeanName) {
    List<JmxStat> attributeList = Lists.newArrayList();
    try {
      Map<String, MBeanAttributeInfo> name2AttrInfo = new LinkedHashMap<>();
      MBeanInfo info = proxy.getMBeanInfo(mbeanName);
      for (MBeanAttributeInfo attrInfo : info.getAttributes()) {
        MBeanAttributeInfo attr = attrInfo;
        if (!attr.isReadable()) {
          LOG.warn("{}.{} not readable", mbeanName, attr);
        } else {
          name2AttrInfo.put(attr.getName(), attr);
        }
      }
      proxy
          .getAttributes(mbeanName, name2AttrInfo.keySet().toArray(new String[0]))
          .asList()
          .forEach(
              (attribute) -> {
                Object value = attribute.getValue();
                JmxStat.Builder jmxStatBuilder =
                    JmxStat.builder()
                        .withAttribute(attribute.getName())
                        .withName(mbeanName.getKeyProperty("name"))
                        .withScope(mbeanName.getKeyProperty("scope"))
                        .withDomain(mbeanName.getDomain())
                        .withType(mbeanName.getKeyProperty("type"))
                        .withMbeanName(mbeanName.toString());
                if (null == value) {
                  attributeList.add(jmxStatBuilder.withValue(0.0).build());
                } else if (value instanceof Number) {
                  attributeList.add(
                      jmxStatBuilder.withValue(((Number) value).doubleValue()).build());
                }
              });
    } catch (JMException | IOException e) {
      LOG.error("Fail getting mbeanInfo or grabbing attributes for mbean {} ", mbeanName, e);
    }
    return attributeList;
  }
}
