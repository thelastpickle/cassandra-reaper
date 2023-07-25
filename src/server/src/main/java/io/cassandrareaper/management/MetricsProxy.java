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

package io.cassandrareaper.management;

import io.cassandrareaper.core.DroppedMessages;
import io.cassandrareaper.core.GenericMetric;
import io.cassandrareaper.core.JmxStat;
import io.cassandrareaper.core.MetricsHistogram;
import io.cassandrareaper.core.Node;
import io.cassandrareaper.core.ThreadPoolStat;
import io.cassandrareaper.management.ICassandraManagementProxy;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import javax.management.JMException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.ObjectName;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class MetricsProxy {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsProxy.class);

  private final ICassandraManagementProxy proxy;

  private MetricsProxy(ICassandraManagementProxy proxy) {
    this.proxy = proxy;
  }

  public static MetricsProxy create(ICassandraManagementProxy proxy) {

    return new MetricsProxy((ICassandraManagementProxy) proxy);
  }

  public static ThreadPoolStat.Builder updateGenericMetricAttribute(GenericMetric stat,
                                                                    ThreadPoolStat.Builder builder) {
    switch (stat.getMetricName()) {
      case "MaxPoolSize":
        return builder.withMaxPoolSize((int) stat.getValue());
      case "TotalBlockedTasks":
        return builder.withTotalBlockedTasks((int) stat.getValue());
      case "PendingTasks":
        return builder.withPendingTasks((int) stat.getValue());
      case "CurrentlyBlockedTasks":
        return builder.withCurrentlyBlockedTasks((int) stat.getValue());
      case "CompletedTasks":
        return builder.withCompletedTasks((int) stat.getValue());
      case "ActiveTasks":
        return builder.withActiveTasks((int) stat.getValue());
      default:
        return builder;
    }
  }

  public static DroppedMessages.Builder updateGenericMetricAttribute(GenericMetric stat,
                                                                   DroppedMessages.Builder builder) {
    switch (stat.getMetricAttribute()) {
      case "Count":
        return builder.withCount((int) stat.getValue());
      case "OneMinuteRate":
        return builder.withOneMinuteRate(stat.getValue());
      case "FiveMinuteRate":
        return builder.withFiveMinuteRate(stat.getValue());
      case "FifteenMinuteRate":
        return builder.withFifteenMinuteRate(stat.getValue());
      case "MeanRate":
        return builder.withMeanRate(stat.getValue());
      default:
        return builder;
    }
  }

  public static MetricsHistogram.Builder updateGenericMetricAttribute(GenericMetric stat,
                                                                  MetricsHistogram.Builder builder) {
    switch (stat.getMetricAttribute()) {
      case "Count":
        return builder.withCount((int) stat.getValue());
      case "OneMinuteRate":
        return builder.withOneMinuteRate(stat.getValue());
      case "FiveMinuteRate":
        return builder.withFiveMinuteRate(stat.getValue());
      case "FifteenMinuteRate":
        return builder.withFifteenMinuteRate(stat.getValue());
      case "MeanRate":
        return builder.withMeanRate(stat.getValue());
      case "StdDev":
        return builder.withStdDev(stat.getValue());
      case "Min":
        return builder.withMin(stat.getValue());
      case "Max":
        return builder.withMax(stat.getValue());
      case "Mean":
        return builder.withMean(stat.getValue());
      case "50thPercentile":
        return builder.withP50(stat.getValue());
      case "75thPercentile":
        return builder.withP75(stat.getValue());
      case "95thPercentile":
        return builder.withP95(stat.getValue());
      case "98thPercentile":
        return builder.withP98(stat.getValue());
      case "99thPercentile":
        return builder.withP99(stat.getValue());
      case "999thPercentile":
        return builder.withP999(stat.getValue());
      default:
        return builder;
    }
  }

  public static List<GenericMetric> convertToGenericMetrics(Map<String, List<JmxStat>> jmxStats, Node node) {
    List<GenericMetric> metrics = Lists.newArrayList();
    DateTime now = DateTime.now();
    for (Entry<String, List<JmxStat>> jmxStatEntry : jmxStats.entrySet()) {
      for (JmxStat jmxStat : jmxStatEntry.getValue()) {
        GenericMetric metric = GenericMetric.builder()
            .withClusterName(node.getClusterName())
            .withHost(node.getHostname())
            .withMetricDomain(jmxStat.getDomain())
            .withMetricType(jmxStat.getType())
            .withMetricScope(jmxStat.getScope())
            .withMetricName(jmxStat.getName())
            .withMetricAttribute(jmxStat.getAttribute())
            .withValue(jmxStat.getValue())
            .withTs(now)
            .build();
        metrics.add(metric);
      }
    }

    return metrics;
  }

  public Map<String, List<JmxStat>> collectTpStats() throws JMException, IOException {
    return collectMetrics(
        "org.apache.cassandra.metrics:type=ThreadPools,path=request,*",
        "org.apache.cassandra.metrics:type=ThreadPools,path=internal,*");
  }

  public Map<String, List<JmxStat>> collectDroppedMessages() throws JMException, IOException {
    return collectMetrics("org.apache.cassandra.metrics:type=DroppedMessage,*");
  }

  public Map<String, List<JmxStat>> collectLatencyMetrics() throws JMException, IOException {
    return collectMetrics("org.apache.cassandra.metrics:type=ClientRequest,*");
  }

  /**
   * Collects all attributes for a given set of JMX beans.
   *
   * @param beans the list of beans to collect through JMX
   * @return a map with a key for each bean and a list of jmx stat in generic format.
   */
  public Map<String, List<JmxStat>> collectMetrics(String... beans) throws JMException, IOException {
    List<List<JmxStat>> allStats = Lists.newArrayList();
    Set<ObjectName> beanSet = Sets.newLinkedHashSet();
    for (String bean : beans) {
      beanSet.addAll(proxy.queryNames(new ObjectName(bean), null));
    }

    beanSet.stream()
        .map((objName) -> scrapeBean(objName))
        .forEachOrdered(attributes -> allStats.add(attributes));

    List<JmxStat> flatStatList = allStats.stream()
        .flatMap(attr -> attr.stream()).collect(Collectors.toList());

    // Group the stats by scope to ease displaying/manipulating the data
    Map<String, List<JmxStat>> groupedStatList = flatStatList.stream()
        .collect(Collectors.groupingBy(JmxStat::getMbeanName));

    return groupedStatList;
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
      proxy.getAttributes(mbeanName, name2AttrInfo.keySet().toArray(new String[0]))
          .asList()
          .forEach((attribute) -> {
            Object value = attribute.getValue();
            JmxStat.Builder jmxStatBuilder = JmxStat.builder()
                .withAttribute(attribute.getName())
                .withName(mbeanName.getKeyProperty("name"))
                .withScope(mbeanName.getKeyProperty("scope"))
                .withDomain(mbeanName.getDomain())
                .withType(mbeanName.getKeyProperty("type"))
                .withMbeanName(mbeanName.toString());
            if (null == value) {
              attributeList.add(jmxStatBuilder.withValue(0.0).build());
            } else if (value instanceof Number) {
              attributeList.add(jmxStatBuilder.withValue(((Number) value).doubleValue()).build());
            }
          });
    } catch (JMException | IOException e) {
      LOG.error("Fail getting mbeanInfo or grabbing attributes for mbean {} ", mbeanName, e);
    }
    return attributeList;
  }


}