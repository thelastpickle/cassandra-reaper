/*
 * Copyright 2023-2023 DataStax, Inc.
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

package io.cassandrareaper.management.http;

import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.GenericMetric;
import io.cassandrareaper.core.Node;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.management.JMException;

import com.datastax.mgmtapi.client.api.DefaultApi;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Response;
import okhttp3.ResponseBody;

import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.collections.Sets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class HttpMetricsProxyTest {

  @Test
  public void testParsePrometheusMetrics() throws IOException {
    DefaultApi mockClient = Mockito.mock(DefaultApi.class);

    HttpCassandraManagementProxy proxy = HttpCassandraManagementProxyTest.mockProxy(mockClient);
    HttpMetricsProxy metricsProxy = HttpMetricsProxy.create(proxy, Mockito.mock(Node.class));

    URL url = Resources.getResource("metric-samples/prom-metrics.txt");
    String data = Resources.toString(url, Charsets.UTF_8);

    List<GenericMetric> parsedMetrics = metricsProxy.parsePrometheusMetrics(
        HttpMetricsProxy.THREAD_POOL_METRICS_PREFIX, data, Optional.empty());
    assertTrue("Metrics were parsed", !parsedMetrics.isEmpty());
    //assertEquals("Right number of metrics for jvm_threads_state", 0, parsedMetrics.get("jvm_threads_state").size());
    assertEquals("Right number of metrics for org_apache_cassandra_metrics_thread_pools_pending_tasks",
        148,
        parsedMetrics.stream().filter(
            metric -> metric.getMetricType().equals("ThreadPools")
            && metric.getMetricName().equals("PendingTasks")).count());
    assertEquals("Right number of thread pools metrics",
        2370,
        parsedMetrics.size());
  }

  @Test
  public void testConvertToUpperCamelCase_allLowerCase() {
    String input = "hello_world";
    String expectedOutput = "HelloWorld";

    DefaultApi mockClient = Mockito.mock(DefaultApi.class);

    HttpCassandraManagementProxy proxy = HttpCassandraManagementProxyTest.mockProxy(mockClient);
    HttpMetricsProxy metricsProxy = HttpMetricsProxy.create(proxy, Mockito.mock(Node.class));
    assertEquals(expectedOutput, metricsProxy.convertToUpperCamelCase(input));
  }

  @Test
  public void testConvertToUpperCamelCase_allUpperCase() {
    String input = "HELLO_WORLD";
    String expectedOutput = "HelloWorld";

    DefaultApi mockClient = Mockito.mock(DefaultApi.class);

    HttpCassandraManagementProxy proxy = HttpCassandraManagementProxyTest.mockProxy(mockClient);
    HttpMetricsProxy metricsProxy = HttpMetricsProxy.create(proxy, Mockito.mock(Node.class));
    assertEquals(expectedOutput, metricsProxy.convertToUpperCamelCase(input));
  }

  @Test
  public void testConvertToUpperCamelCase_mixedCase() {
    String input = "HeLLo_WoRLd";
    String expectedOutput = "HelloWorld";

    DefaultApi mockClient = Mockito.mock(DefaultApi.class);

    HttpCassandraManagementProxy proxy = HttpCassandraManagementProxyTest.mockProxy(mockClient);
    HttpMetricsProxy metricsProxy = HttpMetricsProxy.create(proxy, Mockito.mock(Node.class));
    assertEquals(expectedOutput, metricsProxy.convertToUpperCamelCase(input));
  }

  @Test
  public void testConvertToUpperCamelCase_noUnderscoreSingleWord() {
    String input = "hello";
    String expectedOutput = "Hello";

    DefaultApi mockClient = Mockito.mock(DefaultApi.class);

    HttpCassandraManagementProxy proxy = HttpCassandraManagementProxyTest.mockProxy(mockClient);
    HttpMetricsProxy metricsProxy = HttpMetricsProxy.create(proxy, Mockito.mock(Node.class));
    assertEquals(expectedOutput, metricsProxy.convertToUpperCamelCase(input));
  }

  @Test
  public void testConvertToUpperCamelCase_EmptyString() {
    String input = "";
    String expectedOutput = "";

    DefaultApi mockClient = Mockito.mock(DefaultApi.class);

    HttpCassandraManagementProxy proxy = HttpCassandraManagementProxyTest.mockProxy(mockClient);
    HttpMetricsProxy metricsProxy = HttpMetricsProxy.create(proxy, Mockito.mock(Node.class));
    assertEquals(expectedOutput, metricsProxy.convertToUpperCamelCase(input));
  }

  @Test
  public void testParseThreadPoolMetric_withMatch() {
    // Prepare
    String metricName = "org_apache_cassandra_metrics_thread_pools_pending_tasks";
    Map<String, String> labels = new HashMap<>();
    labels.put("pool_name", "TestPool");
    double value = 10.0;


    DefaultApi mockClient = Mockito.mock(DefaultApi.class);

    HttpCassandraManagementProxy proxy = HttpCassandraManagementProxyTest.mockProxy(mockClient);
    HttpMetricsProxy metricsProxy = HttpMetricsProxy.create(proxy, Mockito.mock(Node.class));
    // Act
    Optional<GenericMetric> result
        = metricsProxy.parseMetric(
            Mockito.mock(Node.class),
            metricName,
            labels,
            value,
            HttpMetricsProxy.THREAD_POOL_METRICS_PREFIX,
            Optional.empty());

    // Assert
    assertTrue(result.isPresent());
    GenericMetric stat = result.get();
    assertEquals("org.apache.cassandra.metrics", stat.getMetricDomain());
    assertEquals("ThreadPools", stat.getMetricType());
    assertEquals("TestPool", stat.getMetricScope());
    assertEquals("PendingTasks", stat.getMetricName());
    assertEquals("Count", stat.getMetricAttribute());
    assertEquals(value, stat.getValue(), 0.0001);
  }

  @Test
  public void testParseDroppedMessage_withMatch() {
    String metricName = "org_apache_cassandra_metrics_dropped_message_dropped_total";
    Map<String, String> labels = new HashMap<>();
    labels.put("message_type", "TestMessage");
    double value = 123.45;


    DefaultApi mockClient = Mockito.mock(DefaultApi.class);

    HttpCassandraManagementProxy proxy = HttpCassandraManagementProxyTest.mockProxy(mockClient);
    HttpMetricsProxy metricsProxy = HttpMetricsProxy.create(proxy, Mockito.mock(Node.class));
    Optional<GenericMetric> result
        = metricsProxy.parseMetric(
            Mockito.mock(Node.class),
            metricName,
            labels,
            value,
            HttpMetricsProxy.DROPPED_MESSAGES_METRICS_PREFIX,
            Optional.empty());

    assertTrue(result.isPresent());
    GenericMetric stat = result.get();
    assertEquals("org.apache.cassandra.metrics", stat.getMetricDomain());
    assertEquals("DroppedMessage", stat.getMetricType());
    assertEquals("TestMessage", stat.getMetricScope());
    assertEquals("DroppedTotal", stat.getMetricName());
    assertEquals("Count", stat.getMetricAttribute());
    assertEquals(value, stat.getValue(), 0.0001);
  }

  @Test
  public void testParsePercentRepaired_withMatch() {
    String metricName = "org_apache_cassandra_metrics_table_percent_repaired";
    Map<String, String> labels = new HashMap<>();
    labels.put("table", "my_table");
    labels.put("keyspace", "my_keyspace");
    double value = 123.45;


    DefaultApi mockClient = Mockito.mock(DefaultApi.class);

    HttpCassandraManagementProxy proxy = HttpCassandraManagementProxyTest.mockProxy(mockClient);
    HttpMetricsProxy metricsProxy = HttpMetricsProxy.create(proxy, Mockito.mock(Node.class));
    Optional<GenericMetric> result
        = metricsProxy.parseMetric(
            Mockito.mock(Node.class),
            metricName,
            labels,
            value,
            HttpMetricsProxy.PERCENT_REPAIRED_METRICS_PREFIX,
            Optional.of("my_keyspace"));

    assertTrue("expected metrics in result but result was empty", result.isPresent());
    GenericMetric stat = result.get();
    assertEquals("org.apache.cassandra.metrics", stat.getMetricDomain());
    assertEquals("ColumnFamily", stat.getMetricType());
    assertEquals("my_table", stat.getMetricScope());
    assertEquals("PercentRepaired", stat.getMetricName());
    assertEquals("Count", stat.getMetricAttribute());
    assertEquals(value, stat.getValue(), 0.0001);
  }

  @Test
  public void testParseThreadPoolMetric_withoutMatch() {
    // Prepare
    String metricName = "incorrect_metric_name";
    Map<String, String> labels = new HashMap<>();
    labels.put("pool_name", "TestPool");
    double value = 10.0;

    DefaultApi mockClient = Mockito.mock(DefaultApi.class);

    HttpCassandraManagementProxy proxy = HttpCassandraManagementProxyTest.mockProxy(mockClient);
    HttpMetricsProxy metricsProxy = HttpMetricsProxy.create(proxy, Mockito.mock(Node.class));
    // Act
    Optional<GenericMetric> result
        = metricsProxy.parseMetric(
            Mockito.mock(Node.class),
            metricName,
            labels,
            value,
            HttpMetricsProxy.THREAD_POOL_METRICS_PREFIX,
            Optional.empty());

    // Assert
    assertFalse(result.isPresent());
  }

  @SuppressWarnings("checkstyle:LineLength")
  @Test
  public void testCollectTpStats() throws IOException, JMException {
    String responseBodyStr = "org_apache_cassandra_metrics_thread_pools_pending_tasks{host=\"eae3481e-f803-49b5-8516-7ff271104db5\",instance=\"172.18.0.3\",cluster=\"test\",datacenter=\"dc1\",rack=\"default\",pod_name=\"test-dc1-default-sts-0\",node_name=\"mc-0-worker3\",pool_type=\"internal\",pool_name=\"TPC\",} 0.0\n"
          + "org_apache_cassandra_metrics_thread_pools_pending_tasks{host=\"eae3481e-f803-49b5-8516-7ff271104db5\",instance=\"172.18.0.3\",cluster=\"test\",datacenter=\"dc1\",rack=\"default\",pod_name=\"test-dc1-default-sts-0\",node_name=\"mc-0-worker3\",pool_type=\"internal\",pool_name=\"ValidationExecutor\",} 0.0\n"
          + "org_apache_cassandra_metrics_thread_pools_pending_tasks{host=\"eae3481e-f803-49b5-8516-7ff271104db5\",instance=\"172.18.0.3\",cluster=\"test\",datacenter=\"dc1\",rack=\"default\",pod_name=\"test-dc1-default-sts-0\",node_name=\"mc-0-worker3\",pool_type=\"internal\",pool_name=\"AntiCompactionExecutor\",} 0.0\n"
          + "org_apache_cassandra_metrics_thread_pools_pending_tasks{host=\"eae3481e-f803-49b5-8516-7ff271104db5\",instance=\"172.18.0.3\",cluster=\"test\",datacenter=\"dc1\",rack=\"default\",pod_name=\"test-dc1-default-sts-0\",node_name=\"mc-0-worker3\",pool_type=\"internal\",pool_name=\"AntiEntropyStage\",} 0.0\n"
          + "org_apache_cassandra_metrics_thread_pools_pending_tasks{host=\"eae3481e-f803-49b5-8516-7ff271104db5\",instance=\"172.18.0.3\",cluster=\"test\",datacenter=\"dc1\",rack=\"default\",pod_name=\"test-dc1-default-sts-0\",node_name=\"mc-0-worker3\",pool_type=\"internal\",pool_name=\"TPC\",} 0.0\n"
          + "org_apache_cassandra_metrics_keyspace_view_read_time_solr_admin_bucket{host=\"eae3481e-f803-49b5-8516-7ff271104db5\",instance=\"172.18.0.3\",cluster=\"test\",datacenter=\"dc1\",rack=\"default\",pod_name=\"test-dc1-default-sts-0\",node_name=\"mc-0-worker3\",le=\"3379391\",} 0.0\n";

    OkHttpClient httpClient = Mockito.mock(OkHttpClient.class);
    Call call = Mockito.mock(Call.class);
    Response response = Mockito.mock(Response.class);

    when(httpClient.newCall(any())).thenReturn(call);
    when(call.execute()).thenReturn(response);
    when(response.isSuccessful()).thenReturn(true);

    ResponseBody responseBody = Mockito.mock(ResponseBody.class);
    when(responseBody.string()).thenReturn(responseBodyStr);
    when(response.body()).thenReturn(responseBody);

    HttpCassandraManagementProxy httpManagementProxy = Mockito.mock(HttpCassandraManagementProxy.class);
    when(httpManagementProxy.getMetricsPort()).thenReturn(9000);
    when(httpManagementProxy.getHost()).thenReturn("172.18.0.3");

    Node node = Node.builder()
          .withHostname("172.18.0.3")
          .withCluster(Cluster.builder()
          .withName("test")
          .withSeedHosts(Sets.newSet("127.0.0.1"))
          .build())
        .build();

    HttpMetricsProxy httpMetricsProxy = HttpMetricsProxy.create(httpManagementProxy, node, () -> httpClient);
    List<GenericMetric> tpStats = httpMetricsProxy.collectTpStats();
    assertEquals(5, tpStats.size());
    assertEquals("org.apache.cassandra.metrics", tpStats.get(0).getMetricDomain());
    assertEquals("ThreadPools", tpStats.get(0).getMetricType());
    assertEquals("TPC", tpStats.get(0).getMetricScope());
    assertEquals("PendingTasks", tpStats.get(0).getMetricName());
  }

  @SuppressWarnings("checkstyle:LineLength")
  @Test
  public void testCollectTpPendingTasks() throws JMException, IOException, ReaperException {
    String responseBodyStr = "org_apache_cassandra_metrics_thread_pools_pending_tasks{host=\"eae3481e-f803-49b5-8516-7ff271104db5\",instance=\"172.18.0.3\",cluster=\"test\",datacenter=\"dc1\",rack=\"default\",pod_name=\"test-dc1-default-sts-0\",node_name=\"mc-0-worker3\",pool_type=\"internal\",pool_name=\"TPC\",} 0.0\n"
          + "org_apache_cassandra_metrics_thread_pools_pending_tasks{host=\"eae3481e-f803-49b5-8516-7ff271104db5\",instance=\"172.18.0.3\",cluster=\"test\",datacenter=\"dc1\",rack=\"default\",pod_name=\"test-dc1-default-sts-0\",node_name=\"mc-0-worker3\",pool_type=\"internal\",pool_name=\"ValidationExecutor\",} 0.0\n"
          + "org_apache_cassandra_metrics_thread_pools_pending_tasks{host=\"eae3481e-f803-49b5-8516-7ff271104db5\",instance=\"172.18.0.3\",cluster=\"test\",datacenter=\"dc1\",rack=\"default\",pod_name=\"test-dc1-default-sts-0\",node_name=\"mc-0-worker3\",pool_type=\"internal\",pool_name=\"AntiCompactionExecutor\",} 0.0\n"
          + "org_apache_cassandra_metrics_thread_pools_pending_tasks{host=\"eae3481e-f803-49b5-8516-7ff271104db5\",instance=\"172.18.0.3\",cluster=\"test\",datacenter=\"dc1\",rack=\"default\",pod_name=\"test-dc1-default-sts-0\",node_name=\"mc-0-worker3\",pool_type=\"internal\",pool_name=\"AntiEntropyStage\",} 0.0\n"
          + "org_apache_cassandra_metrics_thread_pools_pending_tasks{host=\"eae3481e-f803-49b5-8516-7ff271104db5\",instance=\"172.18.0.3\",cluster=\"test\",datacenter=\"dc1\",rack=\"default\",pod_name=\"test-dc1-default-sts-0\",node_name=\"mc-0-worker3\",pool_type=\"internal\",pool_name=\"TPC\",} 0.0\n"
          + "org_apache_cassandra_metrics_thread_pools_pending_tasks{host=\"eae3481e-f803-49b5-8516-7ff271104db5\",instance=\"172.18.0.3\",cluster=\"test\",datacenter=\"dc1\",rack=\"default\",pod_name=\"test-dc1-default-sts-0\",node_name=\"mc-0-worker3\",pool_type=\"internal\",pool_name=\"CompactionExecutor\",} 0.0\n"
          + "org_apache_cassandra_metrics_keyspace_view_read_time_solr_admin_bucket{host=\"eae3481e-f803-49b5-8516-7ff271104db5\",instance=\"172.18.0.3\",cluster=\"test\",datacenter=\"dc1\",rack=\"default\",pod_name=\"test-dc1-default-sts-0\",node_name=\"mc-0-worker3\",le=\"3379391\",} 0.0\n";

    OkHttpClient httpClient = Mockito.mock(OkHttpClient.class);
    Call call = Mockito.mock(Call.class);
    Response response = Mockito.mock(Response.class);

    when(httpClient.newCall(any())).thenReturn(call);
    when(call.execute()).thenReturn(response);
    when(response.isSuccessful()).thenReturn(true);

    ResponseBody responseBody = Mockito.mock(ResponseBody.class);
    when(responseBody.string()).thenReturn(responseBodyStr);
    when(response.body()).thenReturn(responseBody);

    HttpCassandraManagementProxy httpManagementProxy = Mockito.mock(HttpCassandraManagementProxy.class);
    when(httpManagementProxy.getHost()).thenReturn("172.18.0.3");
    when(httpManagementProxy.getMetricsPort()).thenReturn(9000);

    Node node = Node.builder()
          .withHostname("172.18.0.3")
          .withCluster(Cluster.builder()
          .withName("test")
          .withSeedHosts(Sets.newSet("127.0.0.1"))
          .build())
        .build();

    HttpMetricsProxy httpMetricsProxy = HttpMetricsProxy.create(httpManagementProxy, node, () -> httpClient);

    List<GenericMetric> pendingTasks = httpMetricsProxy.collectTpPendingTasks();
    assertEquals(1, pendingTasks.size()); // there's only 1 that is a CompactionExecutor metric
    assertEquals("org.apache.cassandra.metrics", pendingTasks.get(0).getMetricDomain());
    assertEquals("ThreadPools", pendingTasks.get(0).getMetricType());
    assertEquals("CompactionExecutor", pendingTasks.get(0).getMetricScope());
    assertEquals("PendingTasks", pendingTasks.get(0).getMetricName());
  }
}
