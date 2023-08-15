/*
 * Copyright 2020-2020 The Last Pickle Ltd
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

import io.cassandrareaper.core.GenericMetric;
import io.cassandrareaper.core.Node;
import io.cassandrareaper.management.MetricsProxy;

import java.io.IOException;
import java.util.List;
import javax.management.JMException;

public final class HttpMetricsProxy implements MetricsProxy {

  private HttpCassandraManagementProxy proxy;

  private HttpMetricsProxy(HttpCassandraManagementProxy proxy) {
    this.proxy = proxy;
  }

  public static HttpMetricsProxy create(HttpCassandraManagementProxy proxy) {
    return new HttpMetricsProxy(proxy);
  }

  @Override
  public List<GenericMetric> collectTpStats(Node node) throws JMException, IOException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public List<GenericMetric> collectDroppedMessages(Node node) throws JMException, IOException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public List<GenericMetric> collectLatencyMetrics(Node node) throws JMException, IOException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public List<GenericMetric> collectGenericMetrics(Node node) throws JMException, IOException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public List<GenericMetric> collectPercentRepairedMetrics(Node node, String keyspaceName)
      throws JMException, IOException {
    throw new UnsupportedOperationException("Not implemented");
  }
}
