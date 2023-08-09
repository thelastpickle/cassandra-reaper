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

package io.cassandrareaper.management;

import io.cassandrareaper.core.GenericMetric;
import io.cassandrareaper.core.Node;
import io.cassandrareaper.management.http.HttpCassandraManagementProxy;
import io.cassandrareaper.management.http.HttpMetricsProxy;
import io.cassandrareaper.management.jmx.JmxCassandraManagementProxy;
import io.cassandrareaper.management.jmx.JmxMetricsProxy;

import java.io.IOException;
import java.util.List;
import javax.management.JMException;

public interface MetricsProxy {

  static MetricsProxy create(ICassandraManagementProxy proxy) {
    if (proxy == null) {
      throw new RuntimeException("ICassandraManagementProxy is null");
    }
    if (proxy instanceof JmxCassandraManagementProxy) {
      return JmxMetricsProxy.create((JmxCassandraManagementProxy) proxy);
    } else if (proxy instanceof HttpCassandraManagementProxy) {
      return HttpMetricsProxy.create((HttpCassandraManagementProxy) proxy);
    }
    throw new UnsupportedOperationException("Unknown ICassandraManagementProxy implementation");
  }

  List<GenericMetric> collectTpStats(Node node) throws JMException, IOException;

  List<GenericMetric> collectDroppedMessages(Node node) throws JMException, IOException;

  List<GenericMetric> collectLatencyMetrics(Node node) throws JMException, IOException;

  List<GenericMetric> collectGenericMetrics(Node node) throws JMException, IOException;

  List<GenericMetric> collectPercentRepairedMetrics(Node node, String keyspaceName)
      throws JMException, IOException;
}
