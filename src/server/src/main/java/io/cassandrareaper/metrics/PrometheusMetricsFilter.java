/*
 * Copyright 2018-2018 The Last Pickle Ltd
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

package io.cassandrareaper.metrics;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import java.util.HashSet;
import java.util.Set;

/*
 * This class helps drop Dropwizard metrics that we don't want converted to the Prometheus format.
 * This is necessary since Dropwizard metrics such as DoneSegmentsPerKeyspace and DoneSegments map to the same name in
 * the Prometheus format. The number of labels differ though.
 * In these cases, we only keep the metric that has the most labels. The Prometheus query language allows users to query
 * for aggregates if they desire.
 */

public class PrometheusMetricsFilter implements MetricFilter {
  private static final Set<String> IGNORED_METRICS = new HashSet<>();

  public static void ignoreMetric(String metricName) {
    IGNORED_METRICS.add(metricName);
  }

  public static void removeIgnoredMetric(String metricNameForMillisSinceLastRepair) {
    IGNORED_METRICS.remove(metricNameForMillisSinceLastRepair);
  }

  @Override
  public boolean matches(String name, Metric metric) {
    for (String prefix : IGNORED_METRICS) {
      if (IGNORED_METRICS.contains(name)) {
        return false;
      }
    }
    return true;
  }
}
