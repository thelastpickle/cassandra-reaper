/*
 * Copyright 2014-2017 Spotify AB
 * Copyright 2016-2019 The Last Pickle Ltd
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

package io.cassandrareaper.storage.metrics;

import com.google.common.collect.Maps;
import io.cassandrareaper.core.PercentRepairedMetric;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class MemoryMetricsDao implements IMetricsDao {
  public final ConcurrentMap<String, Map<String, PercentRepairedMetric>> percentRepairedMetrics =
      Maps.newConcurrentMap();

  public MemoryMetricsDao() {}

  @Override
  public List<PercentRepairedMetric> getPercentRepairedMetrics(
      String clusterName, UUID repairScheduleId, Long since) {
    return percentRepairedMetrics.entrySet().stream()
        .filter(entry -> entry.getKey().equals(clusterName + "-" + repairScheduleId))
        .map(entry -> entry.getValue().entrySet())
        .flatMap(Collection::stream)
        .map(Map.Entry::getValue)
        .collect(Collectors.toList());
  }

  @Override
  public void storePercentRepairedMetric(PercentRepairedMetric metric) {
    synchronized (MemoryMetricsDao.class) {
      String metricKey = metric.getCluster() + "-" + metric.getRepairScheduleId();
      Map<String, PercentRepairedMetric> newValue = Maps.newHashMap();
      if (percentRepairedMetrics.containsKey(metricKey)) {
        newValue.putAll(percentRepairedMetrics.get(metricKey));
      }
      newValue.put(metric.getNode(), metric);
      percentRepairedMetrics.put(metricKey, newValue);
    }
  }
}
