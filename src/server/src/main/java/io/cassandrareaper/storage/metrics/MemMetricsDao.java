package io.cassandrareaper.storage.metrics;

import io.cassandrareaper.core.PercentRepairedMetric;
import io.cassandrareaper.core.RepairRun;
import io.cassandrareaper.core.RepairSegment;
import io.cassandrareaper.core.RepairUnit;
import io.cassandrareaper.resources.view.RepairRunStatus;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class MemMetricsDao implements IMetrics {
  public final ConcurrentMap<String, Map<String, PercentRepairedMetric>> percentRepairedMetrics
        = Maps.newConcurrentMap();

  public MemMetricsDao() {
  }

  @Override
  public List<PercentRepairedMetric> getPercentRepairedMetrics(String clusterName, UUID repairScheduleId, Long since) {
    return percentRepairedMetrics.entrySet().stream()
          .filter(entry -> entry.getKey().equals(clusterName + "-" + repairScheduleId))
          .map(entry -> entry.getValue().entrySet())
          .flatMap(Collection::stream)
          .map(Map.Entry::getValue)
          .collect(Collectors.toList());
  }


  @Override
  public void storePercentRepairedMetric(PercentRepairedMetric metric) {
    synchronized (MemMetricsDao.class) {
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