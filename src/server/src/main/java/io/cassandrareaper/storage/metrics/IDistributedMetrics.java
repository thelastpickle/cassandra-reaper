package io.cassandrareaper.storage.metrics;

import io.cassandrareaper.core.GenericMetric;

import java.util.List;
import java.util.Optional;

public interface IDistributedMetrics {
  List<GenericMetric> getMetrics(
        String clusterName,
        Optional<String> host,
        String metricDomain,
        String metricType,
        long since);

  void storeMetrics(List<GenericMetric> metric);
}
