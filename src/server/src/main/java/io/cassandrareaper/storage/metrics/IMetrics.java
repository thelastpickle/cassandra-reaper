package io.cassandrareaper.storage.metrics;

import io.cassandrareaper.core.PercentRepairedMetric;
import io.cassandrareaper.resources.view.RepairRunStatus;
import io.cassandrareaper.resources.view.RepairScheduleStatus;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

public interface IMetrics {

  List<PercentRepairedMetric> getPercentRepairedMetrics(
        String clusterName,
        UUID repairScheduleId,
        Long since);

  void storePercentRepairedMetric(PercentRepairedMetric metric);
}
