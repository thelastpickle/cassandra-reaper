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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.prometheus.client.dropwizard.samplebuilder.CustomMappingSampleBuilder;
import io.prometheus.client.dropwizard.samplebuilder.MapperConfig;

public final class PrometheusMetricsConfiguration {

  private PrometheusMetricsConfiguration() {
  }

  public static CustomMappingSampleBuilder getCustomSampleMethodBuilder() {
    final List<MapperConfig> mapperConfigs = new ArrayList<>();
    final Map<String,String> segmentMetricLabels = new HashMap<>();
    segmentMetricLabels.put("cluster", "${0}");
    segmentMetricLabels.put("keyspace", "${1}");
    segmentMetricLabels.put("repairid", "${2}");
    mapperConfigs.add(new MapperConfig("io.cassandrareaper.service.RepairRunner.segmentsDone.*.*.*",
        "io.cassandrareaper.service.RepairRunner.segmentsDone", segmentMetricLabels));
    mapperConfigs.add(new MapperConfig("io.cassandrareaper.service.RepairRunner.segmentsTotal.*.*.*",
        "io.cassandrareaper.service.RepairRunner.segmentsTotal", segmentMetricLabels));

    final Map<String,String> millisSinceLastRepairMetricLabels = new HashMap<>();
    millisSinceLastRepairMetricLabels.put("cluster", "${0}");
    millisSinceLastRepairMetricLabels.put("keyspace", "${1}");
    millisSinceLastRepairMetricLabels.put("runid", "${2}");
    mapperConfigs.add(new MapperConfig("io.cassandrareaper.service.RepairRunner.millisSinceLastRepair.*.*.*",
        "io.cassandrareaper.service.RepairRunner.millisSinceLastRepair", millisSinceLastRepairMetricLabels));

    final Map<String,String> millisSinceLastScheduleRepairMetricLabels = new HashMap<>();
    millisSinceLastScheduleRepairMetricLabels.put("cluster", "${0}");
    millisSinceLastScheduleRepairMetricLabels.put("keyspace", "${1}");
    millisSinceLastScheduleRepairMetricLabels.put("scheduleid", "${2}");
    mapperConfigs.add(new MapperConfig(
        "io.cassandrareaper.service.RepairScheduleService.millisSinceLastRepairForSchedule.*.*.*",
        "io.cassandrareaper.service.RepairScheduleService.millisSinceLastRepairForSchedule",
        millisSinceLastScheduleRepairMetricLabels));

    final Map<String,String> repairProgressMetricLabels = new HashMap<>();
    millisSinceLastScheduleRepairMetricLabels.put("cluster", "${0}");
    millisSinceLastScheduleRepairMetricLabels.put("keyspace", "${1}");
    mapperConfigs.add(new MapperConfig(
        "io.cassandrareaper.service.RepairRunner.repairProgress.*.*",
        "io.cassandrareaper.service.RepairRunner.repairProgress",
        repairProgressMetricLabels));

    final Map<String,String> segmentRunnerMetricLabels = new HashMap<>();
    segmentRunnerMetricLabels.put("host", "${0}");
    segmentRunnerMetricLabels.put("cluster", "${1}");
    segmentRunnerMetricLabels.put("keyspace", "${2}");
    mapperConfigs.add(new MapperConfig(
        "io.cassandrareaper.service.SegmentRunner.repairing.*.*.*",
        "io.cassandrareaper.service.SegmentRunner.repairing",
        segmentRunnerMetricLabels));
    mapperConfigs.add(new MapperConfig(
        "io.cassandrareaper.service.SegmentRunner.postpone.*.*.*",
        "io.cassandrareaper.service.SegmentRunner.postpone",
        segmentRunnerMetricLabels));
    mapperConfigs.add(new MapperConfig(
        "io.cassandrareaper.service.SegmentRunner.runRepair.*.*.*",
        "io.cassandrareaper.service.SegmentRunner.runRepair",
        segmentRunnerMetricLabels));

    return new CustomMappingSampleBuilder(mapperConfigs);
  }
}
