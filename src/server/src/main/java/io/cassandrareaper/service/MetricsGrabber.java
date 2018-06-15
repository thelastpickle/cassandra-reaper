/*
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

package io.cassandrareaper.service;

import io.cassandrareaper.AppContext;
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.DroppedMessages;
import io.cassandrareaper.core.JmxStat;
import io.cassandrareaper.core.MetricsHistogram;
import io.cassandrareaper.core.Node;
import io.cassandrareaper.core.ThreadPoolStat;
import io.cassandrareaper.core.ThreadPoolStat.Builder;
import io.cassandrareaper.jmx.JmxProxy;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import javax.management.JMException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class MetricsGrabber {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsGrabber.class);

  private final AppContext context;
  private final ExecutorService executor = Executors.newFixedThreadPool(5);

  private MetricsGrabber(AppContext context) {
    this.context = context;
  }

  public static MetricsGrabber create(AppContext context) {
    return new MetricsGrabber(context);
  }

  public List<ThreadPoolStat> getTpStats(Node host) throws ReaperException {
    try {
      JmxProxy jmxProxy
          = context.jmxConnectionFactory.connect(host, context.config.getJmxConnectionTimeoutInSeconds());

      return convertToThreadPoolStats(jmxProxy.collectTpStats());
    } catch (JMException | RuntimeException | InterruptedException | IOException e) {
      LOG.error("Failed collecting tpstats for host {}", host, e);
      throw new ReaperException(e);
    }
  }

  @VisibleForTesting
  public List<ThreadPoolStat> convertToThreadPoolStats(Map<String, List<JmxStat>> jmxStats) {
    List<ThreadPoolStat> tpstats = Lists.newArrayList();
    for (Entry<String, List<JmxStat>> pool : jmxStats.entrySet()) {
      Builder builder = ThreadPoolStat.builder().withName(pool.getKey());
      for (JmxStat stat : pool.getValue()) {
        if (stat.getName().equals("MaxPoolSize")) {
          builder.withMaxPoolSize(stat.getValue().intValue());
        } else if (stat.getName().equals("TotalBlockedTasks")) {
          builder.withTotalBlockedTasks(stat.getValue().intValue());
        } else if (stat.getName().equals("PendingTasks")) {
          builder.withPendingTasks(stat.getValue().intValue());
        } else if (stat.getName().equals("CurrentlyBlockedTasks")) {
          builder.withCurrentlyBlockedTasks(stat.getValue().intValue());
        } else if (stat.getName().equals("CompletedTasks")) {
          builder.withCompletedTasks(stat.getValue().intValue());
        } else if (stat.getName().equals("ActiveTasks")) {
          builder.withActiveTasks(stat.getValue().intValue());
        }
      }
      tpstats.add(builder.build());
    }

    return tpstats;
  }

  public List<DroppedMessages> getDroppedMessages(Node host) throws ReaperException {
    try {
      JmxProxy jmxProxy =
          context.jmxConnectionFactory.connect(
              host, context.config.getJmxConnectionTimeoutInSeconds());

      return convertToDroppedMessages(jmxProxy.collectDroppedMessages());
    } catch (JMException | RuntimeException | InterruptedException | IOException e) {
      LOG.error("Failed collecting tpstats for host {}", host, e);
      throw new ReaperException(e);
    }
  }

  @VisibleForTesting
  public List<DroppedMessages> convertToDroppedMessages(Map<String, List<JmxStat>> jmxStats) {
    List<DroppedMessages> droppedMessages = Lists.newArrayList();
    for (Entry<String, List<JmxStat>> pool : jmxStats.entrySet()) {
      DroppedMessages.Builder builder = DroppedMessages.builder().withName(pool.getKey());
      for (JmxStat stat : pool.getValue()) {
        if (stat.getAttribute().equals("Count")) {
          builder.withCount(stat.getValue().intValue());
        } else if (stat.getAttribute().equals("OneMinuteRate")) {
          builder.withOneMinuteRate(stat.getValue());
        } else if (stat.getAttribute().equals("FiveMinuteRate")) {
          builder.withFiveMinuteRate(stat.getValue());
        } else if (stat.getAttribute().equals("FifteenMinuteRate")) {
          builder.withFifteenMinuteRate(stat.getValue());
        } else if (stat.getAttribute().equals("MeanRate")) {
          builder.withMeanRate(stat.getValue());
        }
      }
      droppedMessages.add(builder.build());
    }

    return droppedMessages;
  }

  public List<MetricsHistogram> getClientRequestLatencies(Node host) throws ReaperException {
    try {
      JmxProxy jmxProxy =
          context.jmxConnectionFactory.connect(
              host, context.config.getJmxConnectionTimeoutInSeconds());

      return convertToMetricsHistogram(jmxProxy.collectLatencyMetrics());
    } catch (JMException | RuntimeException | InterruptedException | IOException e) {
      LOG.error("Failed collecting tpstats for host {}", host, e);
      throw new ReaperException(e);
    }
  }

  @VisibleForTesting
  public List<MetricsHistogram> convertToMetricsHistogram(Map<String, List<JmxStat>> jmxStats) {
    List<MetricsHistogram> droppedMessages = Lists.newArrayList();
    for (Entry<String, List<JmxStat>> pool : jmxStats.entrySet()) {
      // We have several metric types that we need to process separately
      // We'll group on MetricsHistogram::getType in order to generate one histogram per type
      Map<String, List<JmxStat>> metrics =
          pool.getValue().stream().collect(Collectors.groupingBy(JmxStat::getName));

      for (Entry<String, List<JmxStat>> metric : metrics.entrySet()) {
        MetricsHistogram.Builder builder =
            MetricsHistogram.builder().withName(pool.getKey()).withType(metric.getKey());
        for (JmxStat stat : metric.getValue()) {
          if (stat.getAttribute().equals("Count")) {
            builder.withCount(stat.getValue().intValue());
          } else if (stat.getAttribute().equals("OneMinuteRate")) {
            builder.withOneMinuteRate(stat.getValue());
          } else if (stat.getAttribute().equals("FiveMinuteRate")) {
            builder.withFiveMinuteRate(stat.getValue());
          } else if (stat.getAttribute().equals("FifteenMinuteRate")) {
            builder.withFifteenMinuteRate(stat.getValue());
          } else if (stat.getAttribute().equals("MeanRate")) {
            builder.withMeanRate(stat.getValue());
          } else if (stat.getAttribute().equals("StdDev")) {
            builder.withStdDev(stat.getValue());
          } else if (stat.getAttribute().equals("Min")) {
            builder.withMin(stat.getValue());
          } else if (stat.getAttribute().equals("Max")) {
            builder.withMax(stat.getValue());
          } else if (stat.getAttribute().equals("Mean")) {
            builder.withMean(stat.getValue());
          } else if (stat.getAttribute().equals("50thPercentile")) {
            builder.withP50(stat.getValue());
          } else if (stat.getAttribute().equals("75thPercentile")) {
            builder.withP75(stat.getValue());
          } else if (stat.getAttribute().equals("95thPercentile")) {
            builder.withP95(stat.getValue());
          } else if (stat.getAttribute().equals("98thPercentile")) {
            builder.withP98(stat.getValue());
          } else if (stat.getAttribute().equals("99thPercentile")) {
            builder.withP99(stat.getValue());
          } else if (stat.getAttribute().equals("999thPercentile")) {
            builder.withP999(stat.getValue());
          }
        }
        droppedMessages.add(builder.build());
      }
    }

    return droppedMessages;
  }

}
