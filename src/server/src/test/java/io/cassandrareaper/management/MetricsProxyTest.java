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

package io.cassandrareaper.management;

import io.cassandrareaper.core.DroppedMessages;
import io.cassandrareaper.core.GenericMetric;
import io.cassandrareaper.core.MetricsHistogram;
import io.cassandrareaper.core.ThreadPoolStat;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class MetricsProxyTest {

  @Test
  public void UpdateGenericMetricAttributeForThreadPoolStatBuilder() {
    GenericMetric metric =
        GenericMetric.builder().withMetricName("MaxPoolSize").withValue(10).build();
    ThreadPoolStat.Builder builder = ThreadPoolStat.builder();
    ThreadPoolStat.Builder updatedBuilder =
        MetricsProxy.updateGenericMetricAttribute(metric, builder);
    assertEquals(10, updatedBuilder.build().getMaxPoolSize().intValue());
  }

  @Test
  public void testUpdateGenericMetricAttributeForDroppedMessagesBuilder() {
    GenericMetric metric =
        GenericMetric.builder().withMetricAttribute("Count").withValue(5).build();
    DroppedMessages.Builder builder = DroppedMessages.builder();
    DroppedMessages.Builder updatedBuilder =
        MetricsProxy.updateGenericMetricAttribute(metric, builder);
    assertEquals(5, updatedBuilder.build().getCount().intValue());
  }

  @Test
  public void testUpdateGenericMetricAttributeForMetricsHistogramBuilder() {
    GenericMetric metric =
        GenericMetric.builder().withMetricAttribute("Mean").withValue(3.14).build();
    MetricsHistogram.Builder builder = MetricsHistogram.builder();
    MetricsHistogram.Builder updatedBuilder =
        MetricsProxy.updateGenericMetricAttribute(metric, builder);
    assertEquals(3.14, updatedBuilder.build().getMean(), 0.001);
  }
}
