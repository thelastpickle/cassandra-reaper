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

package io.cassandrareaper.resources.view;

import io.cassandrareaper.SimpleReaperClient;
import io.cassandrareaper.core.RepairSchedule;

import com.datastax.driver.core.utils.UUIDs;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.apache.cassandra.repair.RepairParallelism;
import org.joda.time.DateTime;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

public final class RepairScheduleStatusTest {

  private static final Logger LOG = LoggerFactory.getLogger(RepairScheduleStatusTest.class);

  @Test
  public void testJacksonJSONParsing() throws Exception {
    RepairScheduleStatus data = new RepairScheduleStatus();
    data.setClusterName("testCluster");
    data.setColumnFamilies(Lists.<String>newArrayList());
    data.setCreationTime(DateTime.now().withMillis(0));
    data.setDaysBetween(2);
    data.setId(UUIDs.timeBased());
    data.setIntensity(0.75);
    data.setIncrementalRepair(false);
    data.setKeyspaceName("testKeyspace");
    data.setOwner("testuser");
    data.setRepairParallelism(RepairParallelism.PARALLEL);
    data.setState(RepairSchedule.State.ACTIVE);

    ObjectMapper mapper = new ObjectMapper();
    String dataAsJson = mapper.writeValueAsString(data);
    LOG.info("DATA: " + dataAsJson);

    RepairScheduleStatus dataAfter = SimpleReaperClient.parseRepairScheduleStatusJSON(dataAsJson);

    assertEquals(data.getClusterName(), dataAfter.getClusterName());
    assertEquals(data.getColumnFamilies(), dataAfter.getColumnFamilies());
    assertEquals(data.getCreationTime(), dataAfter.getCreationTime());
    assertEquals(data.getDaysBetween(), dataAfter.getDaysBetween());
    assertEquals(data.getId(), dataAfter.getId());
    assertEquals(data.getIntensity(), dataAfter.getIntensity(), 0.0);
    assertEquals(data.getIncrementalRepair(), dataAfter.getIncrementalRepair());
    assertEquals(data.getKeyspaceName(), dataAfter.getKeyspaceName());
    assertEquals(data.getRepairParallelism(), dataAfter.getRepairParallelism());
    assertEquals(data.getState(), dataAfter.getState());
  }

}
