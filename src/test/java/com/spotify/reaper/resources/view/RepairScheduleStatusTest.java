package com.spotify.reaper.resources.view;

import com.google.common.collect.Lists;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.reaper.SimpleReaperClient;
import com.spotify.reaper.core.RepairSchedule;

import org.apache.cassandra.repair.RepairParallelism;
import org.joda.time.DateTime;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

public class RepairScheduleStatusTest {

  private static final Logger LOG = LoggerFactory.getLogger(RepairScheduleStatusTest.class);

  @Test
  public void testJacksonJSONParsing() throws Exception {
    RepairScheduleStatus data = new RepairScheduleStatus();
    data.setClusterName("testCluster");
    data.setColumnFamilies(Lists.<String>newArrayList());
    data.setCreationTime(DateTime.now().withMillis(0));
    data.setDaysBetween(2);
    data.setId(1);
    data.setIntensity(0.75);
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
    assertEquals(data.getKeyspaceName(), dataAfter.getKeyspaceName());
    assertEquals(data.getRepairParallelism(), dataAfter.getRepairParallelism());
    assertEquals(data.getState(), dataAfter.getState());
  }

}
