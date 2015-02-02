package com.spotify.reaper.resources.view;

import org.junit.Test;

import static org.junit.Assert.*;

public class RepairRunStatusTest {

  @Test
  public void testRoundIntensity() throws Exception {
    assertEquals(0.0f, RepairRunStatus.roundIntensity(0.0f), 0.00000f);
    assertEquals(0.1f, RepairRunStatus.roundIntensity(0.1f), 0.00001f);
    assertEquals(0.2f, RepairRunStatus.roundIntensity(0.2f), 0.00001f);
    assertEquals(0.3f, RepairRunStatus.roundIntensity(0.3f), 0.00001f);
    assertEquals(0.4f, RepairRunStatus.roundIntensity(0.4f), 0.00001f);
    assertEquals(0.5f, RepairRunStatus.roundIntensity(0.5f), 0.00001f);
    assertEquals(0.6f, RepairRunStatus.roundIntensity(0.6f), 0.00001f);
    assertEquals(0.7f, RepairRunStatus.roundIntensity(0.7f), 0.00001f);
    assertEquals(0.8f, RepairRunStatus.roundIntensity(0.8f), 0.00001f);
    assertEquals(0.9f, RepairRunStatus.roundIntensity(0.9f), 0.00001f);
    assertEquals(1.0f, RepairRunStatus.roundIntensity(1.0f), 0.00001f);
  }
}
