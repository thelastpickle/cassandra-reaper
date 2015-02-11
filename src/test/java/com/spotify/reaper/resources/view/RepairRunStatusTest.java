package com.spotify.reaper.resources.view;

import com.spotify.reaper.resources.CommonTools;

import org.junit.Test;

import static org.junit.Assert.*;

public class RepairRunStatusTest {

  @Test
  public void testRoundIntensity() throws Exception {
    assertEquals(0.0f, CommonTools.roundDoubleNicely(0.0f), 0.00000f);
    assertEquals(0.1f, CommonTools.roundDoubleNicely(0.1f), 0.00001f);
    assertEquals(0.2f, CommonTools.roundDoubleNicely(0.2f), 0.00001f);
    assertEquals(0.3f, CommonTools.roundDoubleNicely(0.3f), 0.00001f);
    assertEquals(0.4f, CommonTools.roundDoubleNicely(0.4f), 0.00001f);
    assertEquals(0.5f, CommonTools.roundDoubleNicely(0.5f), 0.00001f);
    assertEquals(0.6f, CommonTools.roundDoubleNicely(0.6f), 0.00001f);
    assertEquals(0.7f, CommonTools.roundDoubleNicely(0.7f), 0.00001f);
    assertEquals(0.8f, CommonTools.roundDoubleNicely(0.8f), 0.00001f);
    assertEquals(0.9f, CommonTools.roundDoubleNicely(0.9f), 0.00001f);
    assertEquals(1.0f, CommonTools.roundDoubleNicely(1.0f), 0.00001f);
  }
}
