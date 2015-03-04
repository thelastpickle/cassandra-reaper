package com.spotify.reaper.resources.view.hierarchy;

import com.spotify.reaper.core.RepairSegment;
import com.spotify.reaper.service.RingRange;

public class HSegment {

  public final long id;
  public final RingRange tokenRange;
  public final RepairSegment.State state;

  public HSegment(RepairSegment segment) {
    id = segment.getId();
    tokenRange = segment.getTokenRange();
    state = segment.getState();
  }
}
