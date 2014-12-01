package com.spotify.reaper.cassandra;

import org.apache.cassandra.service.ActiveRepairService;

public interface RepairStatusHandlder {

  /**
   * Handle an event representing a change in the state of a running repair.
   *
   * Implementation of this method is intended to persist the repair state change in Reaper's
   * state.
   * @param repairNumber repair sequence number, obtained when triggering a repair
   * @param status new status of the repair
   * @param message additional information about the repair
   */
  public void handle(int repairNumber, ActiveRepairService.Status status, String message);

}
