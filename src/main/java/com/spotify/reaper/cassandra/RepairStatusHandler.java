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
package com.spotify.reaper.cassandra;

import org.apache.cassandra.service.ActiveRepairService;

public interface RepairStatusHandler {

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
