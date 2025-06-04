/*
 * Copyright 2014-2017 Spotify AB
 * Copyright 2016-2018 The Last Pickle Ltd
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

import java.util.Optional;
import org.apache.cassandra.utils.progress.ProgressEventType;

public interface RepairStatusHandler {

  /**
   * Handle an event representing a change in the state of a running repair.
   *
   * <p>Implementation of this method is intended to persist the repair state change in Reaper's
   * state.
   *
   * @param repairNumber repair sequence number, obtained when triggering a repair
   * @param progress new status of the repair (new API)
   * @param message additional information about the repair
   */
  void handle(
      int repairNumber,
      Optional<ProgressEventType> progress,
      String message,
      ICassandraManagementProxy cassandraManagementProxy);
}
