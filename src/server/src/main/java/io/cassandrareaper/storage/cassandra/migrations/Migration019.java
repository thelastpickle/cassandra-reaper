/*
 *
 * Copyright 2019-2019 The Last Pickle Ltd
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

package io.cassandrareaper.storage.cassandra.migrations;

import com.datastax.oss.driver.api.core.CqlSession;

public final class Migration019 {

  private Migration019() {}

  /** fix repair start, pause and end times in the repair_run table. */
  public static void migrate(CqlSession session) {
    FixRepairRunTimestamps.migrate(session);
  }
}
