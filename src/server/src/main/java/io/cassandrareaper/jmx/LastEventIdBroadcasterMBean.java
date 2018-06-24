/*
 * Copyright 2018-2018 Stefan Podkowinski
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

package io.cassandrareaper.jmx;

import java.util.Map;

public interface LastEventIdBroadcasterMBean {

  /**
   * Retrieves a list of all event types and their highest IDs.
   */
  Map<String, Comparable> getLastEventIds();

  /**
   * Retrieves a list of all event types and their highest IDs, if updated since specified timestamp, or null.
   * @param lastUpdate timestamp to use to determine if IDs have been updated
   */
  Map<String, Comparable> getLastEventIdsIfModified(long lastUpdate);
}
