/*
 * Copyright 2016-2017 Spotify AB
 * Copyright 2016-2019 The Last Pickle Ltd
 * Copyright 2020-2020 DataStax, Inc.
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

/*
 * Copyright 2017-2017 Spotify AB
 * Copyright 2017-2019 The Last Pickle Ltd
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

package io.cassandrareaper.storage;

import io.cassandrareaper.core.RepairSegment;
import io.cassandrareaper.service.RingRange;
import io.cassandrareaper.storage.metrics.IDistributedMetrics;
import io.cassandrareaper.storage.operations.IOperationsDao;
import java.util.List;
import java.util.UUID;

/**
 * Definition for a storage that can run in distributed (peer-to-peer) mode. For example Cassandra.
 */
public interface IDistributedStorage extends IDistributedMetrics {

  boolean takeLead(UUID leaderId);

  boolean takeLead(UUID leaderId, int ttl);

  boolean renewLead(UUID leaderId);

  boolean renewLead(UUID leaderId, int ttl);

  List<UUID> getLeaders();

  void releaseLead(UUID leaderId);

  int countRunningReapers();

  List<UUID> getRunningReapers();

  void saveHeartbeat();

  /**
   * Gets the next free segment from the backend that is both within the parallel range and the
   * local node ranges.
   *
   * @param runId id of the repair run
   * @param ranges list of ranges we're looking a segment for
   * @return an optional repair segment to process
   */
  List<RepairSegment> getNextFreeSegmentsForRanges(UUID runId, List<RingRange> ranges);

  /** Purges old metrics from the database (no-op for databases w/ TTL) */
  void purgeMetrics();

  IOperationsDao getOperationsDao();
}
