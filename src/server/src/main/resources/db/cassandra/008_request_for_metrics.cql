--
--  Copyright 2017-2017 Spotify AB
--
--  Licensed under the Apache License, Version 2.0 (the "License");
--  you may not use this file except in compliance with the License.
--  You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
--  Unless required by applicable law or agreed to in writing, software
--  distributed under the License is distributed on an "AS IS" BASIS,
--  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--  See the License for the specific language governing permissions and
--  limitations under the License.
--
-- Upgrade to handle the metrics request mechanism


DROP TABLE IF EXISTS node_metrics;

CREATE TABLE IF NOT EXISTS node_metrics_v1 (
  time_partition          bigint,
  run_id                  uuid,
  node                    text,
  cluster                 text,
  datacenter              text,
  requested               boolean,
  pending_compactions     int,
  has_repair_running      boolean,
  active_anticompactions  int,
  PRIMARY KEY((run_id, time_partition), node)
)
  WITH compaction = {'class':'SizeTieredCompactionStrategy', 'unchecked_tombstone_compaction':'true'}
   AND caching = {'keys':'ALL', 'rows_per_partition':'ALL'}
   AND  gc_grace_seconds = 120
   AND  default_time_to_live = 180;
