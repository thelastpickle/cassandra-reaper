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
-- Optimal table properties

ALTER TABLE repair_unit_v1
  WITH caching = {'keys':'ALL', 'rows_per_partition':'1'};

ALTER TABLE repair_schedule_by_cluster_and_keyspace
  WITH caching = {'keys':'ALL', 'rows_per_partition':'10'};

ALTER TABLE repair_run_by_cluster
  WITH caching = {'keys':'ALL', 'rows_per_partition':'ALL'};

ALTER TABLE repair_schedule_v1
  WITH caching = {'keys':'ALL', 'rows_per_partition':'1'};

ALTER TABLE cluster
  WITH caching = {'keys':'ALL', 'rows_per_partition':'ALL'};

ALTER TABLE repair_run
  WITH caching = {'keys':'ALL', 'rows_per_partition':'5000'};

ALTER TABLE repair_run_by_unit
  WITH caching = {'keys':'ALL', 'rows_per_partition':'ALL'};
