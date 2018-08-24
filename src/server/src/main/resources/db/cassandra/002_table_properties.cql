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

ALTER TABLE cluster
    WITH compaction = {'class': 'LeveledCompactionStrategy'}
    AND caching = {'rows_per_partition': 'ALL'};

ALTER TABLE repair_unit
    WITH compaction = {'class': 'LeveledCompactionStrategy'}
    AND caching = {'rows_per_partition': 10};

ALTER TABLE repair_run
    WITH compaction = {'class': 'LeveledCompactionStrategy'}
    AND caching = {'rows_per_partition': 10};

ALTER TABLE repair_run_by_cluster
    WITH compaction = {'class': 'LeveledCompactionStrategy'}
    AND caching = {'rows_per_partition': 10};

ALTER TABLE repair_run_by_unit
    WITH compaction = {'class': 'LeveledCompactionStrategy'}
    AND caching = {'rows_per_partition': 10};

ALTER TABLE repair_segment
    WITH compaction = {'class': 'LeveledCompactionStrategy'};

ALTER TABLE repair_segment_by_run_id
    WITH compaction = {'class': 'LeveledCompactionStrategy'}
    AND caching = {'rows_per_partition': 10};

ALTER TABLE repair_schedule
    WITH compaction = {'class': 'LeveledCompactionStrategy'}
    AND caching = {'rows_per_partition': 10};

ALTER TABLE repair_schedule_by_cluster_and_keyspace
    WITH compaction = {'class': 'LeveledCompactionStrategy'}
    AND caching = {'rows_per_partition': 10};

ALTER TABLE repair_id
    WITH compaction = {'class': 'LeveledCompactionStrategy'}
    AND caching = {'rows_per_partition': 10};
