--
--  Copyright 2021-2021 Datastax inc.
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
-- Add a table to store the percent repaired metrics for incremental repair schedules

CREATE TABLE IF NOT EXISTS percent_repaired_by_schedule(
     cluster_name text,
     repair_schedule_id uuid,
     time_bucket text,
     node text,
     keyspace_name text,
     table_name text,
     percent_repaired int,
     ts timestamp,
     PRIMARY KEY((cluster_name, repair_schedule_id, time_bucket), node))
WITH CLUSTERING ORDER BY (node ASC)
 AND default_time_to_live = 3600;