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
-- Upgrade for fault tolerance addons

CREATE TABLE IF NOT EXISTS leader (
	leader_id timeuuid PRIMARY KEY,
	reaper_instance_id uuid,
	reaper_instance_host text,
	last_heartbeat timestamp
) WITH compaction = {'class': 'LeveledCompactionStrategy'}
    AND default_time_to_live = 600
    AND gc_grace_seconds = 600;

--
-- Add table to track concurrently running Reapers

CREATE TABLE IF NOT EXISTS running_reapers (
    reaper_instance_id uuid PRIMARY KEY,
    reaper_instance_host text,
    last_heartbeat timestamp
) WITH compaction = {'class': 'LeveledCompactionStrategy'}
    AND default_time_to_live = 180
    AND gc_grace_seconds = 180;


--
-- Add table to store metrics of Cassandra nodes

CREATE TABLE IF NOT EXISTS node_metrics (
    host_address text,
    datacenter text,
    pending_compactions int,
    has_repair_running boolean,
    active_anticompactions int,
    PRIMARY KEY (host_address)
 ) WITH compaction = {'class': 'LeveledCompactionStrategy'}
    AND default_time_to_live = 180
    AND gc_grace_seconds = 180;
