--
--  Copyright 2018-2019 The Last Pickle Ltd
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

CREATE TABLE IF NOT EXISTS reaper_db.running_reapers (
    reaper_instance_id uuid PRIMARY KEY,
    last_heartbeat timestamp,
    reaper_instance_host text
) WITH bloom_filter_fp_chance = 0.1
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'}
    AND default_time_to_live = 180
    AND gc_grace_seconds = 180;

CREATE TABLE IF NOT EXISTS reaper_db.repair_unit_v1 (
    id timeuuid PRIMARY KEY,
    blacklisted_tables set<text>,
    cluster_name text,
    column_families set<text>,
    "datacenters" set<text>,
    incremental_repair boolean,
    keyspace_name text,
    nodes set<text>,
    repair_thread_count int
) WITH bloom_filter_fp_chance = 0.1
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'}
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000;

CREATE TABLE IF NOT EXISTS reaper_db.repair_schedule_by_cluster_and_keyspace (
    cluster_name text,
    keyspace_name text,
    repair_schedule_id timeuuid,
    PRIMARY KEY ((cluster_name, keyspace_name), repair_schedule_id)
) WITH CLUSTERING ORDER BY (repair_schedule_id ASC)
    AND bloom_filter_fp_chance = 0.1
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'}
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000;

CREATE TABLE IF NOT EXISTS reaper_db.repair_run_by_cluster (
    cluster_name text,
    id timeuuid,
    PRIMARY KEY (cluster_name, id)
) WITH CLUSTERING ORDER BY (id ASC)
    AND bloom_filter_fp_chance = 0.1
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'}
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000;

CREATE TABLE IF NOT EXISTS reaper_db.repair_schedule_v1 (
    id timeuuid PRIMARY KEY,
    creation_time timestamp,
    days_between int,
    intensity double,
    next_activation timestamp,
    owner text,
    pause_time timestamp,
    repair_parallelism text,
    repair_unit_id timeuuid,
    run_history set<timeuuid>,
    segment_count int,
    segment_count_per_node int,
    state text
) WITH bloom_filter_fp_chance = 0.1
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'}
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000;

CREATE TABLE IF NOT EXISTS reaper_db.cluster (
    name text PRIMARY KEY,
    partitioner text,
    seed_hosts set<text>
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys':'ALL', 'rows_per_partition':'ALL'}
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'}
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000;

CREATE TABLE IF NOT EXISTS reaper_db.snapshot (
    cluster text,
    snapshot_name text,
    cause text,
    creation_time timestamp,
    owner text,
    PRIMARY KEY ((cluster, snapshot_name))
) WITH bloom_filter_fp_chance = 0.1
    AND caching = {'keys':'ALL', 'rows_per_partition':'ALL'}
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'}
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000;

CREATE TABLE IF NOT EXISTS reaper_db.node_metrics_v1 (
    run_id uuid,
    time_partition bigint,
    node text,
    active_anticompactions int,
    cluster text,
    "datacenter" text,
    has_repair_running boolean,
    pending_compactions int,
    requested boolean,
    PRIMARY KEY ((run_id, time_partition), node)
) WITH CLUSTERING ORDER BY (node ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys':'ALL', 'rows_per_partition':'NONE'}
    AND compaction = {'unchecked_tombstone_compaction': 'true', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'}
    AND default_time_to_live = 180
    AND gc_grace_seconds = 120;

CREATE TABLE IF NOT EXISTS reaper_db.repair_run (
    id timeuuid,
    segment_id timeuuid,
    cause text static,
    cluster_name text static,
    coordinator_host text,
    creation_time timestamp static,
    end_time timestamp static,
    end_token varint,
    fail_count int,
    intensity double static,
    last_event text static,
    owner text static,
    pause_time timestamp static,
    repair_parallelism text static,
    repair_unit_id timeuuid static,
    segment_count int static,
    segment_end_time timestamp,
    segment_start_time timestamp,
    segment_state int,
    start_time timestamp static,
    start_token varint,
    state text static,
    token_ranges text,
    PRIMARY KEY (id, segment_id)
) WITH CLUSTERING ORDER BY (segment_id ASC)
    AND bloom_filter_fp_chance = 0.1
    AND caching = {'keys':'ALL', 'rows_per_partition':'5000'}
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'}
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000;

CREATE TABLE IF NOT EXISTS reaper_db.repair_run_by_unit (
    repair_unit_id timeuuid,
    id timeuuid,
    PRIMARY KEY (repair_unit_id, id)
) WITH CLUSTERING ORDER BY (id ASC)
    AND bloom_filter_fp_chance = 0.1
    AND caching = {'keys':'ALL', 'rows_per_partition':'ALL'}
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'}
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000;

CREATE TABLE IF NOT EXISTS reaper_db.leader (
    leader_id timeuuid PRIMARY KEY,
    last_heartbeat timestamp,
    reaper_instance_host text,
    reaper_instance_id uuid
) WITH bloom_filter_fp_chance = 0.1
    AND caching = {'keys':'ALL', 'rows_per_partition':'NONE'}
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'}
    AND default_time_to_live = 600
    AND gc_grace_seconds = 600;

