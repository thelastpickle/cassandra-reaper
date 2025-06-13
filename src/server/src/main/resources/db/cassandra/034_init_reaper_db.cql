--
--  Copyright 2025-2025 DataStax, Inc.
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

CREATE TABLE IF NOT EXISTS cluster (
    name text PRIMARY KEY,
    last_contact timestamp,
    namespace text,
    partitioner text,
    properties text,
    state text,
    seed_hosts set<text>
) WITH additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
    AND cdc = false
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128;

CREATE TABLE IF NOT EXISTS diagnostic_event_subscription (
    id uuid PRIMARY KEY,
    cluster text,
    description text,
    export_file_logger text,
    export_http_endpoint text,
    export_sse boolean,
    events set<text>,
    nodes set<text>
) WITH additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.1
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
    AND cdc = false
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128;

CREATE TABLE IF NOT EXISTS leader (
    leader_id timeuuid PRIMARY KEY,
    last_heartbeat timestamp,
    reaper_instance_host text,
    reaper_instance_id uuid
) WITH additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.1
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND cdc = false
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND default_time_to_live = 600
    AND gc_grace_seconds = 600
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128;

CREATE TABLE IF NOT EXISTS node_metrics_v1 (
    run_id uuid,
    time_partition bigint,
    node text,
    active_anticompactions int,
    cluster text,
    datacenter text,
    has_repair_running boolean,
    pending_compactions int,
    requested boolean,
    PRIMARY KEY ((run_id, time_partition), node)
) WITH CLUSTERING ORDER BY (node ASC)
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND cdc = false
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy', 'compaction_window_size': '2', 'compaction_window_unit': 'MINUTES', 'max_threshold': '32', 'min_threshold': '4', 'unchecked_tombstone_compaction': 'true'}
    AND compression = {'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND default_time_to_live = 180
    AND gc_grace_seconds = 120
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128;

CREATE TABLE IF NOT EXISTS node_metrics_v3 (
    cluster text,
    metric_domain text,
    metric_type text,
    time_bucket text,
    host text,
    ts timestamp,
    metric_scope text,
    metric_name text,
    metric_attribute text,
    value double,
    PRIMARY KEY ((cluster, metric_domain, metric_type, time_bucket, host), ts, metric_scope, metric_name, metric_attribute)
) WITH CLUSTERING ORDER BY (ts DESC, metric_scope ASC, metric_name ASC, metric_attribute ASC)
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND cdc = false
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy', 'compaction_window_size': '10', 'compaction_window_unit': 'MINUTES', 'max_threshold': '32', 'min_threshold': '4', 'unchecked_tombstone_compaction': 'true'}
    AND compression = {'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND default_time_to_live = 3600
    AND gc_grace_seconds = 300
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128;

CREATE TABLE IF NOT EXISTS node_operations (
    cluster text,
    type text,
    time_bucket text,
    host text,
    data text,
    ts timestamp,
    PRIMARY KEY ((cluster, type, time_bucket), host)
) WITH CLUSTERING ORDER BY (host ASC)
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND cdc = false
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy', 'compaction_window_size': '30', 'compaction_window_unit': 'MINUTES', 'max_threshold': '32', 'min_threshold': '4', 'unchecked_tombstone_compaction': 'true'}
    AND compression = {'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND default_time_to_live = 300
    AND gc_grace_seconds = 300
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128;

CREATE TABLE IF NOT EXISTS percent_repaired_by_schedule (
    cluster_name text,
    repair_schedule_id uuid,
    time_bucket text,
    node text,
    keyspace_name text,
    percent_repaired int,
    table_name text,
    ts timestamp,
    PRIMARY KEY ((cluster_name, repair_schedule_id, time_bucket), node)
) WITH CLUSTERING ORDER BY (node ASC)
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.1
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND cdc = false
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND default_time_to_live = 3600
    AND gc_grace_seconds = 300
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128;

CREATE TABLE IF NOT EXISTS repair_run (
    id timeuuid,
    segment_id timeuuid,
    adaptive_schedule boolean static,
    cause text static,
    cluster_name text static,
    creation_time timestamp static,
    end_time timestamp static,
    intensity double static,
    last_event text static,
    owner text static,
    pause_time timestamp static,
    repair_parallelism text static,
    repair_unit_id timeuuid static,
    segment_count int static,
    start_time timestamp static,
    state text static,
    tables set<text> static,
    coordinator_host text,
    end_token varint,
    fail_count int,
    host_id uuid,
    replicas frozen<map<text, text>>,
    segment_end_time timestamp,
    segment_start_time timestamp,
    segment_state int,
    start_token varint,
    token_ranges text,
    PRIMARY KEY (id, segment_id)
) WITH CLUSTERING ORDER BY (segment_id ASC)
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.1
    AND caching = {'keys': 'ALL', 'rows_per_partition': '5000'}
    AND cdc = false
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128;

CREATE TABLE IF NOT EXISTS repair_run_by_cluster (
    cluster_name text,
    id timeuuid,
    PRIMARY KEY (cluster_name, id)
) WITH CLUSTERING ORDER BY (id ASC)
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.1
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND cdc = false
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128;

CREATE TABLE IF NOT EXISTS repair_run_by_cluster_v2 (
    cluster_name text,
    id timeuuid,
    repair_run_state text,
    PRIMARY KEY (cluster_name, id)
) WITH CLUSTERING ORDER BY (id DESC)
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.1
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND cdc = false
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128;

CREATE INDEX IF NOT EXISTS state2i ON repair_run_by_cluster_v2 (repair_run_state);

CREATE TABLE IF NOT EXISTS repair_run_by_unit (
    repair_unit_id timeuuid,
    id timeuuid,
    PRIMARY KEY (repair_unit_id, id)
) WITH CLUSTERING ORDER BY (id ASC)
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.1
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
    AND cdc = false
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128;

CREATE TABLE IF NOT EXISTS repair_schedule_by_cluster_and_keyspace (
    cluster_name text,
    keyspace_name text,
    repair_schedule_id timeuuid,
    PRIMARY KEY ((cluster_name, keyspace_name), repair_schedule_id)
) WITH CLUSTERING ORDER BY (repair_schedule_id ASC)
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.1
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND cdc = false
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128;

CREATE TABLE IF NOT EXISTS repair_schedule_v1 (
    id timeuuid PRIMARY KEY,
    adaptive boolean,
    creation_time timestamp,
    days_between int,
    intensity double,
    last_run timeuuid,
    next_activation timestamp,
    owner text,
    pause_time timestamp,
    percent_unrepaired_threshold int,
    repair_parallelism text,
    repair_unit_id timeuuid,
    segment_count int,
    segment_count_per_node int,
    state text,
    run_history set<timeuuid>
) WITH additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.1
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND cdc = false
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128;

CREATE TABLE IF NOT EXISTS repair_unit_v1 (
    id timeuuid PRIMARY KEY,
    cluster_name text,
    incremental_repair boolean,
    keyspace_name text,
    repair_thread_count int,
    subrange_incremental boolean,
    timeout int,
    blacklisted_tables set<text>,
    column_families set<text>,
    datacenters set<text>,
    nodes set<text>
) WITH additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.1
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND cdc = false
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128;

CREATE TABLE IF NOT EXISTS running_reapers (
    reaper_instance_id uuid PRIMARY KEY,
    last_heartbeat timestamp,
    reaper_instance_host text
) WITH additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.1
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND cdc = false
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND default_time_to_live = 180
    AND gc_grace_seconds = 180
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128;

CREATE TABLE IF NOT EXISTS running_repairs (
    repair_id uuid,
    node text,
    reaper_instance_host text,
    reaper_instance_id uuid,
    segment_id uuid,
    PRIMARY KEY (repair_id, node)
) WITH CLUSTERING ORDER BY (node ASC)
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.1
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND cdc = false
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND default_time_to_live = 300
    AND gc_grace_seconds = 300
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128;

CREATE TABLE IF NOT EXISTS schema_migration (
    applied_successful boolean,
    version int,
    executed_at timestamp,
    script text,
    script_name text,
    PRIMARY KEY (applied_successful, version)
) WITH CLUSTERING ORDER BY (version ASC)
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND cdc = false
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128;

CREATE TABLE IF NOT EXISTS schema_migration_leader (
    keyspace_name text PRIMARY KEY,
    leader uuid,
    leader_hostname text,
    took_lead_at timestamp
) WITH additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND cdc = false
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128;

CREATE TABLE IF NOT EXISTS snapshot (
    cluster text,
    snapshot_name text,
    cause text,
    creation_time timestamp,
    owner text,
    PRIMARY KEY ((cluster, snapshot_name))
) WITH additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.1
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
    AND cdc = false
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128;