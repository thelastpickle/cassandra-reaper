--
--  Copyright 2025-2025 DataStax, Inc.
--
--  Licensed under the Apache License, Version 2.0 (the "License")
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

-- Schema version tracking
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY,
    applied_at INTEGER NOT NULL
);

-- Cluster information
CREATE TABLE IF NOT EXISTS cluster (
    name TEXT PRIMARY KEY,
    partitioner TEXT,
    seed_hosts TEXT, -- JSON array
    properties TEXT, -- JSON object
    state TEXT,
    last_contact INTEGER, -- timestamp millis
    namespace TEXT
);

-- Repair units (keyspace + tables configuration for repair)
CREATE TABLE IF NOT EXISTS repair_unit (
    id BLOB PRIMARY KEY, -- UUID as bytes
    cluster_name TEXT NOT NULL,
    keyspace_name TEXT NOT NULL,
    column_families TEXT, -- JSON array (set<text>)
    incremental_repair INTEGER NOT NULL, -- boolean (0/1)
    subrange_incremental INTEGER NOT NULL, -- boolean (0/1)
    nodes TEXT, -- JSON array (set<text>)
    datacenters TEXT, -- JSON array (set<text>)
    blacklisted_tables TEXT, -- JSON array (set<text>)
    repair_thread_count INTEGER,
    timeout INTEGER
);

CREATE INDEX IF NOT EXISTS idx_repair_unit_cluster_keyspace ON repair_unit(cluster_name, keyspace_name);

-- Repair schedules
CREATE TABLE IF NOT EXISTS repair_schedule (
    id BLOB PRIMARY KEY, -- UUID as bytes
    repair_unit_id BLOB NOT NULL,
    owner TEXT,
    state TEXT NOT NULL,
    days_between INTEGER,
    next_activation INTEGER, -- timestamp millis
    creation_time INTEGER, -- timestamp millis
    pause_time INTEGER, -- timestamp millis
    intensity REAL NOT NULL,
    segment_count INTEGER,
    segment_count_per_node INTEGER,
    repair_parallelism TEXT NOT NULL,
    adaptive INTEGER NOT NULL, -- boolean (0/1)
    percent_unrepaired_threshold INTEGER,
    run_history TEXT, -- JSON array of UUIDs
    last_run BLOB, -- UUID as bytes
    FOREIGN KEY (repair_unit_id) REFERENCES repair_unit(id)
);

CREATE INDEX IF NOT EXISTS idx_repair_schedule_unit ON repair_schedule(repair_unit_id);
CREATE INDEX IF NOT EXISTS idx_repair_schedule_state ON repair_schedule(state);
CREATE INDEX IF NOT EXISTS idx_repair_schedule_next_activation ON repair_schedule(next_activation);

-- Repair runs
CREATE TABLE IF NOT EXISTS repair_run (
    id BLOB PRIMARY KEY, -- UUID as bytes
    cluster_name TEXT NOT NULL,
    repair_unit_id BLOB NOT NULL,
    cause TEXT,
    owner TEXT,
    state TEXT NOT NULL,
    creation_time INTEGER, -- timestamp millis
    start_time INTEGER, -- timestamp millis
    end_time INTEGER, -- timestamp millis
    pause_time INTEGER, -- timestamp millis
    intensity REAL NOT NULL,
    last_event TEXT,
    segment_count INTEGER NOT NULL,
    repair_parallelism TEXT NOT NULL,
    tables TEXT, -- JSON array (set<text>)
    adaptive_schedule INTEGER NOT NULL, -- boolean (0/1)
    FOREIGN KEY (repair_unit_id) REFERENCES repair_unit(id)
);

CREATE INDEX IF NOT EXISTS idx_repair_run_cluster ON repair_run(cluster_name);
CREATE INDEX IF NOT EXISTS idx_repair_run_unit ON repair_run(repair_unit_id);
CREATE INDEX IF NOT EXISTS idx_repair_run_state ON repair_run(state);
CREATE INDEX IF NOT EXISTS idx_repair_run_end_time ON repair_run(end_time);

-- Repair segments (individual token ranges to repair)
CREATE TABLE IF NOT EXISTS repair_segment (
    id BLOB PRIMARY KEY, -- UUID as bytes
    run_id BLOB NOT NULL,
    repair_unit_id BLOB NOT NULL,
    start_token TEXT, -- BigInteger as string
    end_token TEXT, -- BigInteger as string
    token_ranges TEXT, -- JSON array of token range objects
    state TEXT NOT NULL,
    coordinator_host TEXT,
    start_time INTEGER, -- timestamp millis
    end_time INTEGER, -- timestamp millis
    fail_count INTEGER NOT NULL DEFAULT 0,
    replicas TEXT, -- JSON object (map<text, text>)
    host_id BLOB, -- UUID as bytes
    FOREIGN KEY (run_id) REFERENCES repair_run(id) ON DELETE CASCADE,
    FOREIGN KEY (repair_unit_id) REFERENCES repair_unit(id)
);

CREATE INDEX IF NOT EXISTS idx_repair_segment_run ON repair_segment(run_id);
CREATE INDEX IF NOT EXISTS idx_repair_segment_state ON repair_segment(state);
CREATE INDEX IF NOT EXISTS idx_repair_segment_unit ON repair_segment(repair_unit_id);

-- Diagnostic event subscriptions
CREATE TABLE IF NOT EXISTS diag_event_subscription (
    id BLOB PRIMARY KEY, -- UUID as bytes
    cluster TEXT NOT NULL,
    description TEXT,
    nodes TEXT, -- JSON array (set<text>)
    events TEXT, -- JSON array (set<text>)
    export_sse INTEGER, -- boolean (0/1)
    export_file_logger TEXT,
    export_http_endpoint TEXT
);

CREATE INDEX IF NOT EXISTS idx_diag_event_subscription_cluster ON diag_event_subscription(cluster);

