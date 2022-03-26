--
--  Copyright 2020 DataStax, Inc.
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

CREATE TABLE IF NOT EXISTS repair_schedule_by_cluster_and_keyspace (
    cluster_name text,
    keyspace_name text,
    repair_schedule_id timeuuid,
    PRIMARY KEY ((cluster_name, keyspace_name), repair_schedule_id)
);

CREATE TABLE IF NOT EXISTS diagnostic_event_subscription (
    id uuid PRIMARY KEY,
    cluster text,
    description text,
    events set<text>,
    export_file_logger text,
    export_http_endpoint text,
    export_sse boolean,
    nodes set<text>
);

CREATE TABLE IF NOT EXISTS running_reapers (
    reaper_instance_id uuid PRIMARY KEY,
    last_heartbeat timestamp,
    reaper_instance_host text
);

CREATE TABLE IF NOT EXISTS repair_unit_v1 (
    id timeuuid PRIMARY KEY,
    blacklisted_tables set<text>,
    cluster_name text,
    column_families set<text>,
    datacenters set<text>,
    incremental_repair boolean,
    keyspace_name text,
    nodes set<text>,
    repair_thread_count int
);

CREATE TABLE IF NOT EXISTS node_operations (
    cluster text,
    type text,
    time_bucket text,
    host text,
    data text,
    ts timestamp,
    PRIMARY KEY ((cluster, type, time_bucket), host)
) WITH default_time_to_live = 300;

CREATE TABLE IF NOT EXISTS repair_run (
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
    tables set<text> static,
    token_ranges text,
    PRIMARY KEY (id, segment_id)
);

CREATE TABLE IF NOT EXISTS repair_schedule_v1 (
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
);

CREATE TABLE IF NOT EXISTS cluster (
    name text PRIMARY KEY,
    last_contact timestamp,
    partitioner text,
    properties text,
    seed_hosts set<text>,
    state text
);

CREATE TABLE IF NOT EXISTS repair_run_by_cluster_v2 (
    cluster_name text,
    id timeuuid,
    repair_run_state text,
    PRIMARY KEY (cluster_name, id)
) WITH CLUSTERING ORDER BY (id DESC);

CREATE TABLE IF NOT EXISTS snapshot (
    cluster text,
    snapshot_name text,
    cause text,
    creation_time timestamp,
    owner text,
    PRIMARY KEY ((cluster, snapshot_name))
);

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
  AND default_time_to_live = 180;

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
  AND default_time_to_live = 3600;

CREATE TABLE IF NOT EXISTS repair_run_by_unit (
    repair_unit_id timeuuid,
    id timeuuid,
    PRIMARY KEY (repair_unit_id, id)
);

CREATE TABLE IF NOT EXISTS leader (
    leader_id timeuuid PRIMARY KEY,
    last_heartbeat timestamp,
    reaper_instance_host text,
    reaper_instance_id uuid
) WITH default_time_to_live = 600;

