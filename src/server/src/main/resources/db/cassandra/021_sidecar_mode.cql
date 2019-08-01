--
--  Copyright 2019-2019 The Last Pickle Ltd
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
-- Metrics table for the sidecar mode
--

CREATE TABLE IF NOT EXISTS node_metrics_v2 (
    cluster text, 
    metric_domain text,
    metric_type text,
    time_bucket text,
    host text,
    metric_scope text,
    metric_name text,
    ts timestamp,
    metric_attribute text,
    value double,
    PRIMARY KEY ((cluster, metric_domain, metric_type, time_bucket), host, metric_scope, metric_name, ts, metric_attribute)
) WITH CLUSTERING ORDER BY (host ASC, metric_scope ASC, metric_name ASC, ts DESC, metric_attribute ASC)
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4', 'unchecked_tombstone_compaction': 'true'}
    AND default_time_to_live = 864000
    AND gc_grace_seconds = 300;

CREATE TABLE IF NOT EXISTS node_operations (
    cluster text, 
    type text,
    time_bucket text,
    host text,
    ts timestamp,
    data text,
    PRIMARY KEY ((cluster, type, time_bucket), host)
) WITH CLUSTERING ORDER BY (host ASC)
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4', 'unchecked_tombstone_compaction': 'true'}
    AND default_time_to_live = 300
    AND gc_grace_seconds = 300;
