--
--  Copyright 2020-2020 The Last Pickle Ltd
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
-- Upgrade to handle diagnostic event subscriptions

DROP TABLE IF EXISTS node_metrics_v2;

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
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4', 'unchecked_tombstone_compaction': 'true'}
    AND default_time_to_live = 3600
    AND gc_grace_seconds = 300;
