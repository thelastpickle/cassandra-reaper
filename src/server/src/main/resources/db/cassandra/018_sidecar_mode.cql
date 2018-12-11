--
-- Metrics table for the sidecar mode
--

CREATE TABLE node_metrics_v2 (
    cluster text, 
    metric text,
    time_bucket text,
    host text,
    ts timestamp,
    value double,
    PRIMARY KEY ((cluster, metric, time_bucket), host, ts)
) WITH CLUSTERING ORDER BY (host ASC, ts DESC)
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4', 'unchecked_tombstone_compaction': 'true'}
    AND default_time_to_live = 864000
    AND gc_grace_seconds = 10800;
