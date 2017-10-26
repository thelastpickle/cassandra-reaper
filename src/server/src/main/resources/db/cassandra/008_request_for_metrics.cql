--
-- Upgrade to handle the metrics request mechanism

CREATE TABLE IF NOT EXISTS node_metrics_request (
  time_partition  text,
  run_id          uuid,
  node            text,
  dc              text,
  segment_id      uuid,
  PRIMARY KEY((run_id, time_partition), node)
)
WITH compaction = {'class':'SizeTieredCompactionStrategy', 'unchecked_tombstone_compaction':'true'}
 AND  gc_grace_seconds = 120
 AND  default_time_to_live = 180;
