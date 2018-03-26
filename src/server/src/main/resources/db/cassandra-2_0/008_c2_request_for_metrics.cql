--
-- Upgrade to handle the metrics request mechanism

use reaper_db;


DROP TABLE IF EXISTS node_metrics;

CREATE TABLE IF NOT EXISTS node_metrics_v1 (
  time_partition          bigint,
  run_id                  uuid,
  node                    text,
  cluster                 text,
  datacenter              text,
  requested               boolean,
  pending_compactions     int,
  has_repair_running      boolean,
  active_anticompactions  int,
  PRIMARY KEY((run_id, time_partition), node)
)
  WITH compaction = {'class':'SizeTieredCompactionStrategy', 'unchecked_tombstone_compaction':'true'}
    AND caching ='all'
   AND  gc_grace_seconds = 120
   AND  default_time_to_live = 180
   AND dclocal_read_repair_chance = 0.0;
