--
-- Upgrade to handle snapshots metadata

CREATE TABLE IF NOT EXISTS snapshot (
  cluster                 text,
  snapshot_name           text,
  owner                   text,
  cause                   text,
  creation_time           timestamp,
  PRIMARY KEY((cluster, snapshot_name))
)
WITH compaction = {'class':'LeveledCompactionStrategy'}
   AND caching = {'keys':'ALL', 'rows_per_partition':'ALL'}
   AND  gc_grace_seconds = 864000
   AND  default_time_to_live = 0;
