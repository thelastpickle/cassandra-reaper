--
-- Upgrade for fault tolerance addons

CREATE TABLE IF NOT EXISTS segment_leader (
	segment_id timeuuid PRIMARY KEY,
	reaper_instance_id uuid,
	reaper_instance_host text,
	last_heartbeat timestamp
) WITH compaction = {'class': 'LeveledCompactionStrategy'}
    AND default_time_to_live = 600
    AND gc_grace_seconds = 600;

CREATE TABLE IF NOT EXISTS host_metrics(
	host_address text,
	ts timestamp,
  pending_compactions int,
	has_repair_running boolean,
  active_anticompactions int,
  PRIMARY KEY(host_address, ts)
) WITH CLUSTERING ORDER BY (ts DESC)
    AND  compaction = {'class': 'LeveledCompactionStrategy'}
    AND default_time_to_live = 600
	AND  gc_grace_seconds = 600;

--
-- Add table to track concurrently running Reapers

CREATE TABLE IF NOT EXISTS running_reapers (
  reaper_instance_id uuid PRIMARY KEY,
  reaper_instance_host text,
	last_heartbeat timestamp
) WITH compaction = {'class': 'LeveledCompactionStrategy'}
    AND default_time_to_live = 600
    AND gc_grace_seconds = 600;
