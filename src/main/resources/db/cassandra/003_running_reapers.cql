--
-- Add table to track concurrently running Reapers

CREATE TABLE IF NOT EXISTS running_reapers (
  reaper_instance_id uuid PRIMARY KEY,
  reaper_instance_host text,
	last_heartbeat timestamp
) WITH default_time_to_live = 180
  AND gc_grace_seconds = 10800;
