--
-- Upgrade for fault tolerance addons

CREATE TABLE IF NOT EXISTS segment_leader (
	segment_id bigint PRIMARY KEY,
	reaper_instance_id uuid,
	reaper_instance_host text,
	last_heartbeat timestamp
) WITH default_time_to_live = 180;

CREATE TABLE IF NOT EXISTS host_metrics(
	host_address text,
	ts timestamp,
  pending_compactions int,
	has_repair_running boolean,
  active_anticompactions int,
  PRIMARY KEY(host_address)
) WITH default_time_to_live=180;

--
-- Add table to track concurrently running Reapers

CREATE TABLE IF NOT EXISTS running_reapers (
  reaper_instance_id uuid PRIMARY KEY,
  reaper_instance_host text,
	last_heartbeat timestamp
) WITH default_time_to_live = 180
  AND gc_grace_seconds = 10800;
