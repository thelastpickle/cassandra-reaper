--
-- Upgrade for fault tolerance addons

CREATE TABLE IF NOT EXISTS segment_leader (
	segment_id bigint PRIMARY KEY,
	reaper_instance_id uuid,
	reaper_instance_host text,
	last_heartbeat timestamp
) WITH default_time_to_live = 180;
