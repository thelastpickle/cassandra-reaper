CREATE TABLE IF NOT EXISTS running_repairs (
  repair_id BIGINT,
  node VARCHAR,
  reaper_instance_host VARCHAR,
  reaper_instance_id BIGINT,
  segment_id INT,
  last_heartbeat TIMESTAMP WITH TIME ZONE,
  PRIMARY KEY(repair_id, node)
);