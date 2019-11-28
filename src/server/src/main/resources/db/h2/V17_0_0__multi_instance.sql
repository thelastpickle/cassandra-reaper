-- H2-compatible version of multi-instance reaper Postgres DDL
-- CHANGES:
--    node VARCHAR  -->  node VARCHAR(255)  because H2 doesn't support index on VARCHAR

CREATE TABLE IF NOT EXISTS leader (
  leader_id BIGINT PRIMARY KEY,
  reaper_instance_id BIGINT,
  reaper_instance_host VARCHAR,
  last_heartbeat TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS running_reapers (
  reaper_instance_id BIGINT PRIMARY KEY,
  reaper_instance_host VARCHAR,
  last_heartbeat TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS node_metrics_v1 (
  run_id                  BIGINT,
  ts                      TIMESTAMP WITH TIME ZONE,
  node                    VARCHAR,
  cluster                 VARCHAR,
  datacenter              VARCHAR,
  requested               BOOLEAN,
  pending_compactions     INT,
  has_repair_running      BOOLEAN,
  active_anticompactions  INT,
  PRIMARY KEY (run_id, ts, node)
);