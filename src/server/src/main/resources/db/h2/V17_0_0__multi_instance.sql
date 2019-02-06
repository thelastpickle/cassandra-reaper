-- H2-compatible version of multi-instance reaper Postgres DDL
-- CHANGES:
--    "node" TEXT  -->  "node" VARCHAR(255)  because H2 doesn't support index on TEXT

CREATE TABLE IF NOT EXISTS leader (
  leader_id BIGINT PRIMARY KEY,
  reaper_instance_id BIGINT,
  reaper_instance_host TEXT,
  last_heartbeat TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS running_reapers (
  reaper_instance_id BIGINT PRIMARY KEY,
  reaper_instance_host TEXT,
  last_heartbeat TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS node_metrics_v1 (
  time_partition          BIGINT,
  run_id                  BIGINT,
  node                    VARCHAR(255),
  cluster                 TEXT,
  datacenter              TEXT,
  requested               BOOLEAN,
  pending_compactions     INT,
  has_repair_running      BOOLEAN,
  active_anticompactions  INT,
  PRIMARY KEY(run_id, time_partition, node)
);