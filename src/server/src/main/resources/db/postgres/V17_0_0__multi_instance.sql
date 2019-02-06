CREATE TABLE IF NOT EXISTS "leader" (
  "leader_id" BIGINT PRIMARY KEY,
  "reaper_instance_id" BIGINT,
  "reaper_instance_host" TEXT,
  "last_heartbeat" TIMESTAMP WITH TIME ZONE
); 

CREATE TABLE IF NOT EXISTS "running_reapers" (
  "reaper_instance_id" BIGINT PRIMARY KEY,
  "reaper_instance_host" TEXT,
  "last_heartbeat" TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS "node_metrics_v1" (
  "run_id"                  BIGINT,
  "ts"                      TIMESTAMP WITH TIME ZONE,
  "node"                    TEXT,
  "cluster"                 TEXT,
  "datacenter"              TEXT,
  "requested"               BOOLEAN,
  "pending_compactions"     INT,
  "has_repair_running"      BOOLEAN,
  "active_anticompactions"  INT,
  PRIMARY KEY("run_id", "ts", "node")
);
