-- H2-compatible version of multi-instance reaper Postgres DDL
-- CHANGES:
--     TEXT  --> VARCHAR(255)  because H2 doesn't support index on TEXT

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
  "node"                    VARCHAR(255),
  "cluster"                 TEXT,
  "datacenter"              TEXT,
  "requested"               BOOLEAN,
  "pending_compactions"     INT,
  "has_repair_running"      BOOLEAN,
  "active_anticompactions"  INT,
  PRIMARY KEY("run_id", "ts", "node")
);

--- Sidecar mode

CREATE TABLE IF NOT EXISTS "node_metrics_v2_source_nodes" (
  "source_node_id" SERIAL UNIQUE,
  "cluster" VARCHAR(255),
  "host" VARCHAR(255),
  "last_updated" TIMESTAMP WITH TIME ZONE,
  PRIMARY KEY ("cluster", "host")
);

CREATE TABLE IF NOT EXISTS "node_metrics_v2_metric_types" (
  "metric_type_id" SERIAL UNIQUE,
  "metric_domain" VARCHAR(255),
  "metric_type" VARCHAR(255),
  "metric_scope" VARCHAR(255),
  "metric_name" VARCHAR(255),
  "metric_attribute" VARCHAR(255),
  PRIMARY KEY ("metric_domain", "metric_type", "metric_scope", "metric_name", "metric_attribute")
);

CREATE TABLE IF NOT EXISTS "node_metrics_v2" (
  "metric_type_id" INT REFERENCES "node_metrics_v2_metric_types"("metric_type_id"),
  "source_node_id" INT REFERENCES "node_metrics_v2_source_nodes"("source_node_id"),
  "ts" TIMESTAMP WITH TIME ZONE,
  "value" DOUBLE PRECISION,
  PRIMARY KEY ("metric_type_id", "source_node_id", "ts")
);

CREATE TABLE IF NOT EXISTS "node_operations" (
    "cluster" VARCHAR(255),
    "type" VARCHAR(255),
    "host" VARCHAR(255),
    "ts" TIMESTAMP WITH TIME ZONE,
    "data" TEXT,
    PRIMARY KEY ("cluster", "type", "host", "ts")
);
