CREATE TABLE IF NOT EXISTS "node_metrics_v2_source_nodes" (
  "source_node_id" SERIAL UNIQUE,
  "cluster" TEXT,
  "host" TEXT,
  "last_updated" TIMESTAMP WITH TIME ZONE,
  PRIMARY KEY ("cluster", "host")
);

CREATE TABLE IF NOT EXISTS "node_metrics_v2_metric_types" (
  "metric_type_id" SERIAL UNIQUE,
  "metric_domain" TEXT,
  "metric_type" TEXT,
  "metric_scope" TEXT,
  "metric_name" TEXT,
  "metric_attribute" TEXT,
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
    "cluster" TEXT,
    "type" TEXT,
    "host" TEXT,
    "ts" TIMESTAMP WITH TIME ZONE,
    "data" TEXT,
    PRIMARY KEY ("cluster", "type", "host", "ts")
);
