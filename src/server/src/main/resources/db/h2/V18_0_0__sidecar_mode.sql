CREATE TABLE IF NOT EXISTS node_metrics_v2_source_nodes (
  source_node_id SERIAL UNIQUE,
  cluster VARCHAR,
  host VARCHAR,
  last_updated TIMESTAMP WITH TIME ZONE,
  PRIMARY KEY (cluster, host)
);

CREATE TABLE IF NOT EXISTS node_metrics_v2_metric_types (
  metric_type_id SERIAL UNIQUE,
  metric_domain VARCHAR,
  metric_type VARCHAR,
  metric_scope VARCHAR,
  metric_name VARCHAR,
  metric_attribute VARCHAR,
  PRIMARY KEY (metric_domain, metric_type, metric_scope, metric_name, metric_attribute)
);

CREATE TABLE IF NOT EXISTS node_metrics_v2 (
  metric_type_id INT,
  source_node_id INT,
  ts TIMESTAMP WITH TIME ZONE,
  value DOUBLE PRECISION,
  PRIMARY KEY (metric_type_id, source_node_id, ts)
);

CREATE TABLE IF NOT EXISTS node_operations (
    cluster VARCHAR,
    type VARCHAR,
    host VARCHAR,
    ts TIMESTAMP WITH TIME ZONE,
    data VARCHAR,
    PRIMARY KEY (cluster, type, host, ts)
);
