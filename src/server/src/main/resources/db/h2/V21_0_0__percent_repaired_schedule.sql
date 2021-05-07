CREATE TABLE IF NOT EXISTS percent_repaired_by_schedule (
  cluster VARCHAR,
  repair_schedule_id BIGINT,
  node VARCHAR,
  keyspace_name VARCHAR,
  table_name VARCHAR,
  percent_repaired INT,
  ts TIMESTAMP WITH TIME ZONE,
  PRIMARY KEY(cluster, repair_schedule_id, node)
);