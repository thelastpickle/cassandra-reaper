CREATE TABLE IF NOT EXISTS "percent_repaired_by_schedule" (
  "cluster" TEXT,
  "repair_schedule_id" BIGINT,
  "node" TEXT,
  "keyspace_name" TEXT,
  "table_name" TEXT,
  "percent_repaired" INT,
  "ts" TIMESTAMP WITH TIME ZONE,
  PRIMARY KEY("cluster", "repair_schedule_id", "node")
); 