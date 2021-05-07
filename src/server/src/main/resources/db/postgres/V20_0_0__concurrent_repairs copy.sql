CREATE TABLE IF NOT EXISTS "running_repairs" (
  "repair_id" BIGINT,
  "node" TEXT,
  "reaper_instance_host" TEXT,
  "reaper_instance_id" BIGINT,
  "segment_id" BIGINT,
  "last_heartbeat" TIMESTAMP WITH TIME ZONE,
  PRIMARY KEY("repair_id", "node")
); 

