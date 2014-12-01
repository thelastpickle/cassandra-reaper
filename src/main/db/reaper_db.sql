--
-- PostgreSQL schema for cassandra-reaper database
-- Assumes PostgreSQL version 9.3 or newer
--

CREATE TABLE IF NOT EXISTS "cluster" (
  "id" SERIAL PRIMARY KEY,
  "partitioner" TEXT NOT NULL,
  "name" TEXT NOT NULL,
  "seed_hosts" TEXT[] NOT NULL
);

CREATE TABLE IF NOT EXISTS "column_family" (
  "id" SERIAL PRIMARY KEY,
  "cluster_id" INT NOT NULL REFERENCES "cluster" ("id"),
  "keyspace_name" TEXT NOT NULL,
  "name" TEXT NOT NULL,
  "strategy" TEXT NOT NULL,
  "segment_count" INT NOT NULL,
  "snapshot_repair" BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS "repair_run" (
  "id" SERIAL PRIMARY KEY,
  "cause" TEXT NOT NULL,
  "owner" TEXT NOT NULL,
  "state" TEXT NOT NULL,
  "creation_time" TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  "start_time" TIMESTAMP WITH TIME ZONE DEFAULT NULL,
  "end_time" TIMESTAMP WITH TIME ZONE DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS "repair_segment" (
  "id" SERIAL PRIMARY KEY,
  "column_family_id" INT NOT NULL REFERENCES "column_family" ("id"),
  "run_id" INT NOT NULL REFERENCES "repair_run" ("id"),
  "start_token" BIGINT NOT NULL,
  "end_token" BIGINT NOT NULL,
  "state" SMALLINT NOT NULL,
  "start_time" TIMESTAMP WITH TIME ZONE DEFAULT NULL,
  "end_time" TIMESTAMP WITH TIME ZONE DEFAULT NULL
);
CREATE UNIQUE INDEX "repair_segment_run_id_idx" ON "repair_segment" USING BTREE ("run_id");
CREATE UNIQUE INDEX "repair_segment_state_idx" ON "repair_segment" USING BTREE ("state");
