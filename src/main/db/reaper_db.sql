--
-- PostgreSQL schema for cassandra-reaper database
-- Assumes PostgreSQL version 9.3 or newer
--

-- CREATE DATABASE reaper_db;
-- CREATE USER reaper WITH PASSWORD 'my_secret_password';
-- GRANT ALL PRIVILEGES ON DATABASE reaper_db TO reaper;

CREATE TABLE IF NOT EXISTS "cluster" (
  "name" TEXT PRIMARY KEY,
  "partitioner" TEXT NOT NULL,
  "seed_hosts" TEXT[] NOT NULL
);

CREATE TABLE IF NOT EXISTS "column_family" (
  "id" SERIAL PRIMARY KEY,
  "cluster_name" TEXT NOT NULL REFERENCES "cluster" ("name"),
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

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE cluster TO reaper;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE column_family TO reaper;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE repair_run TO reaper;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE repair_segment TO reaper;
