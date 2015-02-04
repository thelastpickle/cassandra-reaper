--
-- PostgreSQL schema for cassandra-reaper database
-- Assumes PostgreSQL version 9.3 or newer
--

-- CREATE DATABASE reaper_db;
-- CREATE USER reaper WITH PASSWORD 'my_secret_password';
-- GRANT ALL PRIVILEGES ON DATABASE reaper_db TO reaper;

-- For cleaning up the database, just do first in the following order:
-- DROP TABLE "repair_segment";
-- DROP TABLE "repair_run";
-- DROP TABLE "repair_unit";
-- DROP TABLE "cluster";

CREATE TABLE IF NOT EXISTS "cluster" (
  "name"        TEXT PRIMARY KEY,
  "partitioner" TEXT    NOT NULL,
  "seed_hosts"  TEXT [] NOT NULL
);

-- Repair unit is basically a keyspace with a set of column families.
-- Cassandra supports repairing multiple column families in one go.
--
CREATE TABLE IF NOT EXISTS "repair_unit" (
  "id"              SERIAL PRIMARY KEY,
  "cluster_name"    TEXT    NOT NULL REFERENCES "cluster" ("name"),
  "keyspace_name"   TEXT    NOT NULL,
  "column_families" TEXT [] NOT NULL
);
-- Using GIN index to make @> (contains) type of array operations faster
CREATE INDEX repair_unit_column_families_gin_idx ON repair_unit USING GIN (column_families);

CREATE TABLE IF NOT EXISTS "repair_run" (
  "id"                 SERIAL PRIMARY KEY,
  "cluster_name"       TEXT NOT NULL REFERENCES "cluster" ("name"),
  "repair_unit_id"     INT  NOT NULL REFERENCES "repair_unit" ("id"),
  "cause"              TEXT NOT NULL,
  "owner"              TEXT NOT NULL,
-- see (Java) RepairRun.RunState for state values
  "state"              TEXT NOT NULL,
  "creation_time"      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  "start_time"         TIMESTAMP WITH TIME ZONE DEFAULT NULL,
  "end_time"           TIMESTAMP WITH TIME ZONE DEFAULT NULL,
  "pause_time"         TIMESTAMP WITH TIME ZONE DEFAULT NULL,
  "intensity"          REAL NOT NULL,
  "last_event"         TEXT NOT NULL,
  "segment_count"      INT  NOT NULL,
  "repair_parallelism" TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS "repair_segment" (
  "id"               SERIAL PRIMARY KEY,
  "repair_unit_id"   INT         NOT NULL REFERENCES "repair_unit" ("id"),
  "run_id"           INT         NOT NULL REFERENCES "repair_run" ("id"),
  "start_token"      NUMERIC(50) NOT NULL,
  "end_token"        NUMERIC(50) NOT NULL,
-- see (Java) RepairSegment.State for state values
  "state"            SMALLINT    NOT NULL,
  "coordinator_host" TEXT                     DEFAULT NULL,
  "start_time"       TIMESTAMP WITH TIME ZONE DEFAULT NULL,
  "end_time"         TIMESTAMP WITH TIME ZONE DEFAULT NULL,
  "fail_count"       INT         NOT NULL     DEFAULT 0
);
CREATE INDEX "repair_segment_run_id_fail_count_idx" ON "repair_segment" USING BTREE ("run_id" ASC, "fail_count" ASC);
CREATE INDEX "repair_segment_state_idx" ON "repair_segment" USING BTREE ("state");

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE cluster TO reaper;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE repair_unit TO reaper;
GRANT USAGE, SELECT ON SEQUENCE repair_unit_id_seq TO reaper;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE repair_run TO reaper;
GRANT USAGE, SELECT ON SEQUENCE repair_run_id_seq TO reaper;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE repair_segment TO reaper;
GRANT USAGE, SELECT ON SEQUENCE repair_segment_id_seq TO reaper;
