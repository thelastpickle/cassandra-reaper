--
--  Copyright 2017-2017 Spotify AB
--
--  Licensed under the Apache License, Version 2.0 (the "License");
--  you may not use this file except in compliance with the License.
--  You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
--  Unless required by applicable law or agreed to in writing, software
--  distributed under the License is distributed on an "AS IS" BASIS,
--  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--  See the License for the specific language governing permissions and
--  limitations under the License.
--
-- H2 schema for cassandra-reaper database
--

-- For cleaning up the database, just do first in the following order:
-- DROP TABLE repair_segment;
-- DROP TABLE repair_run;
-- DROP TABLE repair_schedule;
-- DROP TABLE repair_unit;
-- DROP TABLE cluster;

CREATE TABLE IF NOT EXISTS cluster (
  name        VARCHAR PRIMARY KEY,
  partitioner VARCHAR    NOT NULL,
  seed_hosts  ARRAY NOT NULL
);

-- Repair unit is basically a keyspace with a set of column families.
-- Cassandra supports repairing multiple column families in one go.
--
CREATE TABLE IF NOT EXISTS repair_unit (
  id              SERIAL PRIMARY KEY,
  cluster_name    VARCHAR    NOT NULL REFERENCES cluster (name),
  keyspace_name   VARCHAR    NOT NULL,
  column_families ARRAY NOT NULL,
  incremental_repair BOOLEAN    NOT NULL
);

CREATE INDEX IF NOT EXISTS repair_unit_column_families_gin_idx ON repair_unit (column_families);


CREATE TABLE IF NOT EXISTS repair_run (
  id                 SERIAL PRIMARY KEY,
  cluster_name       VARCHAR                     NOT NULL REFERENCES cluster (name),
  repair_unit_id     INT                      NOT NULL REFERENCES repair_unit (id),
  cause              VARCHAR                     NOT NULL,
  owner              VARCHAR                     NOT NULL,
-- see (Java) RepairRun.RunState for state values
  state              VARCHAR                     NOT NULL,
  creation_time      TIMESTAMP NOT NULL,
  start_time         TIMESTAMP DEFAULT NULL,
  end_time           TIMESTAMP DEFAULT NULL,
  pause_time         TIMESTAMP DEFAULT NULL,
  intensity          REAL                     NOT NULL,
  last_event         VARCHAR                     NOT NULL,
  segment_count      INT                      NOT NULL,
  repair_parallelism VARCHAR                     NOT NULL
);

CREATE TABLE IF NOT EXISTS repair_segment (
  id               SERIAL PRIMARY KEY,
  repair_unit_id   INT         NOT NULL REFERENCES repair_unit (id),
  run_id           INT         NOT NULL REFERENCES repair_run (id),
  start_token      NUMERIC(50) NOT NULL,
  end_token        NUMERIC(50) NOT NULL,
-- see (Java) RepairSegment.State for state values
  state            SMALLINT    NOT NULL,
  coordinator_host VARCHAR                     DEFAULT NULL,
  start_time       TIMESTAMP DEFAULT NULL,
  end_time         TIMESTAMP DEFAULT NULL,
  fail_count       INT         NOT NULL     DEFAULT 0
);

CREATE INDEX IF NOT EXISTS repair_segment_run_id_fail_count_idx ON repair_segment (run_id ASC, fail_count ASC);

CREATE INDEX IF NOT EXISTS repair_segment_state_idx ON repair_segment (state);

CREATE TABLE IF NOT EXISTS repair_schedule (
  id                 SERIAL PRIMARY KEY,
  repair_unit_id     INT                      NOT NULL REFERENCES repair_unit (id),
-- see (Java) RepairSchedule.State for state values
  state              VARCHAR                     NOT NULL,
  days_between       SMALLINT                 NOT NULL,
  next_activation    TIMESTAMP NOT NULL,
-- run_history contains repair run ids, with latest scheduled run in the end
  run_history        ARRAY                   NOT NULL,
  segment_count      INT                      NOT NULL,
  repair_parallelism VARCHAR                     NOT NULL,
  intensity          REAL                     NOT NULL,
  creation_time      TIMESTAMP NOT NULL,
  owner              VARCHAR                     NOT NULL,
  pause_time         TIMESTAMP DEFAULT NULL
);

