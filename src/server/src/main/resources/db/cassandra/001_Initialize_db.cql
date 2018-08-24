--
--  Copyright 2016-2017 Spotify AB
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
-- Cassandra schema for cassandra-reaper database

-- CREATE KEYSPACE IF NOT EXISTS reaper_db WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor':3};

-- use reaper_db;

CREATE TABLE IF NOT EXISTS cluster (
  name        text PRIMARY KEY,
  partitioner text,
  seed_hosts  set<text>
);

-- Repair unit is basically a keyspace with a set of column families.
-- Cassandra supports repairing multiple column families in one go.

CREATE TABLE IF NOT EXISTS repair_unit (
  id              bigint PRIMARY KEY,
  cluster_name    text,
  keyspace_name   text,
  column_families set<text>,
  incremental_repair boolean
);

CREATE TABLE IF NOT EXISTS repair_run (
  id                 bigint PRIMARY KEY,
  cluster_name       text,
  repair_unit_id     bigint,
  cause              text,
  owner              text,
  state              text,
  creation_time      timestamp,
  start_time         timestamp,
  end_time           timestamp,
  pause_time         timestamp,
  intensity          double                    ,
  last_event         text                    ,
  segment_count      int                     ,
  repair_parallelism text
);

CREATE TABLE IF NOT EXISTS repair_run_by_cluster(
  cluster_name text,
  id bigint,
  PRIMARY KEY(cluster_name, id)
);

CREATE TABLE IF NOT EXISTS repair_run_by_unit(
  repair_unit_id bigint,
  id bigint,
  PRIMARY KEY(repair_unit_id, id)
);

CREATE TABLE IF NOT EXISTS repair_segment (
  id               bigint PRIMARY KEY,
  repair_unit_id   bigint,
  run_id           bigint,
  start_token      varint,
  end_token        varint,
  state            int   ,
  coordinator_host text,
  start_time       timestamp,
  end_time         timestamp,
  fail_count       INT
);

CREATE TABLE IF NOT EXISTS repair_segment_by_run_id (
  run_id           bigint,
  segment_id       bigint,
  PRIMARY KEY(run_id, segment_id)
);


CREATE TABLE IF NOT EXISTS repair_schedule (
  id                 bigint PRIMARY KEY,
  repair_unit_id     bigint,
  state              text                    ,
  days_between       int                ,
  next_activation    timestamp,
  run_history        set<bigint>,
  segment_count      int                     ,
  repair_parallelism text                    ,
  intensity          double                    ,
  creation_time      timestamp,
  owner              text                    ,
  pause_time         timestamp
);


CREATE TABLE IF NOT EXISTS repair_schedule_by_cluster_and_keyspace(
  cluster_name text,
  keyspace_name text,
  repair_schedule_id bigint,
  PRIMARY KEY((cluster_name, keyspace_name), repair_schedule_id)
);

CREATE TABLE IF NOT EXISTS repair_id(
  id_type text PRIMARY KEY,
  id bigint
);
