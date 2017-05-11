--
-- Cassandra schema for cassandra-reaper database

-- CREATE KEYSPACE IF NOT EXISTS reaper_db WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor':3};

-- use reaper_db;

CREATE TABLE IF NOT EXISTS cluster (
  name        text PRIMARY KEY,
  partitioner text,
  seed_hosts  set<text>
)
  WITH compaction = {'class': 'LeveledCompactionStrategy'}
  AND caching = {'rows_per_partition': 'ALL'};

-- Repair unit is basically a keyspace with a set of column families.
-- Cassandra supports repairing multiple column families in one go.

CREATE TABLE IF NOT EXISTS repair_unit (
  id              timeuuid PRIMARY KEY,
  cluster_name    text,
  keyspace_name   text,
  column_families set<text>,
  incremental_repair boolean
)
  WITH compaction = {'class': 'LeveledCompactionStrategy'}
  AND caching = {'rows_per_partition': 10};

CREATE TABLE IF NOT EXISTS repair_run (
  id                 timeuuid PRIMARY KEY,
  cluster_name       text,
  repair_unit_id     timeuuid,
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
)
  WITH compaction = {'class': 'LeveledCompactionStrategy'}
  AND caching = {'rows_per_partition': 10};

CREATE TABLE IF NOT EXISTS repair_run_by_cluster(
  cluster_name text,
  id           timeuuid,
  PRIMARY KEY(cluster_name, id)
)
  WITH compaction = {'class': 'LeveledCompactionStrategy'}
  AND caching = {'rows_per_partition': 10};

CREATE TABLE IF NOT EXISTS repair_run_by_unit(
  repair_unit_id timeuuid,
  id             timeuuid,
  PRIMARY KEY(repair_unit_id, id)
)
  WITH compaction = {'class': 'LeveledCompactionStrategy'}
  AND caching = {'rows_per_partition': 10};

CREATE TABLE IF NOT EXISTS repair_segment (
  id               timeuuid PRIMARY KEY,
  repair_unit_id   timeuuid,
  run_id           timeuuid,
  start_token      varint,
  end_token        varint,
  state            int   ,
  coordinator_host text,
  start_time       timestamp,
  end_time         timestamp,
  fail_count       INT
)
  WITH compaction = {'class': 'LeveledCompactionStrategy'}
  AND caching = {'rows_per_partition': 10};

CREATE TABLE IF NOT EXISTS repair_segment_by_run_id (
  run_id           timeuuid,
  segment_id       timeuuid,
  PRIMARY KEY(run_id, segment_id)
)
  WITH compaction = {'class': 'LeveledCompactionStrategy'}
  AND caching = {'rows_per_partition': 10};


CREATE TABLE IF NOT EXISTS repair_schedule (
  id                 timeuuid PRIMARY KEY,
  repair_unit_id     timeuuid,
  state              text                    ,
  days_between       int                ,
  next_activation    timestamp,
  run_history        set<timeuuid>,
  segment_count      int                     ,
  repair_parallelism text                    ,
  intensity          double                    ,
  creation_time      timestamp,
  owner              text                    ,
  pause_time         timestamp
)
  WITH compaction = {'class': 'LeveledCompactionStrategy'}
  AND caching = {'rows_per_partition': 10};


CREATE TABLE IF NOT EXISTS repair_schedule_by_cluster_and_keyspace(
  cluster_name text,
  keyspace_name text,
  repair_schedule_id timeuuid,
  PRIMARY KEY((cluster_name, keyspace_name), repair_schedule_id)
)
  WITH compaction = {'class': 'LeveledCompactionStrategy'}
  AND caching = {'rows_per_partition': 10};

