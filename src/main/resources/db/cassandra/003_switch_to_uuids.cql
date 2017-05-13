--
-- Cassandra schema for cassandra-reaper database

-- CREATE KEYSPACE IF NOT EXISTS reaper_db WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor':3};

-- use reaper_db;

ALTER TABLE cluster
  WITH compaction = {'class': 'LeveledCompactionStrategy'}
  AND caching = {'rows_per_partition': 'ALL'};

-- Repair unit is basically a keyspace with a set of column families.
-- Cassandra supports repairing multiple column families in one go.

CREATE TABLE IF NOT EXISTS repair_unit_v1 (
  id              timeuuid PRIMARY KEY,
  cluster_name    text,
  keyspace_name   text,
  column_families set<text>,
  incremental_repair boolean
)
  WITH compaction = {'class': 'LeveledCompactionStrategy'}
    AND caching = {'rows_per_partition': 1};

DROP TABLE IF EXISTS repair_run;

CREATE TABLE IF NOT EXISTS repair_run (
  id                 timeuuid,
  cluster_name       text      STATIC,
  repair_unit_id     timeuuid    STATIC,
  cause              text      STATIC,
  owner              text      STATIC,
  state              text      STATIC,
  creation_time      timestamp STATIC,
  start_time         timestamp STATIC,
  end_time           timestamp STATIC,
  pause_time         timestamp STATIC,
  intensity          double    STATIC,
  last_event         text      STATIC,
  segment_count      int       STATIC,
  repair_parallelism text      STATIC,
  segment_id         timeuuid,
  start_token        varint,
  end_token          varint,
  segment_state      int,
  coordinator_host   text,
  segment_start_time timestamp,
  segment_end_time   timestamp,
  fail_count         int,
  PRIMARY KEY (id, segment_id)
)
  WITH compaction = {'class': 'LeveledCompactionStrategy'}
  AND caching = {'rows_per_partition': 5000};


DROP TABLE IF EXISTS repair_run_by_cluster;

CREATE TABLE IF NOT EXISTS repair_run_by_cluster (
  cluster_name text,
  id           timeuuid,
  PRIMARY KEY(cluster_name, id)
)
  WITH compaction = {'class': 'LeveledCompactionStrategy'}
  AND caching = {'rows_per_partition': 'ALL'};


DROP TABLE IF EXISTS repair_run_by_unit;

CREATE TABLE IF NOT EXISTS repair_run_by_unit (
  repair_unit_id timeuuid,
  id             timeuuid,
  PRIMARY KEY(repair_unit_id, id)
)
  WITH compaction = {'class': 'LeveledCompactionStrategy'}
  AND caching = {'rows_per_partition': 'ALL'};




CREATE TABLE IF NOT EXISTS repair_schedule_v1 (
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
  AND caching = {'rows_per_partition': 1};


DROP TABLE IF EXISTS repair_schedule_by_cluster_and_keyspace;

CREATE TABLE IF NOT EXISTS repair_schedule_by_cluster_and_keyspace (
  cluster_name text,
  keyspace_name text,
  repair_schedule_id timeuuid,
  PRIMARY KEY((cluster_name, keyspace_name), repair_schedule_id)
)
  WITH compaction = {'class': 'LeveledCompactionStrategy'}
  AND caching = {'rows_per_partition': 10};


DROP TABLE IF EXISTS repair_segment;
DROP TABLE IF EXISTS repair_segment_by_run_id;
DROP TABLE IF EXISTS repair_id;