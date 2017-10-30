
-- Optimal table properties

ALTER TABLE repair_unit_v1
  WITH caching = {'keys':'ALL', 'rows_per_partition':'1'}
  AND dclocal_read_repair_chance = 0.0;

ALTER TABLE repair_schedule_by_cluster_and_keyspace
  WITH caching = {'keys':'ALL', 'rows_per_partition':'10'}
  AND dclocal_read_repair_chance = 0.0;

ALTER TABLE repair_run_by_cluster
  WITH caching = {'keys':'ALL', 'rows_per_partition':'ALL'}
  AND dclocal_read_repair_chance = 0.0;

ALTER TABLE repair_schedule_v1
  WITH caching = {'keys':'ALL', 'rows_per_partition':'1'}
  AND dclocal_read_repair_chance = 0.0;

ALTER TABLE cluster
  WITH caching = {'keys':'ALL', 'rows_per_partition':'ALL'}
  AND dclocal_read_repair_chance = 0.0;

ALTER TABLE repair_run
  WITH caching = {'keys':'ALL', 'rows_per_partition':'5000'}
  AND dclocal_read_repair_chance = 0.0;

ALTER TABLE repair_run_by_unit
  WITH caching = {'keys':'ALL', 'rows_per_partition':'ALL'}
  AND dclocal_read_repair_chance = 0.0;
