
-- Optimal table properties

use reaper_db;

ALTER TABLE repair_unit_v1
    WITH caching ='keys_only'
  AND dclocal_read_repair_chance = 0.0;

ALTER TABLE repair_schedule_by_cluster_and_keyspace
    WITH caching ='keys_only'
  AND dclocal_read_repair_chance = 0.0;

ALTER TABLE repair_run_by_cluster
    WITH caching ='all'
  AND dclocal_read_repair_chance = 0.0;

ALTER TABLE repair_schedule_v1
    WITH caching ='keys_only'
  AND dclocal_read_repair_chance = 0.0;

ALTER TABLE cluster
    WITH caching ='all'
  AND dclocal_read_repair_chance = 0.0;

ALTER TABLE repair_run
    WITH caching ='all'
  AND dclocal_read_repair_chance = 0.0;

ALTER TABLE repair_run_by_unit
    WITH caching ='all'
  AND dclocal_read_repair_chance = 0.0;
