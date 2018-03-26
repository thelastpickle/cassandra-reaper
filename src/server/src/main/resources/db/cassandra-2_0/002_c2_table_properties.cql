
-- Optimal table properties
use reaper_db;

ALTER TABLE cluster
    WITH compaction = {'class': 'LeveledCompactionStrategy'}
    AND caching ='all';

ALTER TABLE repair_unit
    WITH compaction = {'class': 'LeveledCompactionStrategy'}
    AND caching ='keys_only';

ALTER TABLE repair_run
    WITH compaction = {'class': 'LeveledCompactionStrategy'}
    AND caching ='keys_only';

ALTER TABLE repair_run_by_cluster
    WITH compaction = {'class': 'LeveledCompactionStrategy'}
    AND caching ='keys_only';

ALTER TABLE repair_run_by_unit
    WITH compaction = {'class': 'LeveledCompactionStrategy'}
    AND caching ='keys_only';

ALTER TABLE repair_segment
    WITH compaction = {'class': 'LeveledCompactionStrategy'};

ALTER TABLE repair_segment_by_run_id
    WITH compaction = {'class': 'LeveledCompactionStrategy'}
    AND caching ='keys_only';

ALTER TABLE repair_schedule
    WITH compaction = {'class': 'LeveledCompactionStrategy'}
    AND caching ='keys_only';

ALTER TABLE repair_schedule_by_cluster_and_keyspace
    WITH compaction = {'class': 'LeveledCompactionStrategy'}
    AND caching ='keys_only';

ALTER TABLE repair_id
    WITH compaction = {'class': 'LeveledCompactionStrategy'}
    AND caching ='keys_only';
