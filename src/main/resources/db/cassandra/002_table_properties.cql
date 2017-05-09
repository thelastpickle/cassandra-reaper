
-- Optimal table properties

ALTER TABLE cluster
    WITH compaction = {'class': 'LeveledCompactionStrategy'}
    AND caching = {'rows_per_partition': 'ALL'};

ALTER TABLE repair_unit
    WITH compaction = {'class': 'LeveledCompactionStrategy'}
    AND caching = {'rows_per_partition': 10};

ALTER TABLE repair_run
    WITH compaction = {'class': 'LeveledCompactionStrategy'}
    AND caching = {'rows_per_partition': 10};

ALTER TABLE repair_run_by_cluster
    WITH compaction = {'class': 'LeveledCompactionStrategy'}
    AND caching = {'rows_per_partition': 10};

ALTER TABLE repair_run_by_unit
    WITH compaction = {'class': 'LeveledCompactionStrategy'}
    AND caching = {'rows_per_partition': 10};

ALTER TABLE repair_segment
    WITH compaction = {'class': 'LeveledCompactionStrategy'};

ALTER TABLE repair_segment_by_run_id
    WITH compaction = {'class': 'LeveledCompactionStrategy'}
    AND caching = {'rows_per_partition': 10};

ALTER TABLE repair_schedule
    WITH compaction = {'class': 'LeveledCompactionStrategy'}
    AND caching = {'rows_per_partition': 10};

ALTER TABLE repair_schedule_by_cluster_and_keyspace
    WITH compaction = {'class': 'LeveledCompactionStrategy'}
    AND caching = {'rows_per_partition': 10};

ALTER TABLE repair_id
    WITH compaction = {'class': 'LeveledCompactionStrategy'}
    AND caching = {'rows_per_partition': 10};
