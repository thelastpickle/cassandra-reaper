--
-- Upgrade to permit major compactions post repairs

ALTER TABLE repair_schedule_v1 ADD major_compaction boolean;
ALTER TABLE repair_run ADD major_compaction boolean static;
