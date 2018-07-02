--
-- Upgrade to handle the new segment count per node
--

ALTER TABLE repair_schedule ADD major_compaction BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE repair_run ADD major_compaction BOOLEAN NOT NULL DEFAULT false;