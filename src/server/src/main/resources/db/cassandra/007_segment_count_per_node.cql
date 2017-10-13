--
-- Upgrade to handle the new segment count per node

ALTER TABLE repair_schedule_v1 ADD segment_count_per_node int;
