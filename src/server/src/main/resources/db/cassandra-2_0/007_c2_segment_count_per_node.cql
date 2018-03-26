--
-- Upgrade to handle the new segment count per node

use reaper_db;

ALTER TABLE repair_schedule_v1 ADD segment_count_per_node int;
