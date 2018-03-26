--
-- Upgrade to handle repair on specific nodes

use reaper_db;

ALTER TABLE repair_unit_v1 ADD blacklisted_tables set<text>;
