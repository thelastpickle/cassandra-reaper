--
-- Upgrade to handle repair on specific nodes

use reaper_db;

ALTER TABLE repair_unit_v1 ADD nodes set<text>;

ALTER TABLE repair_unit_v1 ADD datacenters set<text>;
