--
-- Upgrade to handle repair on specific nodes

ALTER TABLE repair_unit_v1 ADD blacklisted_tables set<text>;
