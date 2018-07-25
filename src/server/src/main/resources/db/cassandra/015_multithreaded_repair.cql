--
-- Upgrade to allow segments to contain several token ranges

ALTER TABLE repair_unit_v1 ADD repair_thread_count int;
