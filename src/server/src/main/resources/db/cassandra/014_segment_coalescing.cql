--
-- Upgrade to allow segments to contain several token ranges

ALTER TABLE repair_run ADD token_ranges text;
