ALTER TABLE repair_unit
ADD timeout INT NOT NULL DEFAULT 0;

ALTER TABLE repair_schedule
ADD adaptive BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE repair_run
ADD adaptive_schedule BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE repair_schedule
DROP segment_count;