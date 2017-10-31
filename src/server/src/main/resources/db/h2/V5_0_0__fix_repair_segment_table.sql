--
-- Fix inconsistencies in the repair_segment table
-- to comply with new safety policies on segments
--

UPDATE repair_segment
SET end_time = NULL
WHERE end_time IS NOT NULL
AND state != 2;

UPDATE repair_segment
SET start_time = end_time
WHERE start_time is NULL
AND   end_time IS NOT NULL;


