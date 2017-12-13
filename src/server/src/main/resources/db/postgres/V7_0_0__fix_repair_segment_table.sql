--
-- fix segment start and end times in the repair_run table
--

UPDATE repair_segment
SET start_time = end_time
WHERE start_time is NULL
AND   end_time IS NOT NULL;


