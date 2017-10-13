--
-- Upgrade to handle the new segment count per node
--


ALTER TABLE "repair_schedule"
ADD "segment_count_per_node" INT NOT NULL DEFAULT 0;
