--
-- H2 schema addition for repair on specific nodes
--

ALTER TABLE repair_unit 
ADD nodes ARRAY NOT NULL;

ALTER TABLE repair_unit 
ADD datacenters ARRAY NOT NULL;
