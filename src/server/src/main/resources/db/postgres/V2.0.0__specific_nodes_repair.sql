--
-- PostgreSQL schema addition for repair on specific nodes
--


ALTER TABLE "repair_unit" 
ADD "nodes" TEXT [] NOT NULL;

ALTER TABLE "repair_unit" 
ADD "datacenters" TEXT [] NOT NULL;
