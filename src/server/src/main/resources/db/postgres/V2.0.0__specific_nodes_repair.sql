--
-- PostgreSQL schema addition for repair on specific nodes
--


ALTER TABLE "repair_unit" 
ADD "nodes" TEXT [];

ALTER TABLE "repair_unit" 
ADD "datacenters" TEXT [];
