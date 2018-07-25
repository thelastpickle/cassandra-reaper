--
-- Support for snapshots
--

CREATE TABLE IF NOT EXISTS "snapshot" (
  "cluster"                 text,
  "snapshot_name"           text,
  "owner"                   text,
  "cause"                   text,
  "creation_time"           TIMESTAMP WITH TIME ZONE NOT NULL,
  PRIMARY KEY("cluster","snapshot_name")
);


