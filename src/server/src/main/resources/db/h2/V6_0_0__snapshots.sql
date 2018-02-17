--
-- Support for snapshots
--

CREATE TABLE IF NOT EXISTS snapshot (
  cluster                 VARCHAR,
  snapshot_name           VARCHAR,
  owner                   VARCHAR,
  cause                   VARCHAR,
  creation_time           TIMESTAMP NOT NULL,
  PRIMARY KEY(cluster, snapshot_name)
);

