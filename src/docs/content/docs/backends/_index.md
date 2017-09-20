+++
[menu.docs]
name = "Backends"
identifier = "backends"
weight = 10
+++

# Backends

Cassandra Reaper can be used with either an ephemeral memory storage or persistent database.

For persistent relational database storage, you must either setup PostgreSQL or H2. You also need to set `storageType: database` in the config file.

Sample yaml files are available in the `resource` directory for each storage backend:

* cassandra-reaper-memory.yaml
* cassandra-reaper-postgres.yaml
* cassandra-reaper-h2.yaml
* cassandra-reaper-cassandra.yaml

For configuring other aspects of the service, see the available configuration options in later section
of this page.
