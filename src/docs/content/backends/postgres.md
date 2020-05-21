+++
title = "Postgres Backend"
menuTitle = "Postgres"
parent = "backends"
weight = 3
+++

To use PostgreSQL as the persistent storage for Reaper, the `storageType` setting must be set to **postgres** in the Reaper configuration YAML file. The schema will be initialized/upgraded automatically upon startup in the configured database. Ensure that the correct JDBC credentials are specified in the *cassandra-reaper.yaml* to allow object creation. An example of how to configure Postgres as persistent storage for Reaper can be found in the *[cassandra-reaper-postgres.yaml](https://github.com/thelastpickle/cassandra-reaper/blob/master/src/packaging/resource/cassandra-reaper-postgres.yaml)*.


```yaml
storageType: postgres
postgres:
  # PostgreSQL JDBC settings
  user: postgres
  password: 
  url: jdbc:postgresql://127.0.0.1/reaper
```
