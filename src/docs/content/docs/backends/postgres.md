+++
[menu.docs]
name = "Postgres"
parent = "backends"
weight = 3
+++

# Postgres Backend

To use PostgreSQL as the persistent storage for Reaper, the `storageType` setting must be set to **database** in the Reaper configuration YAML file. The schema will be initialized/upgraded automatically upon startup in the configured database. Ensure that the correct JDBC credentials are specified in the `cassandra-reaper.yaml` to allow object creation.


```yaml
storageType: database
database:
  # PostgreSQL JDBC settings
  driverClass: org.postgresql.Driver
  user: postgres
  password: 
  url: jdbc:postgresql://127.0.0.1/reaper
```
