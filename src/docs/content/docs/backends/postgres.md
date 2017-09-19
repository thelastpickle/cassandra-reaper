
# Postgres Backend

The schema will be initialized/upgraded automatically upon startup in the configured database.
Make sure to specify the correct credentials in your JDBC settings in `cassandra-reaper.yaml` to allow objects creation.


```yaml
storageType: database
database:
  # PostgreSQL JDBC settings
  driverClass: org.postgresql.Driver
  user: postgres
  password: 
  url: jdbc:postgresql://127.0.0.1/reaper
```
