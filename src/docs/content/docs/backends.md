+++
menu: "backends"
+++


### Backends

Cassandra Reaper can be used with either an ephemeral memory storage or persistent database. 

For persistent relational database storage, you must either setup PostgreSQL or H2. You also need to set `storageType: database` in the config file.

Sample yaml files are available in the `resource` directory for each storage backend:  
* cassandra-reaper-memory.yaml
* cassandra-reaper-postgres.yaml
* cassandra-reaper-h2.yaml
* cassandra-reaper-cassandra.yaml

For configuring other aspects of the service, see the available configuration options in later section
of this page.

#### Memory Backend

Running Reaper with memory storage, which is not persistent, means that all
the registered clusters, column families, and repair runs will be lost upon service restart.
The memory based storage is meant to be used for testing purposes only. Enable this type of storage by using the `storageType: memory` setting in your config file (enabled by default).

```yaml
storageType: memory
```


#### H2 Backend

When using H2 storage the database will automatically created for you under the path configured in `cassandra-reaper.yaml`. Please
comment/uncomment the H2 settings and modify the path as needed or use the `cassandra-reaper-h2.yaml` as a base.

```yaml
storageType: database
database:
  # H2 JDBC settings
  driverClass: org.h2.Driver
  url: jdbc:h2:~/reaper-db/db;MODE=PostgreSQL
  user:
  password:
  
```

#### Postgres Backend

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

#### Cassandra Backend

For persistent Apache Cassandra storage, you need to set `storageType: cassandra` in the config file.
You'll also need to fill in the connection details to your Apache Cassandra cluster used to store the Reaper schema (reaper_db by default), in the `cassandra: ` section of the yaml file. 

```yaml
storageType: cassandra
cassandra:
  clusterName: "test"
  contactPoints: ["127.0.0.1"]
  keyspace: reaper_db
  queryOptions:
    consistencyLevel: LOCAL_QUORUM
    serialConsistencyLevel: SERIAL
```

When using SSL:

```yaml
cassandra:
  storageType: cassandra
  clusterName: "test"
  contactPoints: ["127.0.0.1"]
  keyspace: reaper_db
  authProvider:
    type: plainText
    username: cassandra
    password: cassandra
  ssl:
    type: jdk
```

The Apache Cassandra backend is the only one that allows running several Reaper instances at once. This provides high availability and allows to repair multi DC clusters (see the **Multi-DC** section below).

You will need to create a keyspace to store your data.  This is installation specific, you will need to fill in your own datacenter names.  For example:

```none
CREATE KEYSPACE reaper_db WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 3};
```