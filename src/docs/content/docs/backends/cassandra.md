+++
[menu.docs]
name = "Cassandra"
parent = "backends"
weight = 4
+++

# Cassandra Backend

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