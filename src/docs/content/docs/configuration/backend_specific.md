+++
[menu.docs]
name = "Backend Specific Settings"
parent = "configuration"
weight = 6
+++

# Backend Specific Settings

Configuration settings in the *cassandra-reaper.yaml* that are specific to a particular backend.

## Cassandra Settings

The following settings are specific to a Reaper deployment that is backed by an Apache Cassandra database.
Note that Cassandra backend configuration relies on the [Dropwizard-Cassandra](https://github.com/composable-systems/dropwizard-cassandra) module.

</br>

### `activateQueryLogger`

Type: *Boolean*

Default: *false*

Records the CQL calls made to the Cassandra backend in the log output.

</br>

### `cassandra`

Settings to configure Reaper to use Cassandra for storage of its control data. Reaper uses the Cassandra Java driver version [3.1.4](http://docs.datastax.com/en/developer/java-driver/3.1/) to perform operations on the cluster. An example of the configuration settings for the driver are as follows.

```yaml
cassandra:
  clusterName: "test"
  contactPoints: ["127.0.0.1"]
  port: 9042
  keyspace: reaper_db
  loadBalancingPolicy:
    type: tokenAware
    shuffleReplicas: true
    subPolicy:
      type: dcAwareRoundRobin
      localDC:
      usedHostsPerRemoteDC: 0
      allowRemoteDCsForLocalConsistencyLevel: false
  authProvider:
    type: plainText
    username: cassandra
    password: cassandra
```



Definitions for some of the above sub-settings are as follows.

#### `clusterName`

Type: *String*

Name of the cluster to use to store the Reaper control data.

#### `contactPoints`

Type: *Array* (comma separated *Strings*)

Seed nodes in the Cassandra cluster to contact.

```yaml
["127.0.0.1", "127.0.0.2", "127.0.0.3"]
```

#### `port`

Type: *Integer*

Default: *9042*

Cassandra's native port to connect to.

#### `keyspace`

Type: *String*

Name of the keyspace to store the Reaper control data.

#### `loadBalancingPolicy`

Settings to configure the policies used to generate the query plan which determines the nodes to connect to when performing query operations. Further information can be found in the Cassandra Java driver [Load balancing](http://docs.datastax.com/en/developer/java-driver/3.1/manual/load_balancing/) section.

#### `subPolicy`

Settings to configure a child policy which used if the initial policy fails to determine a node to connect to.

#### `type`

Type: *String*

The policy type used to contribute to the computation of the query plan.

#### `localDC`

Type: *String*

Specifies the name of the datacenter closest to Reaper when using the `dcAwareRoundRobin` policy.

#### `authProvider`

If native protocol authentication is enabled on Cassandra,  settings configure Reaper to pass credentials to Cassandra when establishing a connection.

#### `username`

Type: *String*

Cassandra native protocol username.

#### `password`

Type: *String*

Cassandra native protocol password.

#### Full Configuration Reference

```
clusterName:
keyspace:
validationQuery:
healthCheckTimeout:
contactPoints:
port:
protocolVersion:
compression:
maxSchemaAgreementWait:
ssl:
  type:
addressTranslator:
  type:
reconnectionPolicy:
  type:
authProvider:
  type:
retryPolicy:
  type:
loadBalancingPolicy:
  type:
speculativeExecutionPolicy:
  type:
queryOptions:
  consistencyLevel:
  serialConsistencyLevel:
  fetchSize:
socketOptions:
  connectTimeoutMillis:
  readTimeoutMillis:
  keepAlive:
  reuseAddress:
  soLinger:
  tcpNoDelay:
  receiveBufferSize:
  sendBufferSize:
poolingOptions:
  heartbeatInterval:
  poolTimeout:
  local:
    maxRequestsPerConnection:
    newConnectionThreshold:
    coreConnections:
    maxConnections:
  remote:
    maxRequestsPerConnection:
    newConnectionThreshold:
    coreConnections:
    maxConnections:
metricsEnabled:
jmxEnabled:
shutdownGracePeriod:


## H2 or Postgres Database Settings

**Removed in v3.0.0**

The following settings are specific to a Reaper deployment that is backed by either a H2 or Postgres database. An example of the configuration settings for a Postgres database are as follows.

```yaml
postgres:
  url: jdbc:postgresql://127.0.0.1/reaper
  user: postgres
  password:
```

Definitions for the above sub-settings are as follows.

</br>

### `h2`

Settings to configure Reaper to use H2 for storage of its control data.

### `postgres`

Settings to configure Reaper to use Postgres for storage of its control data.

#### `driverClass`

Type: *String*

**WARNING** this setting is **DEPRECATED** and its usage should be avoided.

Specifies the driver to use to connect to the database.

#### `url`

Type: *String*

Specifies the URL to connect to the database (either H2 or Postgres) on.

#### `user`

Type: *String*

Database username.

#### `password`

Type: *String*

Database password.
