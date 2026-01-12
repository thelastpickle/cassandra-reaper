---
title: "Docker Variables"
parent: "configuration"
weight: 10
---

The Reaper Docker container has been designed to be highly configurable. Many of the environment variables map directly or indirectly to a settings in the *cassandra-reaper.yaml* configuration file.

## Direct Mapping to Reaper Specific Configuration Settings

The Docker environment variables listed in this section map directly to Reaper specific settings in the *cassandra-reaper.yaml* configuration file. The following table below lists the Docker environment variables, their associated Reaper specific setting in the *cassandra-reaper.yaml* configuration file, and the default value assigned by the Docker container (if any). Definitions for each Docker environment variable can be found via the link to the associated setting.

| Environment Variable | Configuration Setting | Default Value |
|---|---|---|
| `REAPER_AUTO_SCHEDULING_ENABLED` | [enabled]({{< relref "reaper_specific.md#enabled" >}}) | false |
| `REAPER_AUTO_SCHEDULING_EXCLUDED_KEYSPACES` | [excludedKeyspaces]({{< relref "reaper_specific.md#excludedkeyspaces" >}}) | [] |
| `REAPER_AUTO_SCHEDULING_EXCLUDED_CLUSTERS` | [excludedClusters]({{< relref "reaper_specific.md#excludedclusters" >}}) | [] |
| `REAPER_AUTO_SCHEDULING_INITIAL_DELAY_PERIOD` | [initialDelayPeriod]({{< relref "reaper_specific.md#initialdelayperiod" >}}) | PT15S |
| `REAPER_AUTO_SCHEDULING_PERIOD_BETWEEN_POLLS` | [periodBetweenPolls]({{< relref "reaper_specific.md#periodbetweenpolls" >}}) | PT10M |
| `REAPER_AUTO_SCHEDULING_SCHEDULE_SPREAD_PERIOD` | [scheduleSpreadPeriod]({{< relref "reaper_specific.md#schedulespreadperiod" >}}) | PT6H |
| `REAPER_AUTO_SCHEDULING_TIME_BEFORE_FIRST_SCHEDULE` | [timeBeforeFirstSchedule]({{< relref "reaper_specific.md#timebeforefirstschedule" >}}) | PT5M |
| `REAPER_AUTO_SCHEDULING_ADAPTIVE` | [adaptive]({{< relref "reaper_specific.md#adaptive" >}}) | true |
| `REAPER_AUTO_SCHEDULING_INCREMENTAL` | [incremental]({{< relref "reaper_specific.md#incremental" >}}) | false |
| `REAPER_AUTO_SCHEDULING_PERCENT_UNREPAIRED_THRESHOLD` | [percentUnrepairedThreshold]({{< relref "reaper_specific.md#percentunrepairedthreshold" >}}) | 10 |
| `REAPER_BLACKLIST_TWCS` | [blacklistTwcsTables]({{< relref "reaper_specific.md#blacklisttwcstables" >}}) | false |
| `REAPER_DATACENTER_AVAILABILITY` | [datacenterAvailability]({{< relref "reaper_specific.md#datacenteravailability" >}}) | ALL |
| `REAPER_ENABLE_CROSS_ORIGIN` | [enableCrossOrigin]({{< relref "reaper_specific.md#enablecrossorigin" >}}) | true |
| `REAPER_ENABLE_DYNAMIC_SEED_LIST` | [enableDynamicSeedList]({{< relref "reaper_specific.md#enabledynamicseedlist" >}}) | true |
| `REAPER_HANGING_REPAIR_TIMEOUT_MINS` | [hangingRepairTimeoutMins]({{< relref "reaper_specific.md#hangingrepairtimeoutmins" >}}) | 30 |
| `REAPER_INCREMENTAL_REPAIR` | [incrementalRepair]({{< relref "reaper_specific.md#incrementalrepair" >}}) | false |
| `REAPER_SUBRANGE_INCREMENTAL` | [subrangeIncrementalRepair]({{< relref "reaper_specific.md#subrangeincremental" >}}) | false |
| `REAPER_JMX_AUTH_PASSWORD` | [password]({{< relref "reaper_specific.md#password" >}}) | |
| `REAPER_JMX_AUTH_USERNAME` | [username]({{< relref "reaper_specific.md#username" >}}) | |
| `REAPER_JMX_CREDENTIALS` | [jmxCredentials]({{< relref "reaper_specific.md#jmxcredentials" >}}) | |
| `REAPER_JMX_CONNECTION_TIMEOUT_IN_SECONDS` | [jmxConnectionTimeoutInSeconds]({{< relref "reaper_specific.md#jmxconnectiontimeoutinseconds" >}}) | 20 |
| `REAPER_JMX_PORTS` | [jmxPorts]({{< relref "reaper_specific.md#jmxports" >}}) | {} |
| `REAPER_LOGGING_APPENDERS_CONSOLE_LOG_FORMAT` | [logFormat]({{< relref "reaper_specific.md#logformat" >}}) | "%-6level [%d] [%t] %logger{5} - %msg %n" |
| `REAPER_LOGGING_APPENDERS_CONSOLE_THRESHOLD` | [threshold]({{< relref "reaper_specific.md#threshold" >}}) | INFO |
| `REAPER_LOGGING_LOGGERS` | [loggers]({{< relref "reaper_specific.md#loggers" >}}) | {} |
| `REAPER_LOGGING_ROOT_LEVEL` | [level]({{< relref "reaper_specific.md#level" >}}) | INFO |
| `REAPER_MAX_PENDING_COMPACTIONS` | [maxPendingCompactions]({{< relref "reaper_specific.md#maxpendingcompactions" >}}) | 20 |
| `REAPER_METRICS_FREQUENCY` | [fequency]({{< relref "reaper_specific.md#fequency" >}}) | 1 minute |
| `REAPER_METRICS_REPORTERS` | [reporters]({{< relref "reaper_specific.md#reporters" >}}) | [] |
| `REAPER_REPAIR_INTENSITY` | [repairIntensity]({{< relref "reaper_specific.md#repairintensity" >}}) | 0.9 |
| `REAPER_REPAIR_MANAGER_SCHEDULING_INTERVAL_SECONDS` | [repairManagerSchedulingIntervalSeconds]({{< relref "reaper_specific.md#repairmanagerschedulingintervalseconds" >}}) | 30 |
| `REAPER_REPAIR_PARALELLISM` | [repairParallelism]({{< relref "reaper_specific.md#repairparallelism" >}}) | DATACENTER_AWARE |
| `REAPER_REPAIR_RUN_THREADS` | [repairRunThreadCount]({{< relref "reaper_specific.md#repairrunthreadcount" >}}) | 15 |
| `REAPER_SCHEDULE_DAYS_BETWEEN` | [scheduleDaysBetween]({{< relref "reaper_specific.md#scheduledaysbetween" >}}) | 7 |
| `REAPER_SEGMENT_COUNT_PER_NODE` | [segmentCountPerNode]({{< relref "reaper_specific.md#segmentcount" >}}) | 64 |
| `REAPER_SERVER_ADMIN_BIND_HOST` | [bindHost]({{< relref "reaper_specific.md#bindhost" >}}) | 0.0.0.0 |
| `REAPER_SERVER_ADMIN_PORT` | [port]({{< relref "reaper_specific.md#port" >}}) | 8081 |
| `REAPER_SERVER_APP_BIND_HOST` | [bindHost]({{< relref "reaper_specific.md#bindhost" >}}) | 0.0.0.0 |
| `REAPER_SERVER_APP_PORT` | [port]({{< relref "reaper_specific.md#port" >}}) | 8080 |
| `REAPER_SERVER_TLS_ENABLE` | [server/applicationConnectors/type]({{< relref "reaper_specific.md#server" >}}) | |
| `REAPER_SERVER_TLS_KEYSTORE_PATH` | [server/applicationConnectors/keyStorePath]({{< relref "reaper_specific.md#server" >}}) | |
| `REAPER_SERVER_TLS_TRUSTSTORE_PATH` | [server/applicationConnectors/trustStorePath]({{< relref "reaper_specific.md#server" >}}) | |
| `REAPER_SERVER_TLS_CLIENT_AUTH` | [server/applicationConnectors/needClientAuth]({{< relref "reaper_specific.md#server" >}}) | |
| `REAPER_SERVER_TLS_DISABLE_SNI` | [server/applicationConnectors/disableSniHostCheck]({{< relref "reaper_specific.md#server" >}}) | |
| `REAPER_STORAGE_TYPE` | [storageType]({{< relref "reaper_specific.md#storagetype" >}}) | memory |
| `REAPER_USE_ADDRESS_TRANSLATOR` | [useAddressTranslator]({{< relref "reaper_specific.md#useaddresstranslator" >}}) | false |
| `REAPER_MAX_PARALLEL_REPAIRS` | [maxParallelRepairs]({{< relref "reaper_specific.md#maxParallelRepairs" >}}) | 2 |
| `CRYPTO_SYSTEM_PROPERTY_SECRET` | [cryptograph/systemPropertySecret]({{< relref "reaper_specific.md#cryptograph" >}}) | Unset |
| `REAPER_HTTP_MANAGEMENT_ENABLE` | [httpManagement/enabled]({{< relref "reaper_specific.md#httpManagement" >}}) | false |
| `REAPER_HTTP_MANAGEMENT_KEYSTORE_PATH` | [httpManagement/keystorePath]({{< relref "reaper_specific.md#httpManagement" >}}) | |
| `REAPER_HTTP_MANAGEMENT_TRUSTSTORE_PATH` | [httpManagement/truststorePath]({{< relref "reaper_specific.md#httpManagement" >}}) | |
| `REAPER_HTTP_MANAGEMENT_TRUSTSTORES_DIR` | [httpManagement/truststoresDir]({{< relref "reaper_specific.md#httpManagement" >}}) | |
| `REAPER_MGMT_API_METRICS_PORT` | [mgmtApiMetricsPort]({{< relref "reaper_specific.md#mgmtapimetricsport" >}}) | 9000 |
| `REAPER_PURGE_RECORDS_AFTER_IN_DAYS` | [purgeRecordsAfterInDays]({{< relref "reaper_specific.md#purgeRecordsAfterInDays" >}}) | 30 |

## Runtime Configuration Variables

The following Docker environment variables control runtime behavior and do not map directly to configuration file settings:

| Environment Variable | Description | Default Value |
|---|---|---|
| `REAPER_HEAP_SIZE` | JVM heap size for the Reaper process | 1G |
| `REAPER_TMP_DIRECTORY` | Temporary directory for Reaper operations | /var/tmp/cassandra-reaper |
| `REAPER_MEMORY_STORAGE_DIRECTORY` | Directory for memory storage persistence | /var/lib/cassandra-reaper/storage |
| `JAVA_OPTS` | Additional JVM options to pass to the Reaper process | |

## Cluster Registration Variables

The following Docker environment variables are used when running the `register-clusters` command:

| Environment Variable | Description | Default Value |
|---|---|---|
| `REAPER_AUTO_SCHEDULING_SEEDS` | Comma-separated list of 'host:port' entries for Cassandra nodes | |
| `REAPER_HOST` | Hostname of the Reaper instance | |
| `REAPER_PORT` | Port of the Reaper instance | 8080 |

## Using Address translator mapping

The Docker environment variables listed in this section are those related to the feature address translator mapping.

| Environment Variable | Configuration Setting | Example Values |
|---|---|---|
| `REAPER_CASS_ADDRESS_TRANSLATOR_TYPE` | [addressTranslator]({{< relref "reaper_specific.md#addressTranslator" >}}) | ec2MultiRegion or multiIpPerNode |
| `REAPER_CASS_ADDRESS_TRANSLATOR_MAPPING` | [addressTranslator]({{< relref "reaper_specific.md#addressTranslator" >}}) | host1:ip1,host2:ip2,host3:ip3 |
| `JMX_ADDRESS_TRANSLATOR_TYPE` | [jmxAddressTranslator]({{< relref "reaper_specific.md#jmxAddressTranslator" >}}) | ec2MultiRegion or multiIpPerNode |
| `JMX_ADDRESS_TRANSLATOR_MAPPING` | [jmxAddressTranslator]({{< relref "reaper_specific.md#jmxAddressTranslator" >}}) | host1:ip1,host2:ip2,host3:ip3 |

Example :

```
REAPER_CASS_ADDRESS_TRANSLATOR_TYPE=multiIpPerNode
REAPER_CASS_ADDRESS_TRANSLATOR_MAPPING=host1:ip1,host2:ip2
```
config bloc at the container startup file '/etc/cassandra-reaper/cassandra-reaper.yml' :
```
 addressTranslator:
    type: multiIpPerNode
    ipTranslations:
    - from: "host1"
      to: "ip1"
    - from: "host2"
      to: "ip2"
```
and the same thing for the jmx mapping
```
JMX_ADDRESS_TRANSLATOR_TYPE=multiIpPerNode
JMX_ADDRESS_TRANSLATOR_MAPPING=host1:ip1,host2:ip2
```
result
```
jmxAddressTranslator:
  type: multiIpPerNode
  ipTranslations:
    - from: "host1"
      to: "ip1"
    - from: "host2"
      to: "ip2"
```

**Note:**

## Authentication Configuration Variables

The following Docker environment variables control authentication settings. These variables are handled specially by configuration scripts that run at container startup.

| Environment Variable | Description | Required |
|---|---|---|
| `REAPER_AUTH_ENABLED` | Enable/disable authentication globally | No (default: true) |
| `REAPER_AUTH_USER` | Username for the admin user with operator role | **Yes** (when auth is enabled) |
| `REAPER_AUTH_PASSWORD` | Password for the admin user | **Yes** (when auth is enabled) |
| `REAPER_READ_USER` | Username for optional read-only user | No (only configured if both user and password are set) |
| `REAPER_READ_USER_PASSWORD` | Password for optional read-only user | No (only configured if both user and password are set) |

**Note**: The read-only user (`REAPER_READ_USER` and `REAPER_READ_USER_PASSWORD`) is configured conditionally. The user is only added to the authentication configuration if both environment variables are set to non-empty values. This prevents environment variable resolution errors while maintaining security.

## Associated Reaper Specific Configuration Settings

The following Docker environment variables have no direct mapping to a setting in the *cassandra-reaper.yaml* configuration file. However, they do affect the content contained in the file that is Reaper specific.

#### `REAPER_METRICS_ENABLED`

Type: *Boolean*

Default: *false*

Allows the sending of Reaper metrics to a metrics reporting system such as Graphite. If enabled, the other associated environment variables `REAPER_METRICS_FREQUENCY` and `REAPER_METRICS_REPORTERS` must be set to appropriate values in order for metrics reporting to function correctly.

## Metrics reporter definition syntax
  
#### `REAPER_METRICS_REPORTERS`

Type: *List*

Default: *[]*

Defines the metrics reporters, using a JSON syntax instead of a YAML one.
To activate graphite metrics reporting, run the container with the following sample arguments :  

```
docker run \
    -p 8080:8080 \
    -p 8081:8081 \
    -e "REAPER_METRICS_ENABLED=true" \
    -e "REAPER_METRICS_FREQUENCY=10 second" \
    -e "REAPER_METRICS_REPORTERS=[{type: graphite, host: my.graphite.host.com, port: 2003, prefix: my.prefix}]" \
    thelastpickle/cassandra-reaper:master
```

## Direct Mapping to Cassandra Backend Specific Configuration Settings

The Docker environment variables listed in this section map directly to Cassandra backend specific settings in the *cassandra-reaper.yaml* configuration file. The following table below lists the Docker environment variables, their associated Cassandra backend specific setting in the *cassandra-reaper.yaml* configuration file, and the default value assigned by the Docker container (if any). Definitions for each Docker environment variable can be found via the link to the associated setting.

In order for the Cassandra backend to be used, `REAPER_STORAGE_TYPE` must be set to `cassandra`.

| Environment Variable | Configuration Setting | Default Value |
|---|---|---|
| `REAPER_CASS_ACTIVATE_QUERY_LOGGER` | [activateQueryLogger]({{< relref "backend_specific.md#activatequerylogger" >}}) | false |
| `REAPER_CASS_CLUSTER_NAME` | [clusterName]({{< relref "backend_specific.md#clustername" >}}) | clustername |
| `REAPER_CASS_CONTACT_POINTS` | [contactPoints]({{< relref "backend_specific.md#contactpoints" >}}) | {"host": "127.0.0.1", "port": "9042"}, {"host": "127.0.0.2", "port": "9042"} |
| `REAPER_CASS_KEYSPACE` | [keyspace]({{< relref "backend_specific.md#keyspace" >}}) | reaper_db |
| `REAPER_CASS_LOCAL_DC` | [localDC]({{< relref "backend_specific.md#localdc" >}}) | |
| `REAPER_CASS_AUTH_USERNAME` | [username]({{< relref "backend_specific.md#username" >}}) | cassandra |
| `REAPER_CASS_AUTH_PASSWORD` | [password]({{< relref "backend_specific.md#password" >}}) | cassandra |
| `REAPER_CASS_SCHEMA_AGREEMENT_INTERVAL` | [agreementIntervalMilliseconds]({{< relref "backend_specific.md#agreementIntervalMilliseconds" >}}) | 2000 |
| `REAPER_CASS_SCHEMA_AGREEMENT_TIMEOUT` | [agreementTimeoutSeconds]({{< relref "backend_specific.md#agreementTimeoutSeconds" >}}) | 10 |
| `REAPER_CASS_REQUEST_TIMEOUT` | [requestTimeout]({{< relref "backend_specific.md#requestTimeout" >}}) | 10s |

**Note:**

Some variable names and defaults have changed between the release of Docker-support and Reaper for Apache Cassandra 1.0. The following Cassandra Backend specific variable name changes have occurred in an effort to match closely with our YAML parameter names:

| Pre Reaper 1.0 | Post Reaper 1.0 |
|---|---|
| `REAPER_ACTIVATE_QUERY_LOGGER` | `REAPER_CASS_ACTIVATE_QUERY_LOGGER` |

The following default values have changed:

| Environment Variable | Previous Default | New Default |
|---|---|---|
| `REAPER_CASS_KEYSPACE` | *cassandra-reaper* | *reaper_db* |
| `REAPER_CASS_SCHEMA_AGREEMENT_TIMEOUT` | *2000* | *10* |

## Associated Cassandra Backend Specific Configuration Settings

The following Docker environment variables have no direct mapping to a setting in the *cassandra-reaper.yaml* configuration file. However, the do affect the content contained in the file that is Cassandra backend specific.

#### `REAPER_CASS_AUTH_ENABLED`

Type: *Boolean*

Default: *false*

Allows Reaper to send authentication credentials when establishing a connection with Cassandra via the native protocol. When enabled, authentication credentials must be specified by setting values for `REAPER_CASS_AUTH_USERNAME` and `REAPER_CASS_AUTH_PASSWORD`.

#### `REAPER_CASS_ADDRESS_TRANSLATOR_ENABLED`

Type: *Boolean*

Default: *false*

Enables the use of address translation for Cassandra connections. When enabled, `REAPER_CASS_ADDRESS_TRANSLATOR_TYPE` and `REAPER_CASS_ADDRESS_TRANSLATOR_MAPPING` must be set appropriately.

#### `REAPER_CASS_NATIVE_PROTOCOL_SSL_ENCRYPTION_ENABLED`

Type: *Boolean*

Default: *false*

Allows Reaper to establish an encrypted connection when establishing a connection with Cassandra via the native protocol.


