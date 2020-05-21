+++
title = "Docker Variables"
menuTitle = "Docker Variables"
parent = "configuration"
weight = 10
+++


The Reaper Docker container has been designed to be highly configurable. Many of the environment variables map directly or indirectly to a settings in the *cassandra-reaper.yaml* configuration file.

## Direct Mapping to Reaper Specific Configuration Settings

The Docker environment variables listed in this section map directly to Reaper specific settings in the *cassandra-reaper.yaml* configuration file. The following table below lists the Docker environment variables, their associated Reaper specific setting in the *cassandra-reaper.yaml* configuration file, and the default value assigned by the Docker container (if any). Definitions for each Docker environment variable can be found via the link to the associated setting.

<h4>Environment Variable</h4> | <h4>Configuration Setting</h4> | <h4>Default Value</h4>
--- | --- | ---
<code class="codeLarge">REAPER_AUTO_SCHEDULING_ENABLED</code> | [enabled]({{< relref "reaper_specific.md#enabled" >}}) | false
<code class="codeLarge">REAPER_AUTO_SCHEDULING_EXCLUDED_KEYSPACES</code> | [excludedKeyspaces]({{< relref "reaper_specific.md#excludedkeyspaces" >}}) | []
<code class="codeLarge">REAPER_AUTO_SCHEDULING_INITIAL_DELAY_PERIOD</code> | [initialDelayPeriod]({{< relref "reaper_specific.md#initialdelayperiod" >}}) | PT15S
<code class="codeLarge">REAPER_AUTO_SCHEDULING_PERIOD_BETWEEN_POLLS</code> | [periodBetweenPolls]({{< relref "reaper_specific.md#periodbetweenpolls" >}}) | PT10M
<code class="codeLarge">REAPER_AUTO_SCHEDULING_SCHEDULE_SPREAD_PERIOD</code> | [scheduleSpreadPeriod]({{< relref "reaper_specific.md#schedulespreadperiod" >}}) | PT6H
<code class="codeLarge">REAPER_AUTO_SCHEDULING_TIME_BEFORE_FIRST_SCHEDULE</code> | [timeBeforeFirstSchedule]({{< relref "reaper_specific.md#timebeforefirstschedule" >}}) | PT5M
<code class="codeLarge">REAPER_DATACENTER_AVAILABILITY</code> | [datacenterAvailability]({{< relref "reaper_specific.md#datacenteravailability" >}}) | ALL
<code class="codeLarge">REAPER_ENABLE_CROSS_ORIGIN</code> | [enableCrossOrigin]({{< relref "reaper_specific.md#enablecrossorigin" >}}) | true
<code class="codeLarge">REAPER_ENABLE_DYNAMIC_SEED_LIST</code> | [enableDynamicSeedList]({{< relref "reaper_specific.md#enabledynamicseedlist" >}}) | true
<code class="codeLarge">REAPER_HANGING_REPAIR_TIMEOUT_MINS</code> | [hangingRepairTimeoutMins]({{< relref "reaper_specific.md#hangingrepairtimeoutmins" >}}) | 30
<code class="codeLarge">REAPER_INCREMENTAL_REPAIR</code> | [incrementalRepair]({{< relref "reaper_specific.md#incrementalrepair" >}}) | false
<code class="codeLarge">REAPER_JMX_AUTH_PASSWORD</code> | [password]({{< relref "reaper_specific.md#password" >}}) |
<code class="codeLarge">REAPER_JMX_AUTH_USERNAME</code> | [username]({{< relref "reaper_specific.md#username" >}}) |
<code class="codeLarge">REAPER_JMX_CONNECTION_TIMEOUT_IN_SECONDS</code> | [jmxConnectionTimeoutInSeconds]({{< relref "reaper_specific.md#jmxconnectiontimeoutinseconds" >}}) | 20
<code class="codeLarge">REAPER_JMX_PORTS</code> | [jmxPorts]({{< relref "reaper_specific.md#jmxports" >}}) | {}
<code class="codeLarge">REAPER_LOGGING_APPENDERS_CONSOLE_LOG_FORMAT</code> | [logFormat]({{< relref "reaper_specific.md#logformat" >}}) | "%-6level [%d] [%t] %logger{5} - %msg %n"
<code class="codeLarge">REAPER_LOGGING_APPENDERS_CONSOLE_THRESHOLD</code> | [threshold]({{< relref "reaper_specific.md#threshold" >}}) | WARN
<code class="codeLarge">REAPER_LOGGING_LOGGERS</code> | [loggers]({{< relref "reaper_specific.md#loggers" >}}) | {}
<code class="codeLarge">REAPER_LOGGING_ROOT_LEVEL</code> | [level]({{< relref "reaper_specific.md#level" >}}) | INFO
<code class="codeLarge">REAPER_METRICS_FREQUENCY</code> | [fequency]({{< relref "reaper_specific.md#fequency" >}}) | 1 minute
<code class="codeLarge">REAPER_METRICS_REPORTERS</code> | [reporters]({{< relref "reaper_specific.md#reporters" >}}) | []
<code class="codeLarge">REAPER_REPAIR_INTENSITY</code> | [repairIntensity]({{< relref "reaper_specific.md#repairintensity" >}}) | 0.9
<code class="codeLarge">REAPER_REPAIR_MANAGER_SCHEDULING_INTERVAL_SECONDS</code> | [repairManagerSchedulingIntervalSeconds]({{< relref "reaper_specific.md#repairmanagerschedulingintervalseconds" >}}) | 30
<code class="codeLarge">REAPER_REPAIR_PARALELLISM</code> | [repairParallelism]({{< relref "reaper_specific.md#repairparallelism" >}}) | DATACENTER_AWARE
<code class="codeLarge">REAPER_REPAIR_RUN_THREADS</code> | [repairRunThreadCount]({{< relref "reaper_specific.md#repairrunthreadcount" >}}) | 15
<code class="codeLarge">REAPER_SCHEDULE_DAYS_BETWEEN</code> | [scheduleDaysBetween]({{< relref "reaper_specific.md#scheduledaysbetween" >}}) | 7
<code class="codeLarge">REAPER_SEGMENT_COUNT</code> | [segmentCount]({{< relref "reaper_specific.md#segmentcount" >}}) | 200
<code class="codeLarge">REAPER_SERVER_ADMIN_BIND_HOST</code> | [bindHost]({{< relref "reaper_specific.md#bindhost" >}}) | 0.0.0.0
<code class="codeLarge">REAPER_SERVER_ADMIN_PORT</code> | [port]({{< relref "reaper_specific.md#port" >}}) | 8081
<code class="codeLarge">REAPER_SERVER_APP_BIND_HOST</code> | [bindHost]({{< relref "reaper_specific.md#bindhost" >}}) | 0.0.0.0
<code class="codeLarge">REAPER_SERVER_APP_PORT</code> | [port]({{< relref "reaper_specific.md#port" >}}) | 8080
<code class="codeLarge">REAPER_STORAGE_TYPE</code> | [storageType]({{< relref "reaper_specific.md#storagetype" >}}) | memory
<code class="codeLarge">REAPER_USE_ADDRESS_TRANSLATOR</code> | [useAddressTranslator]({{< relref "reaper_specific.md#useaddresstranslator" >}}) | false

<br/>

**Note:**

Some variable names have changed between the release of Docker-support and Reaper for Apache Cassandra 1.0. The following Reaper specific variable name changes have occurred in an effort to match closely with the YAML parameter names:

<h4>Pre Reaper 1.0</h4> | <h4>Post Reaper 1.0</h4>
---|---
`REAPER_ENABLE_CORS` | `REAPER_ENABLE_CROSS_ORIGIN`
`REAPER_ENABLE_DYNAMIC_SEEDS` | `REAPER_ENABLE_DYNAMIC_SEED_LIST`
`REAPER_AUTO_SCHEDULE_ENABLED` | `REAPER_AUTO_SCHEDULING_ENABLED`
`REAPER_AUTO_SCHEDULE_INITIAL_DELAY_PERIOD` | `REAPER_AUTO_SCHEDULING_INITIAL_DELAY_PERIOD`
`REAPER_AUTO_SCHEDULE_PERIOD_BETWEEN_POLLS` | `REAPER_AUTO_SCHEDULING_PERIOD_BETWEEN_POLLS`
`REAPER_AUTO_SCHEDULE_TIME_BETWEEN_FIRST_SCHEDULE` | `REAPER_AUTO_SCHEDULING_TIME_BEFORE_FIRST_SCHEDULE`
`REAPER_AUTO_SCHEDULE_EXCLUDED_KEYSPACES` | `REAPER_AUTO_SCHEDULING_EXCLUDED_KEYSPACES`
`REAPER_JMX_USERNAME` | `REAPER_JMX_AUTH_USERNAME`
`REAPER_JMX_PASSWORD` | `REAPER_JMX_AUTH_PASSWORD`
`REAPER_LOGGERS` | `REAPER_LOGGING_LOGGERS`
`REAPER_LOGGING_FORMAT` | `REAPER_LOGGING_APPENDERS_CONSOLE_LOG_FORMAT`
`REAPER_APP_PORT` | `REAPER_SERVER_APP_PORT`
`REAPER_APP_BIND_HOST` | `REAPER_SERVER_APP_BIND_HOST`
`REAPER_ADMIN_PORT` | `REAPER_SERVER_ADMIN_PORT`
`REAPER_ADMIN_BIND_HOST` | `REAPER_SERVER_ADMIN_BIND_HOST`
`REAPER_ACTIVATE_QUERY_LOGGER` | `REAPER_CASS_ACTIVATE_QUERY_LOGGER`

## Associated Reaper Specific Configuration Settings

The following Docker environment variables have no direct mapping to a setting in the *cassandra-reaper.yaml* configuration file. However, they do affect the content contained in the file that is Reaper specific.

</br>

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

<br/>

<h4>Environment Variable</h4> | <h4>Configuration Setting</h4> | <h4>Default Value</h4>
---|---|---
<code class="codeLarge">REAPER_CASS_ACTIVATE_QUERY_LOGGER</code> | [activateQueryLogger]({{< relref "backend_specific.md#activatequerylogger" >}}) | false
<code class="codeLarge">REAPER_CASS_CLUSTER_NAME</code> | [clusterName]({{< relref "backend_specific.md#clustername" >}}) | clustername
<code class="codeLarge">REAPER_CASS_CONTACT_POINTS</code> | [contactPoints]({{< relref "backend_specific.md#contactpoints" >}}) | []
<code class="codeLarge">REAPER_CASS_PORT</code> | [port]({{< relref "backend_specific.md#port" >}}) | []
<code class="codeLarge">REAPER_CASS_KEYSPACE</code> | [keyspace]({{< relref "backend_specific.md#keyspace" >}}) | cassandra-reaper
<code class="codeLarge">REAPER_CASS_LOCAL_DC</code> | [localDC]({{< relref "backend_specific.md#localdc" >}}) |
<code class="codeLarge">REAPER_CASS_AUTH_USERNAME</code> | [username]({{< relref "backend_specific.md#username" >}}) | cassandra
<code class="codeLarge">REAPER_CASS_AUTH_PASSWORD</code> | [password]({{< relref "backend_specific.md#password" >}}) | cassandra

<br/>

**Note:**

Some variable names and defaults have changed between the release of Docker-support and Reaper for Apache Cassandra 1.0. The following Cassandra Backend specific variable name changes have occurred in an effort to match closely with our YAML parameter names:

<h4>Pre Reaper 1.0</h4> | <h4>Post Reaper 1.0</h4>
---|---
`REAPER_ACTIVATE_QUERY_LOGGER` | `REAPER_CASS_ACTIVATE_QUERY_LOGGER`

<br/>

The following default values have changed:

<h4>Environment Variable</h4> | <h4>Previous Default</h4> | <h4>New Default</h4>
---|---|---
`REAPER_CASS_KEYSPACE` | *cassandra-reaper* | *reaper_db*

## Associated Cassandra Backend Specific Configuration Settings

The following Docker environment variables have no direct mapping to a setting in the *cassandra-reaper.yaml* configuration file. However, the do affect the content contained in the file that is Cassandra backend specific.

</br>

#### `REAPER_CASS_AUTH_ENABLED`

Type: *Boolean*

Default: *false*

Allows Reaper to send authentication credentials when establishing a connection with Cassandra via the native protocol. When enabled, authentication credentials must be specified by setting values for `REAPER_CASS_AUTH_USERNAME` and `REAPER_CASS_AUTH_PASSWORD`.

</br>

#### `REAPER_CASS_NATIVE_PROTOCOL_SSL_ENCRYPTION_ENABLED`

Type: *Boolean*

Default: *false*

Allows Reaper to establish an encrypted connection when establishing a connection with Cassandra via the native protocol.

## Direct Mapping to H2 or Postgres Backend Configuration Settings

The Docker environment variables listed in this section map directly to H2/Postgres backend specific settings in the *cassandra-reaper.yaml* configuration file. The following table below lists the Docker environment variables, their associated H2/Postgres backend specific setting in the *cassandra-reaper.yaml* configuration file, and the default value assigned by the Docker container (if any). Definitions for each Docker environment variable can be found via the link to the associated setting.

In order to use the following settings, `REAPER_STORAGE_TYPE` must be set to `h2` or `postgres`.

<h4>Environment Variable</h4> | <h4>Configuration Setting</h4> | <h4>Default Value</h4>
---|---|---
<code class="codeLarge">REAPER_DB_URL</code> | [url]({{< relref "backend_specific.md#url" >}}) | jdbc:h2:/var/lib/cassandra-reaper/db;MODE=PostgreSQL
<code class="codeLarge">REAPER_DB_USERNAME</code> | [user]({{< relref "backend_specific.md#user" >}}) |
<code class="codeLarge">REAPER_DB_PASSWORD</code> | [password]({{< relref "backend_specific.md#password-1" >}}) |

<br/>

**Note:**

Some variable names have changed between the release of Docker-support and Reaper for Apache Cassandra 1.0. The following Reaper specific variable name changes have occurred in an effort to match closely with the YAML parameter names:

<h4>Pre Reaper 1.0</h4> | <h4>Post Reaper 1.0</h4>
---|---
`REAPER_DB_DRIVER_CLASS` | N/A - The associated parameter has been deprecated
