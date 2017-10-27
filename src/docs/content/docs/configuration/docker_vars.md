+++
[menu.docs]
name = "Docker Variables"
parent = "configuration"
weight = 10
+++

# Docker Variables

The Reaper Docker container has been designed to be highly configurable. Many of the environment variables map directly or indirectly to a settings in the *cassandra-reaper.yaml* configuration file.

## Direct Mapping to Reaper Specific Configuration Settings

The Docker environment variables listed in this section map directly to Reaper specific settings in the *cassandra-reaper.yaml* configuration file. The following table below lists the Docker environment variables, their associated Reaper specific setting in the *cassandra-reaper.yaml* configuration file, and the default value assigned by the Docker container (if any). Definitions for each Docker environment variable can be found via the link to the associated setting.

Environment Variable | Configuration Setting | Default Value
--- | --- | ---
<code class="codeLarge">REAPER_AUTO_SCHEDULING_ENABLED</code> | <a href="../reaper_specific#endabled">endabled</a> | false
<code class="codeLarge">REAPER_AUTO_SCHEDULING_EXCLUDED_KEYSPACES</code> | <a href="../reaper_specific#excludedkeyspaces">excludedKeyspaces</a> | []
<code class="codeLarge">REAPER_AUTO_SCHEDULING_INITIAL_DELAY_PERIOD</code> | <a href="../reaper_specific#initialdelayperiod">initialDelayPeriod</a> | PT15S
<code class="codeLarge">REAPER_AUTO_SCHEDULING_PERIOD_BETWEEN_POLLS</code> | <a href="../reaper_specific#periodbetweenpolls">periodBetweenPolls</a> | PT10M
<code class="codeLarge">REAPER_AUTO_SCHEDULING_SCHEDULE_SPREAD_PERIOD</code> | <a href="../reaper_specific#schedulespreadperiod">scheduleSpreadPeriod</a> | PT6H
<code class="codeLarge">REAPER_AUTO_SCHEDULING_TIME_BEFORE_FIRST_SCHEDULE</code> | <a href="../reaper_specific#timebeforefirstschedule">timeBeforeFirstSchedule</a> | PT5M
<code class="codeLarge">REAPER_DATACENTER_AVAILABILITY</code> | <a href="../reaper_specific#datacenteravailability">datacenterAvailability</a> | ALL
<code class="codeLarge">REAPER_ENABLE_CROSS_ORIGIN</code> | <a href="../reaper_specific#enablecrossorigin">enableCrossOrigin</a> | true
<code class="codeLarge">REAPER_ENABLE_DYNAMIC_SEED_LIST</code> | <a href="../reaper_specific#enabledynamicseedlist">enableDynamicSeedList</a> | true
<code class="codeLarge">REAPER_HANGING_REPAIR_TIMEOUT_MINS</code> | <a href="../reaper_specific#hangingrepairtimeoutmins">hangingRepairTimeoutMins</a> | 30
<code class="codeLarge">REAPER_INCREMENTAL_REPAIR</code> | <a href="../reaper_specific#incrementalrepair">incrementalRepair</a> | false
<code class="codeLarge">REAPER_JMX_AUTH_PASSWORD</code> | <a href="../reaper_specific#password">password</a> |
<code class="codeLarge">REAPER_JMX_AUTH_USERNAME</code> | <a href="../reaper_specific#username">username</a> |
<code class="codeLarge">REAPER_JMX_CONNECTION_TIMEOUT_IN_SECONDS</code> | <a href="../reaper_specific#jmxconnectiontimeoutinseconds">jmxConnectionTimeoutInSeconds</a> | 20
<code class="codeLarge">REAPER_JMX_PORTS</code> | <a href="../reaper_specific#jmxports">jmxPorts</a> | {}
<code class="codeLarge">REAPER_LOGGING_APPENDERS_LOG_FORMAT</code> | <a href="../reaper_specific#logformat">logFormat</a> | "%-6level [%d] [%t] %logger{5} - %msg %n"
<code class="codeLarge">REAPER_LOGGING_LOGGERS</code> | <a href="../reaper_specific#loggers">loggers</a> | {}
<code class="codeLarge">REAPER_LOGGING_ROOT_LEVEL</code> | <a href="../reaper_specific#level">level</a> | INFO
<code class="codeLarge">REAPER_METRICS_FREQUENCY</code> | <a href="../reaper_specific#fequency">fequency</a> | 1 minute
<code class="codeLarge">REAPER_METRICS_REPORTERS</code> | <a href="../reaper_specific#reporters">reporters</a> | []
<code class="codeLarge">REAPER_REPAIR_INTENSITY</code> | <a href="../reaper_specific#repairintensity">repairIntensity</a> | 0.9
<code class="codeLarge">REAPER_REPAIR_MANAGER_SCHEDULING_INTERVAL_SECONDS</code> | <a href="../reaper_specific#repairmanagerschedulingintervalseconds">repairManagerSchedulingIntervalSeconds</a> | 30
<code class="codeLarge">REAPER_REPAIR_PARALELLISM</code> | <a href="../reaper_specific#repairparallelism">repairParallelism</a> | DATACENTER_AWARE
<code class="codeLarge">REAPER_REPAIR_RUN_THREADS</code> | <a href="../reaper_specific#repairrunthreadcount">repairRunThreadCount</a> | 15
<code class="codeLarge">REAPER_SCHEDULE_DAYS_BETWEEN</code> | <a href="../reaper_specific#scheduledaysbetween">scheduleDaysBetween</a> | 7
<code class="codeLarge">REAPER_SEGMENT_COUNT</code> | <a href="../reaper_specific#segmentcount">segmentCount</a> | 200
<code class="codeLarge">REAPER_SERVER_ADMIN_BIND_HOST</code> | <a href="../reaper_specific#bindhost">bindHost</a> | 0.0.0.0
<code class="codeLarge">REAPER_SERVER_ADMIN_PORT</code> | <a href="../reaper_specific#port">port</a> | 8081
<code class="codeLarge">REAPER_SERVER_APP_BIND_HOST</code> | <a href="../reaper_specific#bindhost">bindHost</a> | 0.0.0.0
<code class="codeLarge">REAPER_SERVER_APP_PORT</code> | <a href="../reaper_specific#port">port</a> | 8080
<code class="codeLarge">REAPER_STORAGE_TYPE</code> | <a href="../reaper_specific#storagetype">storageType</a> | memory
<code class="codeLarge">REAPER_USE_ADDRESS_TRANSLATOR</code> | <a href="../reaper_specific#useaddresstranslator">useAddressTranslator</a> | false

<br/>

**Note:**

Some variable names have changed between the release of Docker-support and Reaper for Apache Cassandra 1.0. The following Reaper specific variable name changes have occurred in an effort to match closely with the YAML parameter names:

Pre Reaper 1.0 | Post Reaper 1.0
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
`REAPER_LOGGING_FORMAT` | `REAPER_LOGGING_APPENDERS_LOG_FORMAT`
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

## Direct Mapping to Cassandra Backend Specific Configuration Settings

The Docker environment variables listed in this section map directly to Cassandra backend specific settings in the *cassandra-reaper.yaml* configuration file. The following table below lists the Docker environment variables, their associated Cassandra backend specific setting in the *cassandra-reaper.yaml* configuration file, and the default value assigned by the Docker container (if any). Definitions for each Docker environment variable can be found via the link to the associated setting.

In order for the Cassandra backend to be used, `REAPER_STORAGE_TYPE` must be set to `cassandra`.

<br/>

Environment Variable | Configuration Setting | Default Value
---|---|---
<code class="codeLarge">REAPER_CASS_ACTIVATE_QUERY_LOGGER</code> | <a href="../backend_specific#activatequerylogger">activateQueryLogger</a> | false
<code class="codeLarge">REAPER_CASS_CLUSTER_NAME</code> | <a href="../backend_specific#clustername">clusterName</a> | clustername
<code class="codeLarge">REAPER_CASS_CONTACT_POINTS</code> | <a href="../backend_specific#contactpoints">contactPoints</a> | []
<code class="codeLarge">REAPER_CASS_KEYSPACE</code> | <a href="../backend_specific#keyspace">keyspace</a> | cassandra-reaper
<code class="codeLarge">REAPER_CASS_LOCAL_DC</code> | <a href="../backend_specific#localdc">localDC</a> |
<code class="codeLarge">REAPER_CASS_AUTH_USERNAME</code> | <a href="../backend_specific#username">username</a> | cassandra
<code class="codeLarge">REAPER_CASS_AUTH_PASSWORD</code> | <a href="../backend_specific#password">password</a> | cassandra

<br/>

**Note:**

Some variable names and defaults have changed between the release of Docker-support and Reaper for Apache Cassandra 1.0. The following Cassandra Backend specific variable name changes have occurred in an effort to match closely with our YAML parameter names:

Pre Reaper 1.0 | Post Reaper 1.0
---|---
`REAPER_ACTIVATE_QUERY_LOGGER` | `REAPER_CASS_ACTIVATE_QUERY_LOGGER`

<br/>

The following default values have changed:

Environment Variable | Previous Default | New Default
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

Environment Variable | Configuration Setting | Default Value
---|---|---
<code class="codeLarge">REAPER_DB_URL</code> | <a href="../backend_specific#url">url</a> | jdbc:h2:/var/lib/cassandra-reaper/db;MODE=PostgreSQL
<code class="codeLarge">REAPER_DB_USERNAME</code> | <a href="../backend_specific#user">user</a> |
<code class="codeLarge">REAPER_DB_PASSWORD</code> | <a href="../backend_specific#password-1">password</a> |

<br/>

**Note:**

Some variable names have changed between the release of Docker-support and Reaper for Apache Cassandra 1.0. The following Reaper specific variable name changes have occurred in an effort to match closely with the YAML parameter names:

Pre Reaper 1.0 | Post Reaper 1.0
---|---
`REAPER_DB_DRIVER_CLASS` | N/A - The associated parameter has been deprecated
