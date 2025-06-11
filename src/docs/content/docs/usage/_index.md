---
title: "Using Reaper"
weight: 25
---

## Starting Reaper

The preferred way of running Reaper is to make it a service, which is the default for debian and RPM packaged installations.
It can also be started from the command line for tarball/source installs, in two different ways:

- by invoking `src/packaging/bin/cassandra-reaper`
- by running `java -jar <path/to/cassandra-reaper-X.X.X.jar> server <path/to/cassandra-reaper.yaml>`

## Schema migrations

Schema migrations are executed on startup and assume that the database/keyspace already exists.
Reaper can run in schema migration mode only, exiting right after the database schema was upgraded, by running the following command:

```
java -jar <path/to/cassandra-reaper-X.X.X.jar> schema-migration <path/to/cassandra-reaper.yaml>
```

## Using Reaper

This section discusses the normal usage of Reaper on a day to day basis.

Reaper includes a community-driven web interface that can be accessed at:

**http://$REAPER_HOST:8080/webui/index.html**

The web interface provides the ability to:

* [Add/remove](add_cluster) clusters
* [Manage repair](schedule) schedules
* [Run manual repairs](single) and manage running repairs
