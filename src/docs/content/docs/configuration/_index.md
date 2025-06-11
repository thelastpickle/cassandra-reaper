---
title: "Configuration Reference"
weight: 15
---

An example testing configuration YAML file can be found from within this project repository:
[src/server/src/test/resources/cassandra-reaper.yaml](https://github.com/thelastpickle/cassandra-reaper/blob/master/src/packaging/resource/cassandra-reaper-cassandra.yaml).

The configuration file structure is provided by Dropwizard, and help on configuring the server, database connection, or logging, can be found on the [Dropwizard Configuration Reference](http://www.dropwizard.io/1.1.0/docs/manual/configuration.html)

The configuration is broken into the following sections:

* [Reaper Specific]({{<ref "reaper_specific.md">}}) - Provides details on settings specific to Reaper.
* [Backend Specific]({{<ref "backend_specific.md">}}) - Provides details on settings specific to the different backends that can be used with Reaper; Cassandra, H2 and Postgres.
* [Docker Variables]({{<ref "docker_vars.md">}}) - Provides details on the Docker Variables that can be used to configure Reaper.

Note that Cassandra backend configuration relies on the [Dropwizard-Cassandra](https://github.com/composable-systems/dropwizard-cassandra) module.
