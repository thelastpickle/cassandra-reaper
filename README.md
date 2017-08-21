Reaper for Apache Cassandra
============================

[![Join the chat at https://gitter.im/thelastpickle/cassandra-reaper](https://badges.gitter.im/thelastpickle/cassandra-reaper.svg)](https://gitter.im/thelastpickle/cassandra-reaper?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

[![Build Status](https://travis-ci.org/thelastpickle/cassandra-reaper.svg?branch=master)](https://travis-ci.org/thelastpickle/cassandra-reaper)

*Note: This repo is a fork from the original Reaper project, created by the awesome folks at Spotify.  The WebUI has been merged in with support for incremental repairs added.* 

Reaper is a centralized, stateful, and highly configurable tool for running Apache Cassandra
repairs against single or multi-site clusters.

The current version supports running Apache Cassandra cluster repairs in a segmented manner, 
opportunistically running multiple parallel repairs at the same time on different nodes
within the cluster. Basic repair scheduling functionality is also supported.

Reaper comes with a GUI, which if you're running in local mode can be at http://localhost:8080/webui/ 

Please see the [Issues](https://github.com/thelastpickle/cassandra-reaper/issues) section for more
information on planned development, and known issues.

Documentation and Help
------------------------

The full documentation is available at the [Reaper website](http://cassandra-reaper.io/).  The [source for the website](https://github.com/thelastpickle/reaper-site) is available as well, and pull requests are encouraged!

Have a question?  Please ask on the [reaper mailing list](https://groups.google.com/forum/#!forum/tlp-apache-cassandra-reaper-users)! 


System Overview
---------------

Reaper consists of a database containing the full state of the system, a REST-full API,
and a CLI tool called *spreaper* that provides an alternative way to issue commands to a running
Reaper instance. Communication with Cassandra nodes in registered clusters is handled through JMX.

Reaper system does not use internal caches for state changes regarding running repairs and
registered clusters, which means that any changes done to the storage will reflect to the running
system dynamically.

You can also run the Reaper with memory storage, which is not persistent, and is meant to
be used only for testing purposes.

This project is built on top of Dropwizard:
http://dropwizard.io/


Clusters with closed cross DC JMX ports
---------------------------------------

For security reasons, it is possible that Reaper will be able to access only a single DC nodes through JMX (multi region clusters for example).
In the case where the JMX port is accessible (with or without authentication) from the running Reaper instance only to the nodes in the current DC, it is possible to have a multiple instances of Reaper running in different DCs.

This setup works with Apache Cassandra as a backend only. It is unsuitable for memory, H2 and Postgres.

Reaper instances will rely on lightweight transactions to get leadership on segments before processing them.
Reaper checks the number of pending compactions and actively running repairs on all replicas before processing a segment. The `datacenterAvailability` setting in the yaml file controls the behavior for metrics collection :  

* `datacenterAvailability: ALL` requires direct JMX access to all nodes across all datacenters.
* `datacenterAvailability: LOCAL` requires jmx access to all nodes in the datacenter local to reaper.
* `datacenterAvailability: EACH` means each datacenter requires at minimum one reaper instance which has jmx access to all nodes within that datacenter

