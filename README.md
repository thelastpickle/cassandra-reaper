Reaper for Apache Cassandra
============================

[![Build Status](https://github.com/thelastpickle/cassandra-reaper/actions/workflows/ci.yaml/badge.svg?branch=master)](https://github.com/thelastpickle/cassandra-reaper/actions?query=branch%3Amaster)

[![codecov](https://codecov.io/gh/thelastpickle/cassandra-reaper/branch/master/graph/badge.svg?token=8q1tX81waa)](https://codecov.io/gh/thelastpickle/cassandra-reaper)

[![Hosted By: Cloudsmith](https://img.shields.io/badge/OSS%20hosting%20by-cloudsmith-blue?logo=cloudsmith&style=flat-square)](https://cloudsmith.io/~thelastpickle/repos/reaper/packages/)

Reaper is a centralized, stateful, and highly configurable tool for running Apache Cassandra repairs against single or multi-site clusters.

The current version supports running Apache Cassandra cluster repairs in a segmented manner,  opportunistically running multiple parallel repairs at the same time on different nodes within the cluster. Basic repair scheduling functionality is also supported.

Reaper comes with a GUI, which if you're running in local mode can be at http://localhost:8080/webui/ 

Please see the [Issues](https://github.com/thelastpickle/cassandra-reaper/issues) section for more information on planned development, and known issues.

Documentation and Help
------------------------

The full documentation is available at the [Reaper website](http://cassandra-reaper.io/).  The source for the site is located in this repo at `src/docs`.

Have a question?  Join us on [the ASF Slack](https://the-asf.slack.com/) in the #cassandra-reaper channel.


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

Dependencies
------------

For information on the packaged dependencies of Reaper for Apache Cassandra&reg; and their licenses, check out our [open source report](https://app.fossa.com/reports/9754f92d-782f-4660-a410-d0337e8ff514).


*Note: This repo is a fork from the original Reaper project, created by the awesome folks at Spotify.*
