Reaper for Apache Cassandra
============================

[![Build Status](https://github.com/thelastpickle/cassandra-reaper/actions/workflows/ci.yaml/badge.svg?branch=master)](https://github.com/thelastpickle/cassandra-reaper/actions?query=branch%3Amaster)

[![codecov](https://codecov.io/gh/thelastpickle/cassandra-reaper/branch/master/graph/badge.svg?token=8q1tX81waa)](https://codecov.io/gh/thelastpickle/cassandra-reaper)

[![Hosted By: Cloudsmith](https://img.shields.io/badge/OSS%20hosting%20by-cloudsmith-blue?logo=cloudsmith&style=flat-square)](https://cloudsmith.io/~thelastpickle/repos/reaper/packages/)

[![javadoc](https://javadoc.io/badge2/io.cassandrareaper/cassandra-reaper/javadoc.svg)](https://javadoc.io/doc/io.cassandrareaper/cassandra-reaper)

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

Version compatibility 
------------
Reaper can be built using Java 8 or 11.  It is tested against Cassandra 3.11 and 4.0.  It is no longer tested against Cassandra 2.x.

We have confirmed the Reaper UI will build with npm 5.6.0, node 10.0.0. We believe that more generally versions of npm up to 6.14 and both node 12.x and 14.x will work. Builds are confirmed to fail with node 16+.

We recommend the use of nvm to manage node versions.


Dependencies
------------

Reaper uses an unmodified EPL-2.0 licensed dependency: [EclipseStore](https://eclipsestore.io/). The source code can be found in the [GitHub repository](https://github.com/eclipse-store/store).

*Note: This repo is a fork from the original Reaper project, created by the awesome folks at Spotify.*
