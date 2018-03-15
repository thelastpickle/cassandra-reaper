+++
show_menu = true
+++

## Welcome to the Cassandra Reaper documentation!

### Overview

Reaper is an open source tool that aims to schedule and orchestrate repairs of Apache Cassandra clusters.

It improves the existing nodetool repair process by

* Splitting repair jobs into smaller tunable segments.
* Handling back-pressure through monitoring running repairs and pending compactions.
* Adding ability to pause or cancel repairs and track progress precisely.

Reaper ships with a REST API, a command line tool and a web UI.

This documentation includes instructions on how build, install, and configure Reaper correctly.


### Compatibility

Reaper supports all versions of Apache Cassandra ranging from 1.2 to 3.10. Incremental repair is supported for versions from 2.1 and above.

A single instance of Reaper can handle repairs for clusters running different Apache Cassandra versions.

### Download and Installation

Head over to the [Downloads](download) section to download the version of Reaper that is compatible with your system.
