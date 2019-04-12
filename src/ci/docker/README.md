# Why it's needed

`cassandra-reaper` application require complicated environment to be set in order to run. Since this takes lots of time to prepare execution/test environment and require installation of additional software libraries (e.g. `python`, `java-jna`, `ccm`, and such) we've made the decision to create dockerized environment with everything preinstalled.

# Building
To build Docker image run that command from root directory. Do not try to run this command from inside `docker` directory, as you will get _docker forbidden path outside the build context_ error!

You can change tag to whatever you wish.
`docker build --tag softsky/cassandra-reaper -f src/ci/docker/Dockerfile .`

# Running docker container and binding all ports to localhost

To run created docker with all ports bond to localhost invoke the following:
`docker run -ti --rm -p 7100:7100 -p 7200:7200 -p 7300:7300 -p 7400:7400 -p 44175:44175 -p 127.0.0.1:9042:9042 -p 127.0.0.2:9042:9042 -p 127.0.0.3:9042:9042 -p 127.0.0.4:9042:9042 -p 127.0.0.1:7000:7000 -p 127.0.0.2:7000:7000 -p 127.0.0.3:7000:7000 -p 127.0.0.4:7000:7000 softsky/cassandra-reaper`

By default CASSANDRA_VERSION=2.1.20 is set

You can set environment variables, e.g.:
`docker run -ti --rm -eCASSANDRA_VERSION=2.2.13 -eNODES_PER_DC=4 softsky/cassandra-reaper`

Here's the list of supported environment variables among with default values:
- CASSANDRA_VERSION=2.1.20
- NODES_PER_DC=2
- LOCAL_JMX=no
