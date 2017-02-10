#!/usr/bin/env bash

set -x

CASSANDRA_HOST=$@

# define the Cassandra IP address to connect to
sed -i'.bak' "s/\[\"127.0.0.1\"\]/[\"$CASSANDRA_HOST\"]/g" \
    ${WORKDIR}/cassandra-reaper/resource/cassandra-reaper-cassandra.yaml

# run Reaper with a Cassandra backend
java -jar cassandra-reaper-0.4.0-SNAPSHOT.jar server cassandra-reaper/resource/cassandra-reaper-cassandra.yaml
