#!/usr/bin/env bash

set -x

CASSANDRA_HOST=$@

# run the specified .cql file using cqlsh, while connecting to the IP address
# provided as an argument to this script
../apache-cassandra-3.0.9/bin/cqlsh $CASSANDRA_HOST \
    --file ../cassandra-reaper/src/main/resources/db/reaper_cassandra_db.cql \
    && echo 'Schema created!'
