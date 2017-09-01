#!/usr/bin/env bash

set -xe

REPLICATION_FACTOR=${1:-1}

CQLSH_COMMAND="CREATE KEYSPACE IF NOT EXISTS reaper_db WITH replication = {'class': 'NetworkTopologyStrategy', '$CASSANDRA_DC': $REPLICATION_FACTOR };"

/docker-entrypoint.sh