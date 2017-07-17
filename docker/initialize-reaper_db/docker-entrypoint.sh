#!/usr/bin/env bash

set -x

REPLICATION_FACTOR=$1

# create the required reaper_db keyspace to allow Reaper to store scheduling data
cqlsh cassandra -e \
    "CREATE KEYSPACE IF NOT EXISTS reaper_db \
    WITH replication = {'class': 'SimpleStrategy', \
                        'replication_factor': $REPLICATION_FACTOR };"
