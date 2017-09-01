#!/bin/bash

set -e

NODETOOL_OPTS=""

if [ "true" = "${NODETOOL_ENABLE_SSL}" ]; then
    NODETOOL_OPTS="--ssl"

    sed -ie "s/CASSANDRA_KEYSTORE_PASSWORD/${CASSANDRA_KEYSTORE_PASSWORD}/g" ${WORKDIR}/.cassandra/nodetool-ssl.properties
    sed -ie "s/CASSANDRA_TRUSTSTORE_PASSWORD/${CASSANDRA_TRUSTSTORE_PASSWORD}/g" ${WORKDIR}/.cassandra/nodetool-ssl.properties
fi

nodetool --host ${CASSANDRA_HOSTNAME} --username cassandraUser --password cassandraPass ${NODETOOL_OPTS} $1