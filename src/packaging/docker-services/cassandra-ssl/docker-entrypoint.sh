#!/bin/bash

# copied from:
# https://github.com/docker-library/cassandra/blob/d83b850cd17bc9198876f8686197c730e29c7448/2.1/docker-entrypoint.sh

set -e

# first arg is `-f` or `--some-option`
if [ "${1:0:1}" = '-' ]; then
    set -- cassandra -f "$@"
fi

# allow the container to be started with `--user`
if [ "$1" = 'cassandra' -a "$(id -u)" = '0' ]; then
    chown -R cassandra /var/lib/cassandra /var/log/cassandra "$CASSANDRA_CONFIG"
    exec gosu cassandra "$BASH_SOURCE" "$@"
fi

if [ "$1" = 'cassandra' ]; then
    : ${CASSANDRA_RPC_ADDRESS='0.0.0.0'}

    : ${CASSANDRA_LISTEN_ADDRESS='auto'}
    if [ "$CASSANDRA_LISTEN_ADDRESS" = 'auto' ]; then
        CASSANDRA_LISTEN_ADDRESS="$(hostname --ip-address)"
    fi

    : ${CASSANDRA_BROADCAST_ADDRESS="$CASSANDRA_LISTEN_ADDRESS"}

    if [ "$CASSANDRA_BROADCAST_ADDRESS" = 'auto' ]; then
        CASSANDRA_BROADCAST_ADDRESS="$(hostname --ip-address)"
    fi
    : ${CASSANDRA_BROADCAST_RPC_ADDRESS:=$CASSANDRA_BROADCAST_ADDRESS}

    if [ -n "${CASSANDRA_NAME:+1}" ]; then
        : ${CASSANDRA_SEEDS:="cassandra"}
    fi
    : ${CASSANDRA_SEEDS:="$CASSANDRA_BROADCAST_ADDRESS"}

    sed -ri 's/(- seeds:).*/\1 "'"$CASSANDRA_SEEDS"'"/' "$CASSANDRA_CONFIG/cassandra.yaml"

    for yaml in \
        broadcast_address \
        broadcast_rpc_address \
        cluster_name \
        endpoint_snitch \
        listen_address \
        num_tokens \
        rpc_address \
        start_rpc \
    ; do
        var="CASSANDRA_${yaml^^}"
        val="${!var}"
        if [ "$val" ]; then
            sed -ri 's/^(# )?('"$yaml"':).*/\2 '"$val"'/' "$CASSANDRA_CONFIG/cassandra.yaml"
        fi
    done

    # Set the Client encryption options in the Cassandra configuration file
    #
    # grab the line number of the 'client_encryption_options' property then iterate down through the file until the
    # first empty line is reached. This will be the end of the block containing all the properties for
    # 'client_encryption_options'.
    start_line_number=$(grep -n "client_encryption_options:" "$CASSANDRA_CONFIG/cassandra.yaml" | cut -d':' -f1)
    count=${start_line_number}
    line=$(sed "${count}q;d" "$CASSANDRA_CONFIG/cassandra.yaml")

    while [ "${line}" != "" ]
    do
      ((count++))
      line=$(sed "${count}q;d" "$CASSANDRA_CONFIG/cassandra.yaml")
    done

    end_line_number=${count}

    for key_val in \
        "enabled:true" \
        "optional:false" \
        "keystore:\/etc\/ssl\/cassandra\-server\-keystore\.jks" \
        "keystore_password:${CASSANDRA_KEYSTORE_PASSWORD}" \
        "require_client_auth:true" \
        "truststore:\/etc\/ssl\/generic\-server\-truststore\.jks" \
        "truststore_password:${CASSANDRA_TRUSTSTORE_PASSWORD}" \
        "protocol:TLS" \
        "algorithm:SunX509" \
        "store_type:JKS" \
        "cipher_suites:[TLS_RSA_WITH_AES_256_CBC_SHA, TLS_RSA_WITH_AES_128_CBC_SHA]"
    do
      property=$(echo ${key_val} | cut -d':' -f1)
      value=$(echo ${key_val} | cut -d':' -f2)
      sed -ie "${start_line_number},${end_line_number} s/\(#\ \)\{0,1\}\(${property}:\).*/\2 ${value}/" "$CASSANDRA_CONFIG/cassandra.yaml"
    done

    # Set the Rack and DC properties
    for rackdc in dc rack; do
        var="CASSANDRA_${rackdc^^}"
        val="${!var}"
        if [ "$val" ]; then
            sed -ri 's/^('"$rackdc"'=).*/\1 '"$val"'/' "$CASSANDRA_CONFIG/cassandra-rackdc.properties"
        fi
    done

    # Set JVM SSL encryption options for Cassandra
    sed -ie 's/#JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.ssl=true"/JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.ssl=true"/g' /etc/cassandra/cassandra-env.sh
    sed -ie 's/#JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.ssl.need.client.auth=true"/JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.ssl.need.client.auth=true"/g' /etc/cassandra/cassandra-env.sh
    sed -ie 's/#JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.ssl.enabled.protocols=<enabled-protocols>"/JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.ssl.enabled.protocols=TLSv1.2,TLSv1.1,TLSv1"/g' /etc/cassandra/cassandra-env.sh
    sed -ie 's/#JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.ssl.enabled.cipher.suites=<enabled-cipher-suites>"/JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.ssl.enabled.cipher.suites=TLS_RSA_WITH_AES_256_CBC_SHA"/g' /etc/cassandra/cassandra-env.sh
    sed -ie "s/#JVM_OPTS=\"\$JVM_OPTS -Djavax.net.ssl.keyStore=\/path\/to\/keystore\"/JVM_OPTS=\"\$JVM_OPTS -Djavax.net.ssl.keyStore=\/etc\/ssl\/cassandra-server-keystore\.jks\"/g" /etc/cassandra/cassandra-env.sh
    sed -ie "s/#JVM_OPTS=\"\$JVM_OPTS -Djavax.net.ssl.keyStorePassword=<keystore-password>\"/JVM_OPTS=\"\$JVM_OPTS -Djavax.net.ssl.keyStorePassword=${CASSANDRA_KEYSTORE_PASSWORD}\"/g" /etc/cassandra/cassandra-env.sh
    sed -ie "s/#JVM_OPTS=\"\$JVM_OPTS -Djavax.net.ssl.trustStore=\/path\/to\/truststore\"/JVM_OPTS=\"\$JVM_OPTS -Djavax.net.ssl.trustStore=\/etc\/ssl\/generic-server-truststore\.jks\"/g" /etc/cassandra/cassandra-env.sh
    sed -ie "s/#JVM_OPTS=\"\$JVM_OPTS -Djavax.net.ssl.trustStorePassword=<truststore-password>\"/JVM_OPTS=\"\$JVM_OPTS -Djavax.net.ssl.trustStorePassword=${CASSANDRA_TRUSTSTORE_PASSWORD}\"/g" /etc/cassandra/cassandra-env.sh
fi

exec "$@" 