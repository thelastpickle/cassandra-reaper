#!/usr/bin/env bash
# Copyright 2017-2017 Spotify AB
# Copyright 2017-2018 The Last Pickle Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -xe

CQLSH_OPTS=""

# Check if SSL encryption is enabled
if [ "true" = "${CQLSH_ENABLE_SSL}" ]; then
    CASSANDRA_KEYSTORE=/etc/ssl/cassandra-server-keystore.jks
    PKCS12_KEYSTORE=/etc/ssl/cassandra-cql-keystore.p12
    CQL_CERT_PEM=/etc/ssl/cql-cer.pem
    CQL_KEY_PEM=/etc/ssl/cql-key.pem
    CQLSH_RC=.cqlshrc

    PKCS12_KEYSTORE_USERNAME=cassandra
    PKCS12_KEYSTORE_PASSWORD=keypassword

    # convert cassandra's key store into the PKCS format
    keytool -importkeystore \
        -srckeystore ${CASSANDRA_KEYSTORE} -srcalias ${CASSANDRA_KEYSTORE_ALIAS} \
        -srcstorepass ${CASSANDRA_KEYSTORE_PASSWORD} \
        -destkeystore ${PKCS12_KEYSTORE} -deststoretype PKCS12 -destalias ${PKCS12_KEYSTORE_USERNAME} \
        -deststorepass ${PKCS12_KEYSTORE_PASSWORD}

    # extract key and cert from the PKCS and place them in their own files
    openssl pkcs12 -in ${PKCS12_KEYSTORE} -nokeys -out ${CQL_CERT_PEM} -passin pass:${PKCS12_KEYSTORE_PASSWORD}
    openssl pkcs12 -in ${PKCS12_KEYSTORE} -nocerts -nodes -out ${CQL_KEY_PEM} -passin pass:${PKCS12_KEYSTORE_PASSWORD}

    # make cqlsh expect the generated key and cert files
    cat <<EOT >> ${CQLSH_RC}
[authentication]
username = ${PKCS12_KEYSTORE_USERNAME}
password = ${PKCS12_KEYSTORE_PASSWORD}

[connection]
factory = cqlshlib.ssl.ssl_transport_factory

[ssl]
certfile = /etc/ssl/ca-cert
validate = true
userkey = ${CQL_KEY_PEM}
usercert = ${CQL_CERT_PEM}
EOT

    cat ${CQLSH_RC}

    CQLSH_OPTS="--cqlshrc ${CQLSH_RC} --ssl"
fi

# Check if an CQLSH command has been specified, if so run it and then exit.
if [ ! "${CQLSH_COMMAND}" = "" ]
then
    cqlsh ${CASSANDRA_HOSTNAME} ${CQLSH_OPTS} -e "${CQLSH_COMMAND}"
    exit
fi

cqlsh ${CASSANDRA_HOSTNAME} ${CQLSH_OPTS}
