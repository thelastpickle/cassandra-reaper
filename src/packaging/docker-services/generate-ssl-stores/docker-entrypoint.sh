#!/bin/sh
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

set -ex

#
# Generate the Key Stores and Trust Store to use when SSL encrypting communications between Cassandra and Reaper. In an
#   SSL handshake, the purpose of a Truststore is to verify credentials and purpose of a Keystore is to provide the
#   credentials. The credentials are derived from the Root Certificate Authority (Root CA).
#
# The Root CA that is generated from the Certificate Authority Configuration file is the core component of SSL
#   encryption. The CA is used to sign other certificates, thus forming a certificate pair with the signed certificate.
#
# The Truststore contains the Root CA, and is used to determine whether the certificate from another party is to be
#   trusted. That is, it is used to verify credentials from a third party. If the certificate from a third party were
#   signed by the Root CA then the remote party can be trusted.
#
# The Keystore contains a certificate generated from the store and signed by the Root CA, and the Root CA used to sign
#   the certificate. The Keystore determines which authentication certificate to send to the remote host and provide
#   those when establishing the connection.
#

CASSANDRA_KEYSTORE_PASSWORD=$1
CASSANDRA_TRUSTSTORE_PASSWORD=$2

#
# Use three separate stores:
#   - The Cassandra Keystore that will contain the Cassandra private certificate.
#   - The Reaper Keystore that will contain the Reaper private certificate.
#   - The Generic Truststore that is will contain the Root CA used to sign the private certificates in the
#       Cassandra and Reaper Keystores.
#
CASSANDRA_KEYSTORE=${WORKDIR}/ssl-stores/cassandra-server-keystore.jks
REAPER_KEYSTORE=${WORKDIR}/ssl-stores/reaper-server-keystore.jks
GENERIC_TRUSTSTORE=${WORKDIR}/ssl-stores/generic-server-truststore.jks

CA_CERT_CONFIG=${WORKDIR}/ca_cert.conf
ROOT_CA_CERT=${WORKDIR}/ssl-stores/ca-cert

# Create the directory where the stores will go into if required.
mkdir -p ${WORKDIR}/ssl-stores/

# Check if there are any of the SSL stores exists and if so, prompt the user to delete them or exit
set +x
if [[ $(ls ${WORKDIR}/ssl-stores/*.jks | wc -l) -gt 0 ]]
then
    echo
    echo "WARNING: If any of the following stores exist, they will need to be deleted to proceed with the generation of new SSL stores."
    echo " - ${CASSANDRA_KEYSTORE}"
    echo " - ${REAPER_KEYSTORE}"
    echo " - ${GENERIC_TRUSTSTORE}"
    echo
    while true
    do
        read -p "Do you wish to delete the above stores if they exist and continue with the generation of new SSL stores [Y/n]?" yn
        case $yn in
            [Yy]* ) break;;
            [Nn]* ) exit;;
            * ) echo "Please answer [Y]es or [n]o.";;
        esac
    done

fi
set -x

for store_name in ${CASSANDRA_KEYSTORE} ${REAPER_KEYSTORE} ${GENERIC_TRUSTSTORE}
do
    rm -f ${store_name}
done

set +x
echo
echo "Generic Certificate Authority configuration"
cat ${CA_CERT_CONFIG}
echo
set -x

# Create the Root Certificate Authority (Root CA) from the Certificate Authority Configuration and verify contents.
openssl req -config ${CA_CERT_CONFIG} -new -x509 -keyout ca-key -out ${ROOT_CA_CERT}
openssl x509 -in ${ROOT_CA_CERT} -text -noout

# Generate public/private key pair and the key stores.
keytool -genkeypair -keyalg RSA -alias cassandra \
        -keystore ${CASSANDRA_KEYSTORE} -storepass ${CASSANDRA_KEYSTORE_PASSWORD} \
        -keypass ${CASSANDRA_KEYSTORE_PASSWORD} -keysize 2048 \
        -dname "CN=node, OU=SSL-verification-cluster, O=TheLastPickle, C=AU"
keytool -genkeypair -keyalg RSA -alias reaper \
        -keystore ${REAPER_KEYSTORE} -storepass ${CASSANDRA_KEYSTORE_PASSWORD} \
        -keypass ${CASSANDRA_KEYSTORE_PASSWORD} -keysize 2048 \
        -dname "CN=reaper, OU=SSL-verification-cluster, O=TheLastPickle, C=AU"

# Export certificates from key stores as a 'Signing Request' which the Root CA can then sign.
keytool -keystore ${CASSANDRA_KEYSTORE} -alias cassandra -certreq -file cassandra_cert_sr \
        -keypass ${CASSANDRA_KEYSTORE_PASSWORD} -storepass ${CASSANDRA_KEYSTORE_PASSWORD}
keytool -keystore ${REAPER_KEYSTORE} -alias reaper -certreq -file reaper_cert_sr \
        -keypass ${CASSANDRA_KEYSTORE_PASSWORD} -storepass ${CASSANDRA_KEYSTORE_PASSWORD}

# Sign each of the certificates using the Root CA.
openssl x509 -req -CA ${ROOT_CA_CERT} -CAkey ca-key -in cassandra_cert_sr -out cassandra_cert_signed -CAcreateserial -passin pass:mypass
openssl x509 -req -CA ${ROOT_CA_CERT} -CAkey ca-key -in reaper_cert_sr -out reaper_cert_signed -CAcreateserial -passin pass:mypass

# Import the the Root CA into the key stores.
keytool -keystore ${CASSANDRA_KEYSTORE} -alias CARoot -import -file ${ROOT_CA_CERT} -noprompt \
        -keypass ${CASSANDRA_KEYSTORE_PASSWORD} -storepass ${CASSANDRA_KEYSTORE_PASSWORD}
keytool -keystore ${REAPER_KEYSTORE} -alias CARoot -import -file ${ROOT_CA_CERT} -noprompt \
        -keypass ${CASSANDRA_KEYSTORE_PASSWORD} -storepass ${CASSANDRA_KEYSTORE_PASSWORD}

# Import the signed certificates back into the key stores so that there is a complete chain.
keytool -keystore ${CASSANDRA_KEYSTORE} -alias cassandra -import -file cassandra_cert_signed \
        -keypass ${CASSANDRA_KEYSTORE_PASSWORD} -storepass ${CASSANDRA_KEYSTORE_PASSWORD}
keytool -keystore ${REAPER_KEYSTORE} -alias reaper -import -file reaper_cert_signed \
        -keypass ${CASSANDRA_KEYSTORE_PASSWORD} -storepass ${CASSANDRA_KEYSTORE_PASSWORD}

# Create the trust store.
keytool -keystore ${GENERIC_TRUSTSTORE} -alias CARoot -importcert -file ${ROOT_CA_CERT} \
        -keypass ${CASSANDRA_TRUSTSTORE_PASSWORD} -storepass ${CASSANDRA_TRUSTSTORE_PASSWORD} -noprompt 
