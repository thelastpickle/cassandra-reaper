#!/usr/bin/env bash
# Copyright 2023-2023 DataStax Inc.
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

# Generate a new, self-signed root CA
echo "--- Generating self-signed root CA and key"
openssl req -extensions v3_ca -new -x509 -days 36500 -nodes -subj "/CN=NettyTestRoot" -newkey rsa:2048 -sha512 -out mutual_auth_ca.pem -keyout mutual_auth_ca.key

# Generate request certificate file
echo "--- Generating server certificate/key"
openssl req -new -keyout mutual_auth_server.key -nodes -newkey rsa:2048 -subj "/CN=NettyTestServer"
openssl req -new -sha256 -key mutual_auth_server.key --subj "/CN=NettyTestServer" -reqexts SAN -extensions SAN -config <(cat /etc/ssl/openssl.cnf <(printf "[SAN]\nsubjectAltName=DNS:localhost,IP:127.0.0.1,IP:127.0.0.2")) -out mutual_auth_server.csr
openssl x509 -req -days 1825 -CAkey mutual_auth_ca.key -CA mutual_auth_ca.pem -CAcreateserial -extensions SAN -extfile <(cat /etc/ssl/openssl.cnf <(printf "\n[SAN]\nsubjectAltName=DNS:localhost,IP:127.0.0.1,IP:127.0.0.2")) -in mutual_auth_server.csr -out mutual_auth_server.crt

# Generate a client certificate/key
echo "--- Generating client certificate/key"
openssl req -new -config openssl.cnf -keyout mutual_auth_client.key -nodes -newkey rsa:2048 -subj "/CN=NettyTestClient/UID=Client"  | \
openssl x509 -req -extfile openssl.cnf -extensions v3_ca -CAkey mutual_auth_ca.key -CA mutual_auth_ca.pem -days 36500 -set_serial $RANDOM -sha512 -out mutual_auth_client.crt

# Create truststore from the CA
echo "--- Creating truststore"
rm -f truststore.jks
keytool -importcert -storetype jks -alias server_auth -keystore truststore.jks -file mutual_auth_server.crt -storepass changeit -noprompt

# Create the keystore from private key
echo "--- Create client keystore"
rm -f keystore.jks
cat mutual_auth_client.key mutual_auth_ca.pem mutual_auth_client.crt > mutual_auth_client_chain.crt
openssl pkcs12 -export -in mutual_auth_client_chain.crt -out mutual_auth_client_chain.p12 -password pass:"changeit" -name mutual_auth_client -noiter -nomaciter
keytool -importkeystore -srckeystore mutual_auth_client_chain.p12 -srcstoretype pkcs12 -destkeystore keystore.jks -deststoretype JKS -storepass changeit -srcstorepass changeit
