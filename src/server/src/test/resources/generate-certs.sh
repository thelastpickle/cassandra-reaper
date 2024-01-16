#!/usr/bin/env bash

# Copied from management-api, modified for reaper to create JKS files also

# Generate a new, self-signed root CA
openssl req -extensions v3_ca -new -x509 -days 36500 -nodes -subj "/CN=NettyTestRoot" -newkey rsa:2048 -sha512 -out mutual_auth_ca.pem -keyout mutual_auth_ca.key

# Generate a certificate/key for the server
openssl req -new -keyout mutual_auth_server.key -nodes -newkey rsa:2048 -subj "/CN=NettyTestServer" | \
openssl x509 -req -CAkey mutual_auth_ca.key -CA mutual_auth_ca.pem -days 36500 -set_serial $RANDOM -sha512 -out mutual_auth_server.crt

# Generate a valid intermediate CA which will be used to sign the client certificate
openssl req -new -keyout mutual_auth_intermediate_ca.key -nodes -newkey rsa:2048 -out mutual_auth_intermediate_ca.key
openssl req -new -sha512 -key mutual_auth_intermediate_ca.key -subj "/CN=NettyTestIntermediate" -out intermediate.csr
openssl x509 -req -days 1825 -in intermediate.csr -extfile openssl.cnf -extensions v3_ca -CA mutual_auth_ca.pem -CAkey mutual_auth_ca.key -set_serial $RANDOM -out mutual_auth_intermediate_ca.pem

# Generate a client certificate signed by the intermediate CA
openssl req -new -keyout mutual_auth_client.key -nodes -newkey rsa:2048 -subj "/CN=NettyTestClient/UID=Client" | \
openssl x509 -req -CAkey mutual_auth_intermediate_ca.key -CA mutual_auth_intermediate_ca.pem -days 36500 -set_serial $RANDOM -sha512 -out mutual_auth_client.crt

# Append
cat mutual_auth_intermediate_ca.pem mutual_auth_ca.pem > mutual_auth_client_cert_chain.pem

# Modify to PKCS12 and JKS for Reaper (use password changeit)
openssl pkcs12 -export -out mutual_auth_client_cert_chain.pkcs12 -inkey mutual_auth_intermediate_ca.key -in mutual_auth_client_cert_chain.pem
openssl pkcs12 -export -out mutual_auth_client.pkcs12 -inkey mutual_auth_client.key -in mutual_auth_client.crt

# Use password changeit
keytool -importkeystore -srckeystore mutual_auth_client_cert_chain.pkcs12 -srcstoretype pkcs12 -destkeystore truststore.jks -deststoretype JKS
keytool -importkeystore -srckeystore mutual_auth_client.pkcs12 -srcstoretype pkcs12 -destkeystore keystore.jks -deststoretype JKS
