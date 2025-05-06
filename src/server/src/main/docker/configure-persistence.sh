#!/bin/bash
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

# Add specific jmxAddressTranslator
if [ ! -z "${JMX_ADDRESS_TRANSLATOR_TYPE}" ]; then
cat <<EOT >> /etc/cassandra-reaper/config/cassandra-reaper.yml
jmxAddressTranslator:
  type: ${JMX_ADDRESS_TRANSLATOR_TYPE}
EOT
fi

if [ "multiIpPerNode" = "${JMX_ADDRESS_TRANSLATOR_TYPE}" ] && [ -n "$JMX_ADDRESS_TRANSLATOR_MAPPING" ]; then
cat <<EOT >> /etc/cassandra-reaper/config/cassandra-reaper.yml
  ipTranslations:
EOT
IFS=',' read -ra mappings <<< "$JMX_ADDRESS_TRANSLATOR_MAPPING"
for mapping in "${mappings[@]}"; do
IFS=':' read -ra mapping <<< "$mapping"
cat <<EOT >> /etc/cassandra-reaper/config/cassandra-reaper.yml
    - from: "${mapping[0]}"
      to: "${mapping[1]}"
EOT
done
fi

case ${REAPER_STORAGE_TYPE} in
    "cassandra")

# BEGIN cassandra persistence options
cat <<EOT >> /etc/cassandra-reaper/config/cassandra-reaper.yml
activateQueryLogger: ${REAPER_CASS_ACTIVATE_QUERY_LOGGER}

cassandra:
  type: basic
  sessionName: ${REAPER_CASS_CLUSTER_NAME}
  contactPoints: ${REAPER_CASS_CONTACT_POINTS}
  sessionKeyspaceName: ${REAPER_CASS_KEYSPACE}
  loadBalancingPolicy:
    type: default
    localDataCenter: ${REAPER_CASS_LOCAL_DC}
  retryPolicy:
    type: default
  schemaOptions:
    agreementIntervalMilliseconds: ${REAPER_CASS_SCHEMA_AGREEMENT_INTERVAL}
    agreementTimeoutSeconds: ${REAPER_CASS_SCHEMA_AGREEMENT_TIMEOUT}
    agreementWarnOnFailure: true
  requestOptionsFactory:
    requestTimeout: ${REAPER_CASS_REQUEST_TIMEOUT}
    requestDefaultIdempotence: true

EOT

if [ "true" = "${REAPER_CASS_AUTH_ENABLED}" ]; then
cat <<EOT >> /etc/cassandra-reaper/config/cassandra-reaper.yml
  authProvider:
    type: plain-text
    username: "$(echo "${REAPER_CASS_AUTH_USERNAME}" | sed 's/"/\\"/g')"
    password: "$(echo "${REAPER_CASS_AUTH_PASSWORD}" | sed 's/"/\\"/g')"
EOT
fi

if [ "true" = "${REAPER_CASS_NATIVE_PROTOCOL_SSL_ENCRYPTION_ENABLED}" ]; then
cat <<EOT >> /etc/cassandra-reaper/config/cassandra-reaper.yml
  ssl:
    type: jdk
EOT
fi

if [ "true" = "${REAPER_CASS_ADDRESS_TRANSLATOR_ENABLED}" ]; then
cat <<EOT >> /etc/cassandra-reaper/config/cassandra-reaper.yml
  addressTranslator:
    type: ${REAPER_CASS_ADDRESS_TRANSLATOR_TYPE}
EOT
fi

if [ "multiIpPerNode" = "${REAPER_CASS_ADDRESS_TRANSLATOR_TYPE}" ] && [ -n "$REAPER_CASS_ADDRESS_TRANSLATOR_MAPPING" ]; then
cat <<EOT >> /etc/cassandra-reaper/config/cassandra-reaper.yml
    ipTranslations:
EOT
IFS=',' read -ra mappings <<< "$REAPER_CASS_ADDRESS_TRANSLATOR_MAPPING"
for mapping in "${mappings[@]}"; do
IFS=':' read -ra mapping <<< "$mapping"
cat <<EOT >> /etc/cassandra-reaper/config/cassandra-reaper.yml
    - from: "${mapping[0]}"
      to: "${mapping[1]}"
EOT
done
fi

# END cassandra persistence options

    ;;
    "memory")
# BEGIN cassandra persistence options
cat <<EOT >> /etc/cassandra-reaper/config/cassandra-reaper.yml
persistenceStoragePath: ${REAPER_MEMORY_STORAGE_DIRECTORY}

EOT
    ;;
esac
