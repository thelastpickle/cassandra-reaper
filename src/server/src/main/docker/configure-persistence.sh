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

case ${REAPER_STORAGE_TYPE} in
    "cassandra")

# BEGIN cassandra persistence options
cat <<EOT >> /etc/cassandra-reaper.yml
activateQueryLogger: ${REAPER_CASS_ACTIVATE_QUERY_LOGGER}

cassandra:
  clusterName: ${REAPER_CASS_CLUSTER_NAME}
  contactPoints: ${REAPER_CASS_CONTACT_POINTS}
  port: ${REAPER_CASS_PORT}
  keyspace: ${REAPER_CASS_KEYSPACE}
  loadBalancingPolicy:
    type: tokenAware
    shuffleReplicas: true
    subPolicy:
      type: dcAwareRoundRobin
      localDC: ${REAPER_CASS_LOCAL_DC}
      usedHostsPerRemoteDC: 0
      allowRemoteDCsForLocalConsistencyLevel: false
EOT

if [ "true" = "${REAPER_CASS_AUTH_ENABLED}" ]; then
cat <<EOT >> /etc/cassandra-reaper.yml
  authProvider:
    type: plainText
    username: ${REAPER_CASS_AUTH_USERNAME}
    password: ${REAPER_CASS_AUTH_PASSWORD}
EOT
fi

if [ "true" = "${REAPER_CASS_NATIVE_PROTOCOL_SSL_ENCRYPTION_ENABLED}" ]; then
cat <<EOT >> /etc/cassandra-reaper.yml
  ssl:
    type: jdk
EOT
fi

if [ "true" = "${REAPER_CASS_ADDRESS_TRANSLATOR_ENABLED}" ]; then
cat <<EOT >> /etc/cassandra-reaper.yml
  addressTranslator:
    type: ${REAPER_CASS_ADDRESS_TRANSLATOR_TYPE}
EOT
fi
# END cassandra persistence options

    ;;
    "postgres")

# BEGIN postgres persistence options
cat <<EOT >> /etc/cassandra-reaper.yml
postgres:
  url: ${REAPER_DB_URL}
  user: ${REAPER_DB_USERNAME}
  password: ${REAPER_DB_PASSWORD}
EOT
# END postgres persistence options

    ;;
    "h2")

# BEGIN h2 persistence options
cat <<EOT >> /etc/cassandra-reaper.yml
h2:
  url: ${REAPER_DB_URL}
  user: ${REAPER_DB_USERNAME}
  password: ${REAPER_DB_PASSWORD}
EOT
# END h2 persistence options

esac

