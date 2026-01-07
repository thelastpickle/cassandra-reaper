#!/bin/bash
# Copyright 2025-2025 DataStax Inc.
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

# Add optional read-only user if environment variables are set
if [ -z "${REAPER_SERVER_TLS_ENABLE}" ]; then
cat <<EOT >> /etc/cassandra-reaper/config/cassandra-reaper.yml
server:
  type: default
  applicationConnectors:
    - type: http
      port: ${REAPER_SERVER_APP_PORT}
      bindHost: ${REAPER_SERVER_APP_BIND_HOST}
  adminConnectors:
    - type: http
      port: ${REAPER_SERVER_ADMIN_PORT}
      bindHost: ${REAPER_SERVER_ADMIN_BIND_HOST}
  requestLog:
    appenders: []
EOT
else
cat <<EOT >> /etc/cassandra-reaper/config/cassandra-reaper.yml
server:
  type: default
  applicationConnectors:
    - type: https
      port: ${REAPER_SERVER_APP_PORT}
      bindHost: ${REAPER_SERVER_APP_BIND_HOST}
      keyStorePath: ${REAPER_SERVER_TLS_KEYSTORE_PATH}
      keyStorePassword: changeit
      trustStorePath: ${REAPER_SERVER_TLS_TRUSTSTORE_PATH}
      trustStorePassword: changeit
      needClientAuth: ${REAPER_SERVER_TLS_CLIENT_AUTH}
      disableSniHostCheck: ${REAPER_SERVER_TLS_DISABLE_SNI}
  adminConnectors:
    - type: http
      port: ${REAPER_SERVER_ADMIN_PORT}
      bindHost: ${REAPER_SERVER_ADMIN_BIND_HOST}
  requestLog:
    appenders: []
EOT
fi