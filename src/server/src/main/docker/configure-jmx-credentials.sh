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

# we expect the jmx credentials to be a comma-separated list of 'user:passowrd@cluster' entries
if [ ! -z "${REAPER_JMX_CREDENTIALS}" ]; then

cat <<EOT >> /etc/cassandra-reaper.yml
jmxCredentials:
EOT

  # first we split them by commas
  for ENTRY in $(echo "${REAPER_JMX_CREDENTIALS}" | sed "s/,/ /g"); do
    # and then just cut out the fields we need
    CLUSTER=$(echo "${ENTRY}" | cut -d'@' -f2)
    USERNAME=$(echo "${ENTRY}" | cut -d'@' -f1 | cut -d':' -f1)
    PASSWORD=$(echo "${ENTRY}" | cut -d'@' -f1 | cut -d':' -f2)

    # finally, write out the YAML entries
cat <<EOT >> /etc/cassandra-reaper.yml
  ${CLUSTER}:
    username: ${USERNAME}
    password: ${PASSWORD}
EOT

  done

fi


if [ ! -z "${REAPER_JMX_AUTH_USERNAME}" ]; then

cat <<EOT >> /etc/cassandra-reaper.yml
jmxAuth:
  username: ${REAPER_JMX_AUTH_USERNAME}
  password: ${REAPER_JMX_AUTH_PASSWORD}
EOT

fi

if [ ! -z "${CRYPTO_SYSTEM_PROPERTY_SECRET}" ]; then
cat <<EOT >> /etc/cassandra-reaper.yml
cryptograph:
  type: symmetric
  systemPropertySecret: ${CRYPTO_SYSTEM_PROPERTY_SECRET}
EOT
