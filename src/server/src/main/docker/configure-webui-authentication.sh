#!/bin/sh
# Copyright 2018-2018 The Last Pickle Ltd
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

if [ ! -z "${REAPER_SHIRO_INI}" ]; then
cat <<EOT >> /etc/cassandra-reaper.yml
accessControl:
  sessionTimeout: PT10M
  shiro:
    iniConfigs: ["file:${REAPER_SHIRO_INI}"]
EOT
elif [ ! -z "${REAPER_AUTH_USER}" ]; then
cat <<EOT >> /etc/cassandra-reaper.yml
accessControl:
  sessionTimeout: PT10M
  shiro:
    iniConfigs: ["file:/etc/shiro.ini"]
EOT
else
cat <<EOT >> /etc/cassandra-reaper.yml
accessControl:
  sessionTimeout: PT10M
  shiro:
    iniConfigs: ["classpath:shiro.ini"]
EOT
fi

if [ ! -z "${REAPER_AUTH_USER}" ]; then
cat <<EOT2 >> /etc/shiro.ini
${REAPER_AUTH_USER} = ${REAPER_AUTH_PASSWORD}, operator
EOT2
fi
