#!/bin/sh
# Copyright 2018-2019 The Last Pickle Ltd
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

if [ "false" = "${REAPER_AUTH_ENABLED}" ]; then
  exit 0
fi

if [ ! -z "${REAPER_AUTH_USER}" ]; then
  _ACTUAL_REAPER_SHIRO_INI_="${REAPER_SHIRO_INI:-/etc/cassandra-reaper/config/shiro.ini}"
  cat <<EOT >> "${_ACTUAL_REAPER_SHIRO_INI_}"
${REAPER_AUTH_USER} = ${REAPER_AUTH_PASSWORD}, operator
EOT
  _REAPER_SHIRO_INI_PATH_="file:${_ACTUAL_REAPER_SHIRO_INI_}"
else
  _REAPER_SHIRO_INI_PATH_="classpath:shiro.ini"
fi

cat <<EOT2 >> /etc/cassandra-reaper/config/cassandra-reaper.yml
accessControl:
  sessionTimeout: PT10M
  shiro:
    iniConfigs: ["${_REAPER_SHIRO_INI_PATH_}"]
EOT2
