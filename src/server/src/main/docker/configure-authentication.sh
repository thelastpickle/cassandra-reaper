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

# Add optional read-only user if environment variables are set
if [ -n "${REAPER_READ_USER}" ] && [ -n "${REAPER_READ_USER_PASSWORD}" ]; then
cat <<EOT >> /etc/cassandra-reaper/config/cassandra-reaper.yml
    # Read-only user configured via environment variables
    - username: "$(echo "${REAPER_READ_USER}" | sed 's/"/\\"/g')"
      password: "$(echo "${REAPER_READ_USER_PASSWORD}" | sed 's/"/\\"/g')"
      roles: ["user"]
EOT
fi 