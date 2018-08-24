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

if [ "$1" = 'cassandra-reaper' ]; then
    set -x

    # get around `/usr/local/bin/configure-persistence.sh: line 65: can't create /etc/cassandra-reaper.yml: Interrupted system call` unknown error
    touch /etc/cassandra-reaper.yml

    su-exec reaper /usr/local/bin/configure-persistence.sh
    su-exec reaper /usr/local/bin/configure-webui-authentication.sh
    su-exec reaper /usr/local/bin/configure-metrics.sh
    exec su-exec reaper java \
                    ${JAVA_OPTS} \
                    -jar /usr/local/lib/cassandra-reaper.jar server \
                    /etc/cassandra-reaper.yml
fi

exec "$@"
