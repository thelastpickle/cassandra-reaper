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

function wait_for {
  HOST=$1
  PORT=$2

  echo "Checking if ${HOST}:${PORT} is up ..."
  nc -zv ${HOST} ${PORT} > /dev/null 2>&1
  port_open=$?
  while [ "${port_open}" != 0 ]
  do
      echo "${HOST} is not yet up, will retry in 20s"
      sleep 20
      nc -zv ${HOST} ${PORT} > /dev/null 2>&1
      port_open=$?
  done
  echo "${HOST}:${PORT} is up!"
}

if [ "$1" = 'cassandra-reaper' ]; then

    if [ -z "$REAPER_HEAP_SIZE" ]; then
        REAPER_HEAP_SIZE="1G"
    fi

    cp /etc/cassandra-reaper/cassandra-reaper.yml /etc/cassandra-reaper/config/cassandra-reaper.yml

    /usr/local/bin/configure-authentication.sh
    /usr/local/bin/configure-persistence.sh
    /usr/local/bin/configure-metrics.sh
    /usr/local/bin/configure-jmx-credentials.sh
    exec java \
            ${JAVA_OPTS} \
            -Xms${REAPER_HEAP_SIZE} \
            -Xmx${REAPER_HEAP_SIZE} \
            -Djava.io.tmpdir=${REAPER_TMP_DIRECTORY} \
            -cp "/usr/local/lib/*" io.cassandrareaper.ReaperApplication server \
            /etc/cassandra-reaper/config/cassandra-reaper.yml
fi

if [ "$1" = 'schema-migration' ]; then

    cp /etc/cassandra-reaper/cassandra-reaper.yml /etc/cassandra-reaper/config/cassandra-reaper.yml

    /usr/local/bin/configure-authentication.sh
    /usr/local/bin/configure-persistence.sh
    /usr/local/bin/configure-metrics.sh
    /usr/local/bin/configure-jmx-credentials.sh
    exec java \
            ${JAVA_OPTS} \
            -Djava.io.tmpdir=${REAPER_TMP_DIRECTORY} \
            -cp "/usr/local/lib/*" io.cassandrareaper.ReaperApplication schema-migration \
            /etc/cassandra-reaper/config/cassandra-reaper.yml
fi

if [ "$1" = 'register-clusters' ]; then

  if [ -z "$2" ]; then
    echo "The register-clusters command needs its 1st argument to be a 'host:port,...' string pointing to C* nodes to repair"
    exit 1
  fi
  REAPER_AUTO_SCHEDULING_SEEDS=$2

  if [ -z "$3" ]; then
    echo "The register-clusters command needs additional argument indicating the Reaper host"
    exit 1
  fi
  REAPER_HOST=$3

  if [ -z "$4" ]; then
    echo "The register-clusters command takes additional argument indicating the Reaper port. Defaulting to 8080"
    REAPER_PORT=8080
  else
    REAPER_PORT=$4
  fi

  if [ -z "$REAPER_AUTH_USER" ]; then
    echo "The register-clusters command did not find a value for the REAPER_AUTH_USER environment variable. Defaulting to the admin user."
    USERNAME="admin"
  else
    USERNAME=$REAPER_AUTH_USER
  fi

if [ -z "$REAPER_AUTH_PASSWORD" ]; then
    echo "The register-clusters command did not find a value for the REAPER_AUTH_PASSWORD environment variable. Defaulting to the default admin password."
    PASSWORD="admin"
  else
    PASSWORD=$REAPER_AUTH_PASSWORD
  fi

  mkdir -p ~/.reaper
  echo ${PASSWORD} > ~/.reaper/credentials

  wait_for ${REAPER_HOST} ${REAPER_PORT}

  for SEED in $(echo "${REAPER_AUTO_SCHEDULING_SEEDS}" | sed "s/,/ /g"); do
    SEED_HOST=$(echo ${SEED} | cut -d':' -f1)
    SEED_PORT=$(echo ${SEED} | cut -d':' -f2)
    wait_for ${SEED_HOST} ${SEED_PORT}
    /usr/local/bin/spreaper login --reaper-host "${REAPER_HOST}" --reaper-port "${REAPER_PORT}" $USERNAME
    /usr/local/bin/spreaper add-cluster --reaper-host "${REAPER_HOST}" --reaper-port "${REAPER_PORT}" "${SEED_HOST}" "${SEED_PORT}"
  done

  exit 0

fi

exec "$@"
