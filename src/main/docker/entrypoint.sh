#!/bin/sh

if [ "$1" = 'cassandra-reaper' ]; then
    /root/append-persistence.sh
    exec java -jar ${JAVA_OPTS} /root/cassandra-reaper.jar server /root/cassandra-reaper.yml
fi

exec "$@"