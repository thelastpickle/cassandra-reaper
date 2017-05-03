#!/bin/sh

if [ "$1" = 'cassandra-reaper' ]; then
    su-exec reaper /usr/local/bin/append-persistence.sh
    exec su-exec reaper java -jar ${JAVA_OPTS} \
        /usr/local/lib/cassandra-reaper.jar server /etc/cassandra-reaper.yml
fi

exec "$@"