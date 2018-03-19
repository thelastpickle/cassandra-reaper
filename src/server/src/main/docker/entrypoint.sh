#!/bin/sh

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
