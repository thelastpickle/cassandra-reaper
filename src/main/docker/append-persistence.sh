#!/bin/sh

case ${REAPER_STORAGE_TYPE} in
    "cassandra")
cat <<EOT >> /etc/cassandra-reaper.yml
cassandra:
  clusterName: ${REAPER_CASS_CLUSTER_NAME}
  contactPoints: ${REAPER_CASS_CONTACT_POINTS}
  keyspace: ${REAPER_CASS_KEYSPACE}
EOT

if [ "true" = "${REAPER_CASS_AUTH_ENABLED}" ]; then
cat <<EOT >> /etc/cassandra-reaper.yml
  authProvider:
    type: plainText
    username: ${REAPER_CASS_AUTH_USERNAME}
    password: ${REAPER_CASS_AUTH_PASSWORD}
EOT
fi
    ;;
    "database")
cat <<EOT >> /etc/cassandra-reaper.yml
database:
  driverClass: ${REAPER_DB_DRIVER_CLASS}
  url: ${REAPER_DB_URL}
  user: ${REAPER_DB_USERNAME}
  password: ${REAPER_DB_PASSWORD}
EOT

esac
