#!/bin/sh

case ${DW_REAPER_STORAGE_TYPE} in
    "cassandra")
cat <<EOT >> /root/cassandra-reaper.yml
cassandra:
  clusterName: ${DW_REAPER_CASS_CLUSTER_NAME}
  contactPoints: ${DW_REAPER_CASS_CONTACT_POINTS}
  keyspace: ${DW_REAPER_CASS_KEYSPACE}
EOT

if [ "true" = "${DW_REAPER_CASS_AUTH_ENABLED}" ]; then
cat <<EOT >> /root/cassandra-reaper.yml
  authProvider:
    type: plainText
    username: ${DW_REAPER_CASS_AUTH_USERNAME}
    password: ${DW_REAPER_CASS_AUTH_PASSWORD}
EOT
fi
    ;;
    "database")
cat <<EOT >> /root/cassandra-reaper.yml
database:
  driverClass: ${DW_REAPER_DB_DRIVER_CLASS}
  url: ${DW_REAPER_DB_URL}
  user: ${DW_REAPER_DB_USERNAME}
  password: ${DW_REAPER_DB_PASSWORD}
EOT

esac
