#!/bin/sh

case ${REAPER_STORAGE_TYPE} in
    "cassandra")

# BEGIN cassandra persistence options
cat <<EOT >> /etc/cassandra-reaper.yml
activateQueryLogger: ${REAPER_CASS_ACTIVATE_QUERY_LOGGER}

cassandra:
  clusterName: ${REAPER_CASS_CLUSTER_NAME}
  contactPoints: ${REAPER_CASS_CONTACT_POINTS}
  keyspace: ${REAPER_CASS_KEYSPACE}
  loadBalancingPolicy:
    type: tokenAware
    shuffleReplicas: true
    subPolicy:
      type: dcAwareRoundRobin
      localDC: ${REAPER_CASS_LOCAL_DC}
      usedHostsPerRemoteDC: 0
      allowRemoteDCsForLocalConsistencyLevel: false
EOT

if [ "true" = "${REAPER_CASS_AUTH_ENABLED}" ]; then
cat <<EOT >> /etc/cassandra-reaper.yml
  authProvider:
    type: plainText
    username: ${REAPER_CASS_AUTH_USERNAME}
    password: ${REAPER_CASS_AUTH_PASSWORD}
EOT
fi

if [ "true" = "${REAPER_CASS_NATIVE_PROTOCOL_SSL_ENCRYPTION_ENABLED}" ]; then
cat <<EOT >> /etc/cassandra-reaper.yml
  ssl:
    type: jdk
EOT
fi
# END cassandra persistence options

    ;;
    "database")

# BEGIN database persistence options
cat <<EOT >> /etc/cassandra-reaper.yml
database:
  driverClass: ${REAPER_DB_DRIVER_CLASS}
  url: ${REAPER_DB_URL}
  user: ${REAPER_DB_USERNAME}
  password: ${REAPER_DB_PASSWORD}
EOT
# END database persistence options

esac
