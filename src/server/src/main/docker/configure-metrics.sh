#!/bin/sh

if [ "true" = "${REAPER_METRICS_ENABLED}" ]; then
cat <<EOT >> /etc/cassandra-reaper.yml
metrics:
  frequency: ${REAPER_METRICS_FREQUENCY}
  reporters: ${REAPER_METRICS_REPORTERS}
EOT
fi