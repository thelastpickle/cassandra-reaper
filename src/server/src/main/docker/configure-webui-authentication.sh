#!/bin/sh

if [ "true" = "${REAPER_ENABLE_WEBUI_AUTH}" ]; then
cat <<EOT >> /etc/cassandra-reaper.yml
accessControl:
  sessionTimeout: PT10M
  shiro:
    iniConfigs: ["file:${REAPER_SHIRO_INI}"]
EOT
fi

if [ "true" = "${REAPER_ENABLE_WEBUI_AUTH}" ]; then
cat <<EOT2 >> /etc/shiro.ini
${REAPER_WEBUI_USER} = ${REAPER_WEBUI_PASSWORD}
EOT2
fi