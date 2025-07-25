# Copyright 2017-2017 Spotify AB
# Copyright 2017-2019 The Last Pickle Ltd
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

FROM amazoncorretto:11-alpine

ARG SHADED_JAR

ENV REAPER_SEGMENT_COUNT_PER_NODE=64 \
    REAPER_REPAIR_PARALELLISM=DATACENTER_AWARE \
    REAPER_REPAIR_INTENSITY=0.9 \
    REAPER_MAX_PENDING_COMPACTIONS=20 \
    REAPER_SCHEDULE_DAYS_BETWEEN=7 \
    REAPER_REPAIR_RUN_THREADS=15 \
    REAPER_HANGING_REPAIR_TIMEOUT_MINS=30 \
    REAPER_STORAGE_TYPE=memory \
    REAPER_ENABLE_CROSS_ORIGIN=true \
    REAPER_INCREMENTAL_REPAIR=false \
    REAPER_SUBRANGE_INCREMENTAL=false \
    REAPER_BLACKLIST_TWCS=false \
    REAPER_ENABLE_DYNAMIC_SEED_LIST=true \
    REAPER_REPAIR_MANAGER_SCHEDULING_INTERVAL_SECONDS=30 \
    REAPER_JMX_CONNECTION_TIMEOUT_IN_SECONDS=20 \
    REAPER_USE_ADDRESS_TRANSLATOR=false \
    REAPER_DATACENTER_AVAILABILITY=ALL \
    REAPER_AUTO_SCHEDULING_ENABLED=false \
    REAPER_AUTO_SCHEDULING_INITIAL_DELAY_PERIOD=PT15S \
    REAPER_AUTO_SCHEDULING_PERIOD_BETWEEN_POLLS=PT10M \
    REAPER_AUTO_SCHEDULING_TIME_BEFORE_FIRST_SCHEDULE=PT5M \
    REAPER_AUTO_SCHEDULING_SCHEDULE_SPREAD_PERIOD=PT6H \
    REAPER_AUTO_SCHEDULING_ADAPTIVE=true \
    REAPER_AUTO_SCHEDULING_INCREMENTAL=false \
    REAPER_AUTO_SCHEDULING_PERCENT_UNREPAIRED_THRESHOLD=10 \
    REAPER_AUTO_SCHEDULING_EXCLUDED_CLUSTERS="[]" \
    REAPER_AUTO_SCHEDULING_EXCLUDED_KEYSPACES="[]" \
    REAPER_JMX_PORTS="{}" \
    REAPER_JMX_AUTH_USERNAME="" \
    REAPER_JMX_AUTH_PASSWORD="" \
    REAPER_JMX_CREDENTIALS="" \
    REAPER_LOGGING_ROOT_LEVEL=INFO \
    REAPER_LOGGING_LOGGERS="{}" \
    REAPER_LOGGING_APPENDERS_CONSOLE_LOG_FORMAT='"%-6level [%d] [%t] %logger{5} - %msg %n"' \
    REAPER_LOGGING_APPENDERS_CONSOLE_THRESHOLD=INFO \
    REAPER_SERVER_APP_PORT=8080 \
    REAPER_SERVER_APP_BIND_HOST="0.0.0.0" \
    REAPER_SERVER_ADMIN_PORT=8081 \
    REAPER_SERVER_ADMIN_BIND_HOST="0.0.0.0" \
    REAPER_CASS_ACTIVATE_QUERY_LOGGER=false \
    REAPER_CASS_CLUSTER_NAME="clustername" \
    REAPER_CASS_CONTACT_POINTS="" \
    REAPER_CASS_KEYSPACE="reaper_db" \
    REAPER_CASS_LOCAL_DC="" \
    REAPER_CASS_AUTH_ENABLED="false" \
    REAPER_CASS_AUTH_USERNAME="cassandra" \
    REAPER_CASS_AUTH_PASSWORD="cassandra" \
    REAPER_CASS_NATIVE_PROTOCOL_SSL_ENCRYPTION_ENABLED="false" \
    REAPER_CASS_SCHEMA_AGREEMENT_INTERVAL="2000" \
    REAPER_CASS_SCHEMA_AGREEMENT_TIMEOUT="10" \
    REAPER_CASS_REQUEST_TIMEOUT="10s" \
    REAPER_DB_URL="" \
    REAPER_DB_USERNAME="" \
    REAPER_DB_PASSWORD="" \
    REAPER_METRICS_ENABLED=false \
    REAPER_METRICS_FREQUENCY="1 minute" \
    REAPER_METRICS_REPORTERS="[]" \
    REAPER_AUTH_ENABLED="true" \
    REAPER_AUTH_USER="" \
    REAPER_AUTH_PASSWORD="" \
    REAPER_READ_USER="" \
    REAPER_READ_USER_PASSWORD="" \
    JAVA_OPTS="" \
    REAPER_MAX_PARALLEL_REPAIRS=2 \
    REAPER_MGMT_API_METRICS_PORT=9000 \
    REAPER_HTTP_MANAGEMENT_ENABLE="false" \
    REAPER_HTTP_MANAGEMENT_KEYSTORE_PATH="" \
    REAPER_HTTP_MANAGEMENT_TRUSTSTORE_PATH="" \
    REAPER_HTTP_MANAGEMENT_TRUSTSTORES_DIR="" \
    REAPER_TMP_DIRECTORY="/var/tmp/cassandra-reaper" \
    REAPER_MEMORY_STORAGE_DIRECTORY="/var/lib/cassandra-reaper/storage" \
    REAPER_PURGE_RECORDS_AFTER_IN_DAYS=30


RUN apk add --update \
    bash \
  && rm -rf /var/cache/apk/*

ADD src/server/src/main/docker/cassandra-reaper.yml /etc/cassandra-reaper/cassandra-reaper.yml
ADD src/server/src/main/docker/entrypoint.sh /usr/local/bin/entrypoint.sh
ADD src/server/src/main/docker/configure-persistence.sh /usr/local/bin/configure-persistence.sh
ADD src/server/src/main/docker/configure-metrics.sh /usr/local/bin/configure-metrics.sh
ADD src/server/src/main/docker/configure-jmx-credentials.sh /usr/local/bin/configure-jmx-credentials.sh
ADD src/server/src/main/docker/configure-authentication.sh /usr/local/bin/configure-authentication.sh
ADD ${SHADED_JAR} /usr/local/lib/cassandra-reaper.jar


RUN addgroup -g 1001 -S reaper && adduser -G reaper -S -u 1001 reaper && \
    mkdir -p /var/lib/cassandra-reaper && \
    mkdir -p /etc/cassandra-reaper/shiro && \
    mkdir -p /etc/cassandra-reaper/config && \
    mkdir -p /var/log/cassandra-reaper && \
    mkdir -p ${REAPER_TMP_DIRECTORY} && \
    chown reaper:reaper \
        /etc/cassandra-reaper/cassandra-reaper.yml \
        /usr/local/lib/cassandra-reaper.jar \
        /usr/local/bin/entrypoint.sh \
        /usr/local/bin/configure-persistence.sh \
        /usr/local/bin/configure-metrics.sh \
        /usr/local/bin/configure-jmx-credentials.sh \
        /usr/local/bin/configure-authentication.sh \
        ${REAPER_TMP_DIRECTORY} && \
    chown -R reaper:reaper \
        /var/lib/cassandra-reaper \
        /etc/cassandra-reaper/shiro \
        /etc/cassandra-reaper/config \
        /var/log/cassandra-reaper && \
    chmod +x \
        /usr/local/bin/entrypoint.sh \
        /usr/local/bin/configure-persistence.sh \
        /usr/local/bin/configure-metrics.sh \
        /usr/local/bin/configure-jmx-credentials.sh \
        /usr/local/bin/configure-authentication.sh

VOLUME /var/lib/cassandra-reaper
VOLUME /etc/cassandra-reaper/shiro
VOLUME /etc/cassandra-reaper/config

USER reaper

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["cassandra-reaper"]