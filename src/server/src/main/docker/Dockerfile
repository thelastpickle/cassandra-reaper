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

FROM adoptopenjdk/openjdk11:alpine

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
    REAPER_CASS_CONTACT_POINTS="[]" \
    REAPER_CASS_PORT=9042 \
    REAPER_CASS_KEYSPACE="reaper_db" \
    REAPER_CASS_LOCAL_DC="" \
    REAPER_CASS_AUTH_ENABLED="false" \
    REAPER_CASS_AUTH_USERNAME="cassandra" \
    REAPER_CASS_AUTH_PASSWORD="cassandra" \
    REAPER_CASS_NATIVE_PROTOCOL_SSL_ENCRYPTION_ENABLED="false" \
    REAPER_DB_URL="" \
    REAPER_DB_USERNAME="" \
    REAPER_DB_PASSWORD="" \
    REAPER_METRICS_ENABLED=false \
    REAPER_METRICS_FREQUENCY="1 minute" \
    REAPER_METRICS_REPORTERS="[]" \
    REAPER_AUTH_ENABLED="true" \
    REAPER_AUTH_USER="" \
    REAPER_AUTH_PASSWORD="" \
    REAPER_SHIRO_INI="" \
    REAPER_JMXMP_ENABLED="false" \
    REAPER_JMXMP_SSL="false" \
    JAVA_OPTS="" \
    REAPER_MAX_PARALLEL_REPAIRS=2

# used to run spreaper cli
RUN apk add --update \
    python3 \
    py3-pip \
    bash \
  && pip3 install requests \
  && rm -rf /var/cache/apk/*


ADD cassandra-reaper.yml /etc/cassandra-reaper/cassandra-reaper.yml
ADD shiro.ini /etc/cassandra-reaper/shiro.ini
ADD entrypoint.sh /usr/local/bin/entrypoint.sh
ADD configure-persistence.sh /usr/local/bin/configure-persistence.sh
ADD configure-metrics.sh /usr/local/bin/configure-metrics.sh
ADD configure-webui-authentication.sh /usr/local/bin/configure-webui-authentication.sh
ADD configure-jmx-credentials.sh /usr/local/bin/configure-jmx-credentials.sh
ADD ${SHADED_JAR} /usr/local/lib/cassandra-reaper.jar
ADD spreaper /usr/local/bin/spreaper


# get around `/usr/local/bin/configure-persistence.sh: line 65: can't create /etc/cassandra-reaper/cassandra-reaper.yml: Interrupted system call` unknown error
RUN touch /etc/cassandra-reaper/cassandra-reaper.yml
# get around `/usr/local/bin/configure-webui-authentication.sh: line 44: can't `create` /etc/cassandra-reaper/shiro.ini: Interrupted system call` unknown error
RUN touch /etc/cassandra-reaper/shiro.ini


RUN addgroup -g 1001 -S reaper && adduser -G reaper -S -u 1001 reaper && \
    mkdir -p /var/lib/cassandra-reaper && \
    mkdir -p /etc/cassandra-reaper/shiro && \
    mkdir -p /var/log/cassandra-reaper && \
    chown reaper:reaper \
        /etc/cassandra-reaper/cassandra-reaper.yml \
        /usr/local/lib/cassandra-reaper.jar \
        /usr/local/bin/entrypoint.sh \
        /usr/local/bin/configure-persistence.sh \
        /usr/local/bin/configure-webui-authentication.sh \
        /usr/local/bin/configure-metrics.sh \
        /usr/local/bin/configure-jmx-credentials.sh \
        /usr/local/bin/spreaper \
        /etc/cassandra-reaper/shiro.ini && \
    chown -R reaper:reaper \
        /var/lib/cassandra-reaper \
        /etc/cassandra-reaper/shiro \
        /var/log/cassandra-reaper && \
    chmod +x \
        /usr/local/bin/entrypoint.sh \
        /usr/local/bin/configure-persistence.sh \
        /usr/local/bin/configure-webui-authentication.sh \
        /usr/local/bin/configure-metrics.sh \
        /usr/local/bin/configure-jmx-credentials.sh \
        /usr/local/bin/spreaper

VOLUME /var/lib/cassandra-reaper
VOLUME /etc/cassandra-reaper/shiro

USER reaper

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["cassandra-reaper"]
