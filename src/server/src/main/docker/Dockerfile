FROM openjdk:8-jre-alpine

ARG SHADED_JAR

ENV REAPER_SEGMENT_COUNT=200 \
    REAPER_REPAIR_PARALELLISM=DATACENTER_AWARE \
    REAPER_REPAIR_INTENSITY=0.9 \
    REAPER_SCHEDULE_DAYS_BETWEEN=7 \
    REAPER_REPAIR_RUN_THREADS=15 \
    REAPER_HANING_REPAIR_TIMEOUT_MINS=30 \
    REAPER_STORAGE_TYPE=memory \
    REAPER_ENABLE_CORS=true \
    REAPER_JMX_CONNECTION_TIMEOUT_IN_SECONDS=20 \
    REAPER_INCREMENTAL_REPAIR=false \
    REAPER_DATACENTER_AVAILABILITY=ALL \
    REAPER_ENABLE_DYNAMIC_SEEDS=true \
    REAPER_REPAIR_MANAGER_SCHEDULING_INTERVAL_SECONDS=30 \
    REAPER_ACTIVATE_QUERY_LOGGER=false \
    REAPER_AUTO_SCHEDULE_ENABLED=false \
    REAPER_AUTO_SCHEDULE_INITIAL_DELAY_PERIOD=PT15S \
    REAPER_AUTO_SCHEDULE_PERIOD_BETWEEN_POLLS=PT10M \
    REAPER_AUTO_SCHEDULE_TIME_BETWEEN_FIRST_SCHEDULE=PT5M \
    REAPER_AUTO_SCHEDULE_TIME_BETWEEN_FIRST_SCHEDULE=PT6H \
    REAPER_AUTO_SCHEDULE_EXCLUDED_KEYSPACES="[]" \
    REAPER_JMX_PORTS="{}" \
    REAPER_JMX_USERNAME="" \
    REAPER_JMX_PASSWORD="" \
    REAPER_LOGGING_ROOT_LEVEL=INFO \
    REAPER_LOGGERS="{}" \
    REAPER_LOGGING_FORMAT='"%-6level [%d] [%t] %logger{5} - %msg %n"' \
    REAPER_APP_PORT=8080 \
    REAPER_APP_BIND_HOST="0.0.0.0" \
    REAPER_ADMIN_PORT=8081 \
    REAPER_ADMIN_BIND_HOST="0.0.0.0" \
    REAPER_CASS_CLUSTER_NAME="clutername" \
    REAPER_CASS_CONTACT_POINTS="[]" \
    REAPER_CASS_KEYSPACE="cassandra-reaper" \
    REAPER_CASS_AUTH_ENABLED="false" \
    REAPER_CASS_AUTH_USERNAME="cassandra" \
    REAPER_CASS_AUTH_PASSWORD="cassandra" \
    REAPER_CASS_NATIVE_PROTOCOL_SSL_ENCRYPTION_ENABLED="false" \
    REAPER_DB_DRIVER_CLASS="org.h2.Driver" \
    REAPER_DB_URL="jdbc:h2:/var/lib/cassandra-reaper/db;MODE=PostgreSQL" \
    REAPER_DB_USERNAME="" \
    REAPER_DB_PASSWORD="" \
    REAPER_METRICS_ENABLED=false \
    REAPER_METRICS_FREQUENCY="1 minute" \
    REAPER_METRICS_REPORTERS="[]" \
    JAVA_OPTS=""

ADD cassandra-reaper.yml /etc/cassandra-reaper.yml
ADD entrypoint.sh /usr/local/bin/entrypoint.sh
ADD configure-persistence.sh /usr/local/bin/configure-persistence.sh
ADD configure-metrics.sh /usr/local/bin/configure-metrics.sh

RUN addgroup -S reaper && \
    adduser -S reaper reaper && \
    apk add --no-cache 'su-exec>=0.2' && \
    mkdir -p /var/lib/cassandra-reaper && \
    chown reaper:reaper \
        /etc/cassandra-reaper.yml \
        /var/lib/cassandra-reaper \
        /usr/local/bin/entrypoint.sh \
        /usr/local/bin/configure-persistence.sh \
        /usr/local/bin/configure-metrics.sh && \
    chmod u+x \
        /usr/local/bin/entrypoint.sh \
        /usr/local/bin/configure-persistence.sh \
        /usr/local/bin/configure-metrics.sh

VOLUME /var/lib/cassandra-reaper

ADD ${SHADED_JAR} /usr/local/lib/cassandra-reaper.jar

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["cassandra-reaper"]
