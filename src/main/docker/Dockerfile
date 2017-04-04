FROM openjdk:8-jre-alpine

ARG SHADED_JAR

ENV DW_REAPER_SEGMENT_COUNT=200 \
    DW_REAPER_REPAIR_PARALELLISM=DATACENTER_AWARE \
    DW_REAPER_REPAIR_INTENSITY=0.9 \
    DW_REAPER_SCHEDULE_DAYS_BETWEEN=7 \
    DW_REAPER_REPAIR_RUN_THREADS=15 \
    DW_REAPER_HANING_REPAIR_TIMEOUT_MINS=30 \
    DW_REAPER_STORAGE_TYPE=memory \
    DW_REAPER_ENABLE_CORS=true \
    DW_REAPER_INCREMENTAL_REPAIR=false \
    DW_REAPER_ALLOW_UNREACHABLE_NODES=false \
    DW_REAPER_ENABLE_DYNAMIC_SEEDS=true \
    DW_REAPER_AUTO_SCHEDULE_ENABLED=false \
    DW_REAPER_AUTO_SCHEDULE_INITIAL_DELAY_PERIOD=PT15S \
    DW_REAPER_AUTO_SCHEDULE_PERIOD_BETWEEN_POLLS=PT10M \
    DW_REAPER_AUTO_SCHEDULE_TIME_BETWEEN_FIRST_SCHEDULE=PT5M \
    DW_REAPER_AUTO_SCHEDULE_TIME_BETWEEN_FIRST_SCHEDULE=PT6H \
    DW_REAPER_AUTO_SCHEDULE_EXCLUDED_KEYSPACES="[]" \
    DW_REAPER_JMX_PORTS="{}" \
    DW_REAPER_LOGGING_ROOT_LEVEL=INFO \
    DW_REAPER_LOGGERS="{}" \
    DW_REAPER_LOGGING_FORMAT='"%-6level [%d] [%t] %logger{5} - %msg %n"' \
    DW_REAPER_APP_PORT=8080 \
    DW_REAPER_APP_BIND_HOST="0.0.0.0" \
    DW_REAPER_ADMIN_PORT=8081 \
    DW_REAPER_ADMIN_BIND_HOST="0.0.0.0" \
    DW_REAPER_CASS_CLUSTER_NAME="clutername" \
    DW_REAPER_CASS_CONTACT_POINTS="[]" \
    DW_REAPER_CASS_KEYSPACE="cassandra-reaper" \
    DW_REAPER_CASS_AUTH_ENABLED="false" \
    DW_REAPER_CASS_AUTH_USERNAME="cassandra" \
    DW_REAPER_CASS_AUTH_PASSWORD="cassandra" \
    DW_REAPER_DB_DRIVER_CLASS="org.h2.Driver" \
    DW_REAPER_DB_URL="jdbc:h2:/var/lib/cassandra-reaper/db;MODE=PostgreSQL" \
    DB_REAPER_DB_USERNAME="" \
    DB_REAPER_DB_PASSWORD="" \
    JAVA_OPTS=""

ADD cassandra-reaper.yml /root/cassandra-reaper.yml
ADD entrypoint.sh /root/entrypoint.sh
ADD append-persistence.sh /root/append-persistence.sh
RUN mkdir -p /var/lib/cassandra-reaper/data && \
    chmod u+x /root/entrypoint.sh && \
    chmod u+x /root/append-persistence.sh

VOLUME /var/lib/cassandra-reaper/data

ADD ${SHADED_JAR} /root/cassandra-reaper.jar

ENTRYPOINT ["/root/entrypoint.sh"]
CMD ["cassandra-reaper"]
