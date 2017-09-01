FROM cassandra:3.11

ENV CQLSH_COMMAND="" \
    CQLSH_ENABLE_SSL=false \
    CASSANDRA_HOSTNAME=""

# create the reaper_db keyspace
COPY docker-entrypoint.sh /
ENTRYPOINT ["/docker-entrypoint.sh"]

# use a default replication factor of 1
CMD [""]