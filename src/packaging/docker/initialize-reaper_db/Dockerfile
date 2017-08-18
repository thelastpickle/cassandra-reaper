FROM cassandra:3.11

# create the reaper_db keyspace
COPY docker-entrypoint.sh /
ENTRYPOINT ["/docker-entrypoint.sh"]

# use a default replication factor of 1
CMD ["1"]
