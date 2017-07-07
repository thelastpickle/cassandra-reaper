FROM cassandra:3.11

# create the required reaper_db keyspace to allow Reaper to store scheduling data
CMD set -x \
    && cqlsh cassandra -e \
    "CREATE KEYSPACE IF NOT EXISTS reaper_db WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };"
