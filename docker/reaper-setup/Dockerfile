FROM ubuntu:16.04

# use a common app path, copied from python-onbuild:latest
ENV WORKDIR /usr/src/app
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}

# install cqlsh dependencies, only used for docker-compose in-tree example
RUN apt-get update \
    && apt-get install -y \
        python \
        wget

# download cassandra for cqlsh, only used for docker-compose in-tree example
WORKDIR ${WORKDIR}
ENV CASSANDRA_FILENAME apache-cassandra-3.0.9-bin.tar.gz
RUN wget http://archive.apache.org/dist/cassandra/3.0.9/${CASSANDRA_FILENAME} \
    && tar xf ${CASSANDRA_FILENAME} \
    && rm ${CASSANDRA_FILENAME}

# grab source
COPY . ${WORKDIR}/cassandra-reaper
WORKDIR ${WORKDIR}/cassandra-reaper

# Add entrypoint script
COPY docker/reaper-setup/docker-entrypoint.sh ${WORKDIR}
ENTRYPOINT ["/usr/src/app/docker-entrypoint.sh"]
