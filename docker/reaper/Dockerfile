FROM ubuntu:16.04

# use a common app path, copied from python-onbuild:latest
ENV WORKDIR /usr/src/app
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}

# install cqlsh dependencies, only used for docker-compose in-tree example
RUN apt-get update \
    && apt-get install -y \
        openjdk-8-jdk

# grab source
COPY ./packages/*.jar ${WORKDIR}/
COPY . ${WORKDIR}/cassandra-reaper

# Add entrypoint script
COPY docker/reaper/docker-entrypoint.sh ${WORKDIR}
ENTRYPOINT ["/usr/src/app/docker-entrypoint.sh"]
