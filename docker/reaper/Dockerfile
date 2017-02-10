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

RUN sed -i'.bak' 's/\["127.0.0.1"\]/["cassandra"]/g' \
    ${WORKDIR}/cassandra-reaper/resource/cassandra-reaper-cassandra.yaml
