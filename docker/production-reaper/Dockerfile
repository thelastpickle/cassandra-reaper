FROM ubuntu:16.04

# use a common app path, copied from python-onbuild:latest
ENV WORKDIR /usr/src/app
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}

# install Java
RUN apt-get update \
    && apt-get install -y \
        openjdk-8-jdk

# grab jar and configuration from the build directory
COPY ./*.jar ${WORKDIR}/
COPY cassandra-reaper*.yaml ${WORKDIR}/

# start the reaper process
CMD java -jar cassandra-reaper*.jar server cassandra-reaper*.yaml
