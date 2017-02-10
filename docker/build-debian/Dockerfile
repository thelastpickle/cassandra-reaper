FROM ubuntu:16.04

# use a common app path, copied from python-onbuild:latest
ENV WORKDIR /usr/src/app
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}

# install dependencies
RUN apt-get update \
    && apt-get install -y \
        apt-file \
        debhelper \
        devscripts \
        maven \
        openjdk-8-jdk \
    && mvn --version

# cache maven dependencies, useful during Dockerfile testing
COPY pom.xml /tmp
WORKDIR /tmp
RUN mvn package
WORKDIR ${WORKDIR}

# grab source
COPY . ${WORKDIR}/cassandra-reaper
WORKDIR ${WORKDIR}/cassandra-reaper

# target directory for final build
RUN mkdir ${WORKDIR}/packages
VOLUME ${WORKDIR}/packages

# Add entrypoint script
COPY docker/debian/docker-entrypoint.sh ${WORKDIR}
ENTRYPOINT ["/usr/src/app/docker-entrypoint.sh"]
